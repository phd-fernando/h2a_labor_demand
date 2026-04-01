###############################################################################
# Fix the dateSubmitted artifact in df_jobs_patched.rds.
#
# Root cause: the JSON API serves "currently active" job orders per snapshot.
# When script 6 deduplicates across snapshots (distinct by caseNumber, first
# row wins), it keeps whichever snapshot FIRST captured a given case.
# If a JO submitted in Oct 2025 first appeared in the Jan 2025 snapshot
# (because the API can return forward-filed JOs), its dateSubmitted from the
# JSON field may differ from the snap_date.
#
# Actually: the JSON field jo$dateSubmitted IS the employer's submission date.
# The mismatch means df_jobs.rds was built from snapshots taken BEFORE the
# Oct 2025 filing dates — i.e., those cases were first seen in earlier
# snapshots with the correct dateSubmitted, but the October JOs that are
# MISSING from the patched file were either:
#   (a) captured in the Oct snapshots only and somehow lost in dedup, OR
#   (b) have a different dateSubmitted in the backfill vs the existing build.
#
# Strategy:
#   1. Load all backfill JSONs (2025-2026) — keep ALL rows, not deduped
#   2. For each caseNumber, take the EARLIEST snap_date's dateSubmitted
#      (most reliable: first time DOL recorded it)
#   3. For every caseNumber in df_jobs_patched, replace dateSubmitted with the
#      backfill's value if:
#        - caseNumber is in the backfill, AND
#        - the backfill dateSubmitted is non-NA
#   4. Report how many dates changed and by how much
###############################################################################
cat("\014"); rm(list = ls())

library(dplyr)
library(jsonlite)
library(lubridate)
library(stringr)

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

zip_dir <- "C:/Data/DOL_SeasonalJobs/zip_backfill_15day"
tmp_dir <- file.path(zip_dir, "_extracted_jo")   # already extracted

parse_dol_date <- function(x) suppressWarnings(as.Date(x, format = "%d-%b-%Y"))

# ── 1. Reload all backfill snapshots — keep every row ─────────────────────────
all_zips <- list.files(zip_dir, pattern = "^sjCaseData_jo_202[56].*\\.zip$",
                       full.names = TRUE)

cat(sprintf("Reading %d zip files...\n", length(all_zips)))

raw_all <- lapply(all_zips, function(zpath) {
  snap <- str_extract(basename(zpath), "\\d{4}-\\d{2}-\\d{2}")
  udir <- file.path(tmp_dir, snap)
  jfiles <- list.files(udir, pattern = "\\.json$", full.names = TRUE)
  if (length(jfiles) == 0) return(NULL)
  df <- tryCatch(fromJSON(jfiles[1], flatten = TRUE) |> as_tibble(),
                 error = function(e) NULL)
  if (is.null(df)) return(NULL)
  df |> mutate(snap_date = as.Date(snap))
}) |> bind_rows()

cat(sprintf("Total rows (all snapshots, all years): %d\n", nrow(raw_all)))

# ── 2. Parse dateSubmitted; for each caseNumber take earliest snap's date ──────
# The backfill JSONs have a `dateSubmitted` field (confirmed in column listing)
if ("dateSubmitted" %in% names(raw_all)) {
  bf_dates <- raw_all |>
    mutate(dateSubmitted_bf = parse_dol_date(dateSubmitted)) |>
    arrange(caseNumber, snap_date) |>                 # earliest snapshot first
    distinct(caseNumber, .keep_all = TRUE) |>
    select(caseNumber, dateSubmitted_bf, snap_date_first = snap_date)
} else {
  # fallback: use snap_date as proxy
  bf_dates <- raw_all |>
    arrange(caseNumber, snap_date) |>
    distinct(caseNumber, .keep_all = TRUE) |>
    mutate(dateSubmitted_bf = snap_date) |>
    select(caseNumber, dateSubmitted_bf, snap_date_first = snap_date)
}

cat(sprintf("Unique caseNumbers with backfill date: %d\n", nrow(bf_dates)))

# ── 3. Load existing patched file ─────────────────────────────────────────────
patched <- readRDS("processed/text/df_jobs_patched.rds")
cat(sprintf("Existing patched rows: %d\n\n", nrow(patched)))

# ── 4. Merge and compare dates ─────────────────────────────────────────────────
comparison <- patched |>
  select(caseNumber, dateSubmitted_old = dateSubmitted) |>
  left_join(bf_dates, by = "caseNumber") |>
  mutate(
    has_bf       = !is.na(dateSubmitted_bf),
    bf_different = has_bf & !is.na(dateSubmitted_old) &
                   dateSubmitted_bf != dateSubmitted_old,
    bf_fills_na  = has_bf & is.na(dateSubmitted_old) & !is.na(dateSubmitted_bf),
    day_diff     = as.integer(dateSubmitted_bf - dateSubmitted_old)
  )

cat("=== DATE COMPARISON SUMMARY ===\n")
cat(sprintf("Cases in patched with backfill match:    %d  (%.1f%%)\n",
            sum(comparison$has_bf), mean(comparison$has_bf)*100))
cat(sprintf("Dates already identical:                 %d\n",
            sum(comparison$has_bf & !comparison$bf_different & !comparison$bf_fills_na)))
cat(sprintf("Dates DIFFER (will be corrected):        %d\n",
            sum(comparison$bf_different)))
cat(sprintf("dateSubmitted was NA, backfill fills it: %d\n\n",
            sum(comparison$bf_fills_na)))

if (sum(comparison$bf_different) > 0) {
  cat("Distribution of day differences (backfill_date - existing_date):\n")
  comparison |>
    filter(bf_different) |>
    pull(day_diff) |>
    summary() |> print()

  cat("\nMonthly breakdown — existing dateSubmitted for the corrected cases:\n")
  comparison |>
    filter(bf_different) |>
    mutate(ym_old = floor_date(dateSubmitted_old, "month"),
           ym_bf  = floor_date(dateSubmitted_bf,  "month")) |>
    count(ym_old, ym_bf) |>
    arrange(ym_old) |>
    print(n = 40)
}

# ── 5. Apply corrections ──────────────────────────────────────────────────────
cat("\n=== APPLYING CORRECTIONS ===\n")

patched_fixed <- patched |>
  left_join(bf_dates |> select(caseNumber, dateSubmitted_bf), by = "caseNumber") |>
  mutate(
    dateSubmitted = case_when(
      !is.na(dateSubmitted_bf) ~ dateSubmitted_bf,   # use backfill date
      TRUE                     ~ dateSubmitted        # keep existing
    )
  ) |>
  select(-dateSubmitted_bf)

# Verify
n_changed <- sum(
  coalesce(patched_fixed$dateSubmitted, as.Date("1900-01-01")) !=
  coalesce(patched$dateSubmitted,       as.Date("1900-01-01")),
  na.rm = TRUE
)
cat(sprintf("dateSubmitted values changed: %d\n", n_changed))

# Annual distribution before and after
cat("\nAnnual JO counts — BEFORE vs AFTER date correction:\n")
before <- patched       |> filter(!is.na(dateSubmitted)) |> count(year=year(dateSubmitted), name="before")
after  <- patched_fixed |> filter(!is.na(dateSubmitted)) |> count(year=year(dateSubmitted), name="after")
full_join(before, after, by="year") |>
  mutate(delta = coalesce(after,0L) - coalesce(before,0L)) |>
  arrange(year) |>
  print()

# Monthly detail for 2025
cat("\nMonthly counts 2025 — before vs after:\n")
b25 <- patched       |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |>
       count(ym=floor_date(dateSubmitted,"month"), name="before")
a25 <- patched_fixed |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |>
       count(ym=floor_date(dateSubmitted,"month"), name="after")
full_join(b25, a25, by="ym") |> mutate(delta=coalesce(after,0L)-coalesce(before,0L)) |>
  arrange(ym) |> print()

# ── 6. Save ───────────────────────────────────────────────────────────────────
saveRDS(patched_fixed, "processed/text/df_jobs_patched.rds")
cat("\nSaved corrected df_jobs_patched.rds\n")

# ── 7. Append report ──────────────────────────────────────────────────────────
report_lines <- c(
  "",
  "===========================================================",
  "  DATE CORRECTION REPORT",
  sprintf("  Run date: %s", Sys.Date()),
  "===========================================================",
  sprintf("  dateSubmitted values corrected: %d", n_changed),
  sprintf("  Source: backfill ZIPs earliest-snapshot date"),
  "  Logic: for each caseNumber in backfill, take the earliest",
  "         snap's dateSubmitted; replace existing if non-NA.",
  sprintf("  Updated file: processed/text/df_jobs_patched.rds")
)
write(report_lines, "notes/backfill_audit_2025_2026.txt", append = TRUE)
cat("Report appended to notes/backfill_audit_2025_2026.txt\nDone.\n")
