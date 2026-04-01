###############################################################################
# Backfill audit: unzip 2025-2026 sjCaseData_jo_*.zip files,
# check duplicates against existing df_jobs_patched.rds,
# add net-new records, and produce a report.
###############################################################################
cat("\014"); rm(list = ls())

library(dplyr)
library(jsonlite)
library(lubridate)
library(stringr)
library(tidyr)

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

zip_dir  <- "C:/Data/DOL_SeasonalJobs/zip_backfill_15day"
tmp_dir  <- file.path(zip_dir, "_extracted_jo")
dir.create(tmp_dir, showWarnings = FALSE)

# ── 1. Identify 2025-2026 jo zips ─────────────────────────────────────────────
all_zips <- list.files(zip_dir, pattern = "^sjCaseData_jo_202[56].*\\.zip$",
                       full.names = TRUE)
cat(sprintf("Found %d job-order zip files for 2025-2026\n", length(all_zips)))
cat(paste(" ", basename(all_zips), collapse = "\n"), "\n\n")

# ── 2. Unzip each and parse JSON ───────────────────────────────────────────────
parse_jo_zip <- function(zpath) {
  fname  <- basename(zpath)
  snap   <- str_extract(fname, "\\d{4}-\\d{2}-\\d{2}")
  udir   <- file.path(tmp_dir, snap)
  dir.create(udir, showWarnings = FALSE)
  unzip(zpath, exdir = udir, overwrite = TRUE)
  jfiles <- list.files(udir, pattern = "\\.json$", full.names = TRUE)
  if (length(jfiles) == 0) return(NULL)
  df <- tryCatch(
    fromJSON(jfiles[1], flatten = TRUE) |> as_tibble(),
    error = function(e) { cat("  ERROR parsing", fname, ":", conditionMessage(e), "\n"); NULL }
  )
  if (!is.null(df)) {
    df <- df |> mutate(snap_date = as.Date(snap), .before = 1)
  }
  df
}

cat("=== Parsing zip files ===\n")
raw_list <- lapply(all_zips, function(z) {
  cat("  ", basename(z), "... ")
  df <- parse_jo_zip(z)
  if (!is.null(df)) cat(nrow(df), "rows\n") else cat("skipped\n")
  df
})

df_new_raw <- bind_rows(raw_list)
cat(sprintf("\nTotal rows across all snapshots: %d\n", nrow(df_new_raw)))
cat(sprintf("Unique columns: %s\n\n", paste(names(df_new_raw)[1:20], collapse=", ")))

# ── 3. Standardise dates and deduplicate within new batch ─────────────────────
# caseNumber is the unique key; keep the most recent snapshot row per case
if (!"caseNumber" %in% names(df_new_raw)) stop("caseNumber column not found in JSON")

# Parse jobBeginDate / jobEndDate (format like "28-Jul-2025")
parse_dol_date <- function(x) {
  suppressWarnings(as.Date(x, format = "%d-%b-%Y"))
}

df_new <- df_new_raw |>
  mutate(
    jobBeginDate   = parse_dol_date(jobBeginDate),
    jobEndDate     = parse_dol_date(jobEndDate),
    dateSubmitted  = parse_dol_date(coalesce(dateSubmitted, as.character(snap_date)))
  ) |>
  arrange(caseNumber, desc(snap_date)) |>
  distinct(caseNumber, .keep_all = TRUE)

cat(sprintf("Unique case numbers in new batch:  %d\n", nrow(df_new)))

# Monthly distribution
cat("\nMonthly submission counts (new batch, by dateSubmitted):\n")
df_new |>
  mutate(ym = floor_date(coalesce(dateSubmitted, snap_date), "month")) |>
  count(ym) |> print(n = 30)

# ── 4. Load existing patched file and compare ─────────────────────────────────
cat("\n=== Comparing against existing df_jobs_patched.rds ===\n")
existing <- readRDS("processed/text/df_jobs_patched.rds")
cat(sprintf("Existing patched rows:             %d\n", nrow(existing)))
cat(sprintf("Existing unique caseNumbers:       %d\n", n_distinct(existing$caseNumber)))

existing_cases <- existing$caseNumber

n_already_in  <- sum(df_new$caseNumber %in% existing_cases)
n_net_new     <- sum(!df_new$caseNumber %in% existing_cases)

cat(sprintf("\nNew-batch cases already in patched: %d  (%.1f%%)\n",
            n_already_in, n_already_in / nrow(df_new) * 100))
cat(sprintf("Net-new cases not in patched:       %d  (%.1f%%)\n\n",
            n_net_new, n_net_new / nrow(df_new) * 100))

df_net_new <- df_new |> filter(!caseNumber %in% existing_cases)

# ── 5. Monthly breakdown of net-new cases ─────────────────────────────────────
cat("=== Net-new cases by submission month ===\n")
df_net_new |>
  mutate(ym = floor_date(coalesce(dateSubmitted, snap_date), "month"),
         year = year(ym)) |>
  count(year, ym) |>
  arrange(ym) |>
  print(n = 30)

# ── 6. Column alignment check ─────────────────────────────────────────────────
cat("\n=== Column alignment: existing vs new ===\n")
cols_existing <- names(existing)
cols_new      <- names(df_new)
cat("Columns in existing NOT in new:\n")
print(setdiff(cols_existing, cols_new))
cat("Columns in new NOT in existing:\n")
print(setdiff(cols_new, cols_existing))

# ── 7. Align and append net-new to patched ────────────────────────────────────
# Keep only columns present in both; retain all existing columns with NA for missing
shared_cols <- intersect(cols_existing, cols_new)
extra_in_existing <- setdiff(cols_existing, cols_new)

df_net_aligned <- df_net_new |>
  select(all_of(shared_cols)) |>
  bind_cols(
    as_tibble(setNames(
      lapply(extra_in_existing, function(x) rep(NA, nrow(df_net_new))),
      extra_in_existing
    ))
  ) |>
  select(all_of(cols_existing))   # restore original column order

df_patched_updated <- bind_rows(existing, df_net_aligned)

cat(sprintf("\n=== Updated patched file ===\n"))
cat(sprintf("Original rows:  %d\n", nrow(existing)))
cat(sprintf("Net-new added:  %d\n", nrow(df_net_aligned)))
cat(sprintf("Updated total:  %d\n", nrow(df_patched_updated)))

# Annual counts before and after
cat("\nAnnual JO counts — BEFORE vs AFTER:\n")
before_annual <- existing |>
  filter(!is.na(dateSubmitted)) |>
  count(year = year(dateSubmitted), name = "n_before")

after_annual <- df_patched_updated |>
  filter(!is.na(dateSubmitted)) |>
  count(year = year(dateSubmitted), name = "n_after")

before_annual |>
  full_join(after_annual, by = "year") |>
  mutate(n_added = coalesce(n_after, 0L) - coalesce(n_before, 0L)) |>
  arrange(year) |>
  print()

# ── 8. October 2025 gap diagnosis ─────────────────────────────────────────────
cat("\n=== October 2025 diagnosis ===\n")
oct25_existing <- existing |>
  filter(!is.na(dateSubmitted),
         year(dateSubmitted) == 2025, month(dateSubmitted) == 10)
oct25_new <- df_net_new |>
  filter(!is.na(dateSubmitted),
         year(dateSubmitted) == 2025, month(dateSubmitted) == 10)
oct25_all <- df_new |>
  filter(!is.na(dateSubmitted),
         year(dateSubmitted) == 2025, month(dateSubmitted) == 10)

cat(sprintf("Oct 2025 in existing patched:  %d\n", nrow(oct25_existing)))
cat(sprintf("Oct 2025 in new batch (all):   %d\n", nrow(oct25_all)))
cat(sprintf("Oct 2025 net-new:              %d\n", nrow(oct25_new)))

# Which snap_dates capture Oct 2025 submissions?
df_new |>
  filter(!is.na(dateSubmitted),
         year(dateSubmitted) == 2025, month(dateSubmitted) == 10) |>
  count(snap_date) |>
  print()

# ── 9. Save updated patched file ──────────────────────────────────────────────
saveRDS(df_patched_updated, "processed/text/df_jobs_patched.rds")
cat("\nSaved updated df_jobs_patched.rds\n")

# ── 10. Write plain-text report ───────────────────────────────────────────────
report <- capture.output({
  cat("===========================================================\n")
  cat("  H-2A BACKFILL AUDIT REPORT\n")
  cat(sprintf("  Run date: %s\n", Sys.Date()))
  cat("===========================================================\n\n")

  cat("SOURCE\n")
  cat(sprintf("  Zip directory : %s\n", zip_dir))
  cat(sprintf("  Files scanned : %d (2025-2026 jo zips)\n", length(all_zips)))
  cat(sprintf("  Total rows parsed          : %d\n", nrow(df_new_raw)))
  cat(sprintf("  Unique caseNumbers (deduped): %d\n\n", nrow(df_new)))

  cat("OVERLAP WITH EXISTING DATA\n")
  cat(sprintf("  Existing df_jobs_patched rows  : %d\n", nrow(existing)))
  cat(sprintf("  Already present in existing    : %d  (%.1f%%)\n",
              n_already_in, n_already_in/nrow(df_new)*100))
  cat(sprintf("  Net-new (added this run)       : %d  (%.1f%%)\n\n",
              n_net_new, n_net_new/nrow(df_new)*100))

  cat("ANNUAL COUNTS BEFORE → AFTER\n")
  before_annual |>
    full_join(after_annual, by = "year") |>
    mutate(n_added = coalesce(n_after,0L) - coalesce(n_before,0L)) |>
    filter(year >= 2019) |>
    arrange(year) |>
    as.data.frame() |>
    print(row.names = FALSE)

  cat("\nOCTOBER 2025 GAP\n")
  cat(sprintf("  Before backfill : %d JOs\n", nrow(oct25_existing)))
  cat(sprintf("  After backfill  : %d JOs\n", nrow(oct25_existing) + nrow(oct25_new)))
  cat(sprintf("  Net added       : %d\n\n", nrow(oct25_new)))

  cat("COLUMN GAPS\n")
  missing_in_new <- setdiff(cols_existing, cols_new)
  if (length(missing_in_new) == 0) {
    cat("  None — all existing columns present in new batch (filled NA where absent)\n")
  } else {
    cat("  Columns in existing NOT in new (set to NA):\n")
    cat(paste("   ", missing_in_new, collapse="\n"), "\n")
  }

  cat("\nOUTPUT\n")
  cat(sprintf("  Updated file: processed/text/df_jobs_patched.rds\n"))
  cat(sprintf("  Total rows  : %d\n", nrow(df_patched_updated)))
})

writeLines(report, "notes/backfill_audit_2025_2026.txt")
cat("\nReport saved: notes/backfill_audit_2025_2026.txt\n")
cat("Done.\n")
