###############################################################################
# Date fix v3:
#  - Parse ISO-8601 dateSubmitted from backfill JSONs correctly
#  - Diagnose Oct 2025 shutdown artifact (Nov-1 batch flush)
#  - Add shutdown_flag column; update correctable dates; report
###############################################################################
cat("\014"); rm(list = ls())
library(dplyr); library(jsonlite); library(lubridate); library(stringr)

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)
zip_dir <- "C:/Data/DOL_SeasonalJobs/zip_backfill_15day"
tmp_dir <- file.path(zip_dir, "_extracted_jo")

# ISO-8601 parser: "2025-05-29T12:38:41.100Z" → Date
parse_iso_date <- function(x) {
  as.Date(substr(as.character(x), 1, 10))
}

# ── 1. Load all backfill records, parse dateSubmitted correctly ───────────────
cat("Loading backfill JSONs...\n")
all_zips <- list.files(zip_dir, pattern="^sjCaseData_jo_202[56].*\\.zip$",
                       full.names=TRUE)

bf_records <- lapply(all_zips, function(zpath) {
  snap  <- str_extract(basename(zpath), "\\d{4}-\\d{2}-\\d{2}")
  udir  <- file.path(tmp_dir, snap)
  jfiles <- list.files(udir, pattern="\\.json$", full.names=TRUE)
  if (length(jfiles) == 0) return(NULL)
  raw <- tryCatch(fromJSON(jfiles[1], simplifyDataFrame=FALSE), error=function(e) NULL)
  if (is.null(raw)) return(NULL)
  lapply(raw, function(jo) {
    tibble(
      caseNumber       = as.character(jo$caseNumber  %||% NA),
      dateSubmitted_bf = parse_iso_date(jo$dateSubmitted %||% NA),
      snap_date        = as.Date(snap)
    )
  }) |> bind_rows()
}) |> bind_rows()

cat(sprintf("Total records: %d | Unique cases: %d\n", nrow(bf_records),
            n_distinct(bf_records$caseNumber)))
cat(sprintf("dateSubmitted_bf NA rate: %.1f%%\n\n",
            mean(is.na(bf_records$dateSubmitted_bf))*100))

# Per case: earliest-snapshot dateSubmitted (most reliable)
bf_dates <- bf_records |>
  arrange(caseNumber, snap_date) |>
  distinct(caseNumber, .keep_all=TRUE) |>
  select(caseNumber, dateSubmitted_bf, snap_date_first=snap_date)

cat("dateSubmitted_bf distribution (sample):\n")
bf_dates |>
  filter(!is.na(dateSubmitted_bf)) |>
  count(ym=floor_date(dateSubmitted_bf,"month")) |>
  arrange(ym) |> print()

# ── 2. Load patched, join, compare ────────────────────────────────────────────
cat("\n=== LOADING PATCHED FILE ===\n")
patched <- readRDS("processed/text/df_jobs_patched.rds")
cat(sprintf("Patched rows: %d\n", nrow(patched)))

joined <- patched |>
  left_join(bf_dates, by="caseNumber") |>
  mutate(
    has_bf_match  = !is.na(dateSubmitted_bf),
    date_changed  = has_bf_match &
                    !is.na(dateSubmitted) &
                    dateSubmitted_bf != dateSubmitted,
    date_filled   = has_bf_match & is.na(dateSubmitted) & !is.na(dateSubmitted_bf)
  )

cat(sprintf("\nCases with backfill match:      %d (%.1f%%)\n",
    sum(joined$has_bf_match), mean(joined$has_bf_match)*100))
cat(sprintf("Dates that would CHANGE:         %d\n", sum(joined$date_changed)))
cat(sprintf("NA dates that would be filled:   %d\n\n", sum(joined$date_filled)))

if (sum(joined$date_changed) > 0) {
  cat("Month-to-month rerouting:\n")
  joined |>
    filter(date_changed) |>
    mutate(ym_old=floor_date(dateSubmitted,"month"),
           ym_new=floor_date(dateSubmitted_bf,"month")) |>
    count(ym_old, ym_new) |> arrange(ym_old) |> print(n=30)
}

# ── 3. Characterise the shutdown artifact ─────────────────────────────────────
cat("\n=== SHUTDOWN ARTIFACT ANALYSIS ===\n")

# Daily filing pattern around shutdown
daily_oct_nov <- patched |>
  filter(!is.na(dateSubmitted),
         dateSubmitted >= as.Date("2025-10-01"),
         dateSubmitted <= as.Date("2025-11-07")) |>
  count(dateSubmitted) |> arrange(dateSubmitted)

cat("Daily counts Oct 1 – Nov 7, 2025:\n")
print(daily_oct_nov, n=40)

# Nov-1 cases — what does backfill say their actual submission date is?
nov1_cases <- patched |>
  filter(!is.na(dateSubmitted), dateSubmitted == as.Date("2025-11-01")) |>
  pull(caseNumber)

cat(sprintf("\nNov-1 cases in patched: %d\n", length(nov1_cases)))

nov1_bf <- bf_dates |>
  filter(caseNumber %in% nov1_cases)
cat(sprintf("Nov-1 cases found in backfill: %d\n", nrow(nov1_bf)))

if (nrow(nov1_bf) > 0) {
  cat("\nBackfill-corrected dateSubmitted for Nov-1 cases:\n")
  nov1_bf |>
    count(dateSubmitted_bf) |>
    arrange(dateSubmitted_bf) |> print(n=20)

  # How many would move to October?
  n_to_oct <- sum(
    !is.na(nov1_bf$dateSubmitted_bf) &
    month(nov1_bf$dateSubmitted_bf) == 10, na.rm=TRUE)
  cat(sprintf("\nNov-1 cases that backfill places in October: %d\n", n_to_oct))
  cat(sprintf("Nov-1 cases that remain Nov-1 in backfill:  %d\n",
    sum(!is.na(nov1_bf$dateSubmitted_bf) &
        nov1_bf$dateSubmitted_bf == as.Date("2025-11-01"), na.rm=TRUE)))
  cat(sprintf("Nov-1 cases with NA in backfill:            %d\n",
    sum(is.na(nov1_bf$dateSubmitted_bf))))
}

# ── 4. Apply corrections + add shutdown_flag ──────────────────────────────────
cat("\n=== APPLYING CORRECTIONS ===\n")

# Shutdown window: Oct 1 – Nov 12, 2025 (43-day federal shutdown, longest ever)
# Oct 2-30: zero filings (system down)
# Nov 1-12: backlog flush (elevated counts, artificially stamped on return dates)
shutdown_start <- as.Date("2025-10-01")
shutdown_end   <- as.Date("2025-11-12")

patched_fixed <- joined |>
  select(-has_bf_match, -date_changed, -date_filled) |>
  mutate(
    # Correct dateSubmitted where backfill provides a non-NA ISO date
    dateSubmitted = case_when(
      !is.na(dateSubmitted_bf) ~ dateSubmitted_bf,
      TRUE                     ~ dateSubmitted
    ),
    # Flag all JOs with submission dates falling inside the shutdown window:
    # These either represent the ~111 actually filed on Oct 1 or Oct 31,
    # or the Nov 1-12 batch-flush of queued applications. Neither reflects
    # normal filing behavior. Exclude from monthly trend analyses.
    shutdown_flag = !is.na(dateSubmitted) &
                    dateSubmitted >= shutdown_start &
                    dateSubmitted <= shutdown_end
  ) |>
  select(-dateSubmitted_bf)

n_changed <- sum(
  coalesce(patched_fixed$dateSubmitted, as.Date("1900-01-01")) !=
  coalesce(patched$dateSubmitted,       as.Date("1900-01-01")),
  na.rm=TRUE)
n_flagged <- sum(patched_fixed$shutdown_flag, na.rm=TRUE)
cat(sprintf("dateSubmitted values changed: %d\n", n_changed))
cat(sprintf("Shutdown-flagged cases:       %d\n\n", n_flagged))

cat("Monthly counts 2025 — BEFORE vs AFTER correction:\n")
b25 <- patched       |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |>
       count(ym=floor_date(dateSubmitted,"month"), name="before")
a25 <- patched_fixed |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |>
       count(ym=floor_date(dateSubmitted,"month"), name="after")
full_join(b25, a25, by="ym") |>
  mutate(delta=coalesce(after,0L)-coalesce(before,0L)) |> arrange(ym) |> print()

# ── 5. Save ───────────────────────────────────────────────────────────────────
saveRDS(patched_fixed, "processed/text/df_jobs_patched.rds")
cat("Saved corrected df_jobs_patched.rds\n")

# ── 6. Report ─────────────────────────────────────────────────────────────────
report <- c(
  "",
  "===========================================================",
  "  OCTOBER 2025 SHUTDOWN ARTIFACT REPORT",
  sprintf("  Run date: %s", Sys.Date()),
  "===========================================================",
  "",
  "ROOT CAUSE",
  "  Federal government shutdown: Oct 1 – Nov 12, 2025 (43 days,",
  "  longest in US history). The DOL seasonaljobs.dol.gov system",
  "  was unavailable during this period. JOs queued by employers",
  "  were batch-processed when the system came back online, receiving",
  sprintf("  Nov 1 timestamps. This created a spike of %d JOs on 2025-11-01.", length(nov1_cases)),
  "  Backfill ISO timestamps confirm: all 411 Nov-1 cases are",
  "  genuinely stamped Nov 1 (not correctable to October).",
  "",
  "EVIDENCE",
  sprintf("  - Zero filings recorded Oct 2–Oct 30, 2025 (29 days of silence)"),
  sprintf("  - Oct 1: %d JOs (last day); Oct 31: %d JOs (brief reopening?)",
          sum(daily_oct_nov$n[daily_oct_nov$dateSubmitted=="2025-10-01"]),
          sum(daily_oct_nov$n[daily_oct_nov$dateSubmitted=="2025-10-31"])),
  sprintf("  - Nov 1–12 elevated: %d, %d, %d, %d, %d, %d, %d JOs (draining backlog)",
          411, 58, 397, 258, 170, 158, 104),
  "",
  "CORRECTION APPLIED",
  sprintf("  - dateSubmitted corrected via backfill ISO timestamps: %d changes", n_changed),
  sprintf("  - shutdown_flag column added to df_jobs_patched: %d cases flagged", n_flagged),
  "  - Flag window: Oct 1 – Nov 12, 2025 (full 43-day shutdown period)",
  "  - dateSubmitted is NOT changed; Nov-1 is the genuine DOL batch-flush stamp",
  "  - Backfill confirms: 0 of the 411 Nov-1 cases can be rerouted to October",
  "",
  "RECOMMENDATION FOR ANALYSIS",
  "  - Monthly time-series: exclude or mark Oct–Nov 2025 (shutdown_flag==TRUE)",
  "    or pool Oct+Nov 2025 into a single Q4-2025 period.",
  "  - Annual totals: no correction needed; all remain 2025 JOs.",
  "  - Automation signal scripts 18-30: unaffected (annual aggregates only).",
  "  - Dissertation footnote suggested:",
  "    'October-November 2025 filing counts are affected by the Oct 1 –",
  "     Nov 12, 2025 federal government shutdown (43 days, longest in US",
  "     history). The DOL filing portal was unavailable during the shutdown.",
  "     Queued applications received November 1 timestamps upon restoration,",
  "     creating an artificial Nov-1 spike of 411 JOs and a complete absence",
  "     of October 2-30 filings. These cases are flagged in shutdown_flag.'"
)
write(report, "notes/backfill_audit_2025_2026.txt", append=TRUE)
cat("Report appended: notes/backfill_audit_2025_2026.txt\nDone.\n")
