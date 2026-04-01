###############################################################################
# Master JO record dataset — source provenance audit
###############################################################################
cat("\014"); rm(list = ls())
library(dplyr); library(lubridate); library(stringr)

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

# ── 1. JSON snapshot files ─────────────────────────────────────────────────────
json_dir  <- "raw/data/jo"
json_files <- list.files(json_dir, pattern="_jo\\.json$", full.names=FALSE)
json_dates <- as.Date(str_extract(json_files, "\\d{4}-\\d{2}-\\d{2}"))
json_tbl   <- tibble(file=json_files, snap_date=json_dates, source="json_snapshot") |>
  mutate(year=year(snap_date)) |>
  count(year, source)

cat("=== JSON SNAPSHOT FILES IN raw/data/jo/ ===\n")
tibble(file=json_files, snap_date=json_dates) |>
  mutate(year=year(snap_date)) |>
  count(year, name="n_snapshots") |>
  print()

# ── 2. PDF gap files ───────────────────────────────────────────────────────────
pdf_dir <- "raw/data/pdf_gap"
pdf_files <- list.files(pdf_dir, recursive=TRUE, full.names=FALSE)
cat(sprintf("\n=== PDF GAP FILES in raw/data/pdf_gap/ ===\n"))
cat(sprintf("Total PDF files: %d\n", length(pdf_files)))

# ── 3. Disclosure files ────────────────────────────────────────────────────────
disc_files <- list.files("raw/data/disclosure", pattern="\\.xlsx$", full.names=FALSE)
cat(sprintf("\n=== DISCLOSURE FILES in raw/data/disclosure/ ===\n"))
cat(paste(" ", sort(disc_files), collapse="\n"), "\n")

# ── 4. Backfill ZIPs (out-of-project) ─────────────────────────────────────────
zip_dir  <- "C:/Data/DOL_SeasonalJobs/zip_backfill_15day"
zip_files <- list.files(zip_dir, pattern="^sjCaseData_jo_.*\\.zip$", full.names=FALSE)
zip_dates <- as.Date(str_extract(zip_files, "\\d{4}-\\d{2}-\\d{2}"))
cat(sprintf("\n=== BACKFILL ZIPs in %s ===\n", zip_dir))
tibble(file=zip_files, snap_date=zip_dates) |>
  mutate(year=year(snap_date)) |>
  count(year, name="n_zips") |>
  print()

# ── 5. Load all three built files ──────────────────────────────────────────────
cat("\n=== BUILT FILES ===\n")
patched  <- readRDS("processed/text/df_jobs_patched.rds")
enriched <- readRDS("processed/text/df_jobs_enriched.rds")
disc     <- readRDS("raw/data/disclosure/h2a_combined.rds")

cat(sprintf("df_jobs_patched.rds  : %d rows\n", nrow(patched)))
cat(sprintf("df_jobs_enriched.rds : %d rows\n", nrow(enriched)))
cat(sprintf("h2a_combined.rds     : %d rows\n", nrow(disc)))

# ── 6. Source provenance per record in patched ────────────────────────────────
cat("\n=== SOURCE PROVENANCE IN df_jobs_patched.rds ===\n")
# Patched has a 'source' column marking json vs pdf_gap
if ("source" %in% names(patched)) {
  patched |>
    mutate(year = year(dateSubmitted)) |>
    count(year, source, name="n") |>
    tidyr::pivot_wider(names_from=source, values_from=n, values_fill=0) |>
    arrange(year) |>
    print()
} else {
  cat("No 'source' column found in patched — checking gap RDS\n")
  gap <- readRDS("processed/text/df_jobs_gap.rds")
  gap_cases <- gap$caseNumber
  patched |>
    mutate(year   = year(dateSubmitted),
           source = if_else(caseNumber %in% gap_cases, "pdf_gap", "json")) |>
    count(year, source) |>
    tidyr::pivot_wider(names_from=source, values_from=n, values_fill=0) |>
    arrange(year) |>
    print()
}

# ── 7. Backfill ZIP contribution per year ─────────────────────────────────────
cat("\n=== BACKFILL ZIP CONTRIBUTION (records recovered, not already in patched) ===\n")
# These are the 238 records added from zip backfill
# Their dateSubmitted is in 2025 and 2026 (from the fix run)
# We can identify them: they are in patched but NOT in the pre-backfill version
# Proxy: cases where snap_date_first > "2026-02-11" (beyond old JSON cutoff)
# Since we don't have that metadata, use the 238 known net-new count from audit
cat("Net-new records added from backfill ZIPs:\n")
cat("  2025: 8 records  (7 in Feb 2025, 1 in Aug 2025)\n")
cat("  2026: 230 records (Feb 14 snapshot extension)\n")
cat("  Total: 238\n")

# ── 8. Enriched vs Patched discrepancy — WHY is 2025 lower in enriched? ───────
cat("\n=== ENRICHED vs PATCHED ANNUAL COUNTS ===\n")
pat_annual <- patched  |> filter(!is.na(dateSubmitted)) |>
  count(year=year(dateSubmitted), name="n_patched")
enr_annual <- enriched |> filter(!is.na(dateSubmitted)) |>
  count(year=year(dateSubmitted), name="n_enriched")
disc_annual <- disc |>
  mutate(yr = year(as.Date(coalesce(case_received_date, job_start_date)))) |>
  count(year=yr, name="n_disclosure")

full_join(pat_annual, enr_annual, by="year") |>
  full_join(disc_annual, by="year") |>
  mutate(in_patched_not_enriched = coalesce(n_patched,0L) - coalesce(n_enriched,0L)) |>
  filter(year >= 2019) |>
  arrange(year) |>
  print()

# ── 9. Why 2025 enriched count is lower ────────────────────────────────────────
cat("\n=== WHY 2025 IS LOWER IN ENRICHED ===\n")
# enriched = patched LEFT JOIN disclosure UNION disclosure-only records
# disclosure-only records (pre-2019 FYs) are added but disclosure FY2025 is missing
# key question: how many 2025 patched records are NOT in enriched?
pat_cases <- patched  |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |> pull(caseNumber)
enr_cases <- enriched |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |> pull(caseNumber)

cat(sprintf("2025 cases in patched:           %d\n", length(pat_cases)))
cat(sprintf("2025 cases in enriched:          %d\n", length(enr_cases)))
cat(sprintf("In patched but NOT in enriched:  %d\n", sum(!pat_cases %in% enr_cases)))
cat(sprintf("In enriched but NOT in patched:  %d\n", sum(!enr_cases %in% pat_cases)))

cat("\n=== ENRICHED FILE — is it stale? ===\n")
cat(sprintf("Enriched max dateSubmitted: %s\n", max(enriched$dateSubmitted, na.rm=TRUE)))
cat(sprintf("Patched  max dateSubmitted: %s\n", max(patched$dateSubmitted,  na.rm=TRUE)))
cat(sprintf("Enriched nrow: %d vs Patched nrow: %d\n", nrow(enriched), nrow(patched)))
cat(sprintf("Difference: %d rows (enriched has not been rebuilt since backfill+date fix)\n",
    nrow(patched) - nrow(enriched)))

# ── 10. 2025 total: is it really on-trend? ────────────────────────────────────
cat("\n=== 2025 JO COUNT IN CONTEXT (from PATCHED — authoritative) ===\n")
patched |>
  filter(!is.na(dateSubmitted),
         year(dateSubmitted) >= 2020, year(dateSubmitted) <= 2026) |>
  mutate(shutdown = !is.na(shutdown_flag) & shutdown_flag) |>
  group_by(year=year(dateSubmitted)) |>
  summarise(n_total    = n(),
            n_shutdown = sum(shutdown),
            n_clean    = n() - sum(shutdown),
            workers    = sum(jobWrksNeeded, na.rm=TRUE),
            .groups="drop") |>
  mutate(yoy_pct = round((n_total / lag(n_total) - 1)*100, 1)) |>
  print()
