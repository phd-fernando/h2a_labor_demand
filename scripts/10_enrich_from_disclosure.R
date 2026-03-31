###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Enrich df_jobs_patched.rds with DOL OFLC disclosure data.
# Disclosure is authoritative for single-valued fields (FEIN, NAICS, SOC,
# workers certified, wage, primary crop, case status).
# Do NOT overwrite multi-site worksite/housing fields — disclosure only
# has the primary entry; full Addendum B detail is in df_locations.rds.
#
# Strategy:
#   1. Join disclosure fields to patched on job_order_number = caseNumber
#   2. Coalesce: prefer existing values (from JSON), fill NAs from disclosure
#   3. Add 50,250 disclosure-only records (earlier FYs, not in JSON feed)
#   4. Save as df_jobs_enriched.rds (never overwrites originals)
###############################################################################

cat("\014"); rm(list = ls())

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "lubridate")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# 1. LOAD
# ==============================================================================
pat  <- readRDS("processed/text/df_jobs_patched.rds")
disc <- readRDS("raw/data/disclosure/h2a_combined.rds")

cat(sprintf("Patched rows       : %s\n", format(nrow(pat),  big.mark=",")))
cat(sprintf("Disclosure rows    : %s\n", format(nrow(disc), big.mark=",")))

# ==============================================================================
# 2. SELECT SINGLE-VALUED DISCLOSURE FIELDS TO CARRY OVER
# ==============================================================================
# Exclude: worksite_address/city/state/zip/county (primary only — we have PDF-parsed)
#          housing_address/city/state/zip/county (same reason)
#          Addendum B details (use df_locations.rds)
disc_slim <- disc |>
  select(
    caseNumber          = job_order_number,
    # employer
    d_empFein           = employer_fein,
    d_empName           = employer_name,
    d_empState          = employer_state,
    d_empNaics          = naics_code,
    # occupation
    d_socCode           = soc_code,
    d_socTitle          = soc_title,
    # workers
    d_wrksRequested     = nbr_workers_requested,
    d_wrksCertified     = nbr_workers_certified,
    d_totalWrksNeeded   = total_workers_needed,
    # wage
    d_wageOffer         = basic_rate_of_pay,
    d_wagePer           = basic_unit_of_pay,
    d_pieceRate         = piece_rate_offer,
    # dates
    d_caseReceived      = case_received_date,
    d_certBegin         = certification_begin_date,
    d_certEnd           = certification_end_date,
    d_jobStart          = job_start_date,
    d_jobEnd            = job_end_date,
    # job details
    d_jobTitle          = job_title,
    d_primaryCrop       = primary_crop,
    d_caseStatus        = case_status,
    d_fiscalYear        = fiscal_year,
    d_housingType       = type_of_housing,
    d_addmBAttached     = addendum_b_attached,
    d_addmAAttached     = addendum_a_attached,
    # H-2A contractor flag
    d_h2aContractor     = h2a_labor_contractor,
    d_agAssoc           = ag_assn_or_agency_status
  ) |>
  # normalize FEIN: remove dashes, keep 9-digit string
  mutate(
    d_empFein   = str_remove_all(as.character(d_empFein), "-"),
    d_empFein   = if_else(nchar(d_empFein) == 9, d_empFein, NA_character_),
    d_wageOffer = suppressWarnings(as.numeric(d_wageOffer)),
    d_wrksRequested   = suppressWarnings(as.integer(d_wrksRequested)),
    d_wrksCertified   = suppressWarnings(as.integer(d_wrksCertified)),
    d_totalWrksNeeded = suppressWarnings(as.integer(d_totalWrksNeeded))
  )

# normalize patched FEIN to same format (remove dashes)
pat <- pat |>
  mutate(empFein = str_remove_all(as.character(coalesce(empFein, "")), "-"),
         empFein = if_else(nchar(empFein) == 9, empFein, NA_character_))

# ==============================================================================
# 3. JOIN DISCLOSURE INTO PATCHED — coalesce (keep existing, fill NAs)
# ==============================================================================
enriched <- pat |>
  left_join(disc_slim, by = "caseNumber") |>
  mutate(
    # fill key fields from disclosure where patched has NA
    empFein       = coalesce(empFein, d_empFein),
    empBusinessName = coalesce(empBusinessName, d_empName),
    empNaics      = coalesce(as.character(empNaics), d_empNaics),
    socCode       = coalesce(socCode, d_socCode),
    socTitle      = coalesce(socTitle, d_socTitle),
    jobWrksNeeded = coalesce(jobWrksNeeded, d_wrksRequested),
    jobWageOffer  = coalesce(jobWageOffer, d_wageOffer),
    jobWagePer    = coalesce(jobWagePer, d_wagePer),
    jobPieceRate  = coalesce(jobPieceRate, d_pieceRate),
    jobBeginDate  = coalesce(jobBeginDate, d_certBegin, d_jobStart),
    jobEndDate    = coalesce(jobEndDate, d_certEnd, d_jobEnd),
    # new fields from disclosure
    wrksCertified = d_wrksCertified,
    primaryCrop   = d_primaryCrop,
    caseStatus    = d_caseStatus,
    fiscalYear    = d_fiscalYear,
    housingType   = coalesce(housingType, d_housingType),
    h2aContractor = d_h2aContractor,
    agAssoc       = d_agAssoc,
    # wage normalization (may have been filled from disclosure)
    jobWageHourly = case_when(
      str_to_title(jobWagePer) == "Hour"  ~ jobWageOffer,
      str_to_title(jobWagePer) == "Week"  ~ jobWageOffer / 40,
      str_to_title(jobWagePer) == "Month" ~ jobWageOffer / (52 / 12 * 40),
      str_to_title(jobWagePer) == "Year"  ~ jobWageOffer / (52 * 40),
      TRUE ~ jobWageHourly
    )
  ) |>
  select(-starts_with("d_"))

# ==============================================================================
# 4. ADD DISCLOSURE-ONLY RECORDS (not in patched at all)
# ==============================================================================
disc_only_ids <- setdiff(disc_slim$caseNumber, pat$caseNumber)
cat(sprintf("\nDisclosure-only cases to add: %s\n", format(length(disc_only_ids), big.mark=",")))

disc_only <- disc |>
  filter(job_order_number %in% disc_only_ids) |>
  transmute(
    caseNumber      = job_order_number,
    dateSubmitted   = as.Date(case_received_date),
    empFein         = str_remove_all(as.character(employer_fein), "-"),
    empFein         = if_else(nchar(empFein) == 9, empFein, NA_character_),
    empBusinessName = employer_name,
    empTradeName    = trade_name_dba,
    empNaics        = naics_code,
    socCode         = soc_code,
    socTitle        = soc_title,
    jobWageOffer    = suppressWarnings(as.numeric(basic_rate_of_pay)),
    jobWagePer      = basic_unit_of_pay,
    jobPieceRate    = suppressWarnings(as.numeric(piece_rate_offer)),
    jobDuties       = NA_character_,
    jobWrksNeeded   = suppressWarnings(as.integer(nbr_workers_requested)),
    jobBeginDate    = as.Date(coalesce(certification_begin_date, job_start_date)),
    jobEndDate      = as.Date(coalesce(certification_end_date, job_end_date)),
    jobHoursTotal   = NA_real_,
    jobCity         = worksite_city,
    jobState        = worksite_state,
    jobCounty       = worksite_county,
    jobPostcode     = as.character(worksite_postal_code),
    jobWageHourly   = case_when(
      basic_unit_of_pay == "Hour"  ~ suppressWarnings(as.numeric(basic_rate_of_pay)),
      basic_unit_of_pay == "Week"  ~ suppressWarnings(as.numeric(basic_rate_of_pay)) / 40,
      basic_unit_of_pay == "Month" ~ suppressWarnings(as.numeric(basic_rate_of_pay)) / (52/12*40),
      basic_unit_of_pay == "Year"  ~ suppressWarnings(as.numeric(basic_rate_of_pay)) / (52*40),
      TRUE ~ NA_real_
    ),
    source          = "disclosure",
    # address fields
    worksiteAddr    = worksite_address,
    housingAddr     = housing_address,
    housingCity     = housing_city,
    housingState    = housing_state,
    housingZip      = as.character(housing_postal_code),
    housingCounty   = housing_county,
    housingType     = type_of_housing,
    # new disclosure fields
    wrksCertified   = suppressWarnings(as.integer(nbr_workers_certified)),
    primaryCrop     = primary_crop,
    caseStatus      = case_status,
    fiscalYear      = fiscal_year,
    h2aContractor   = h2a_labor_contractor,
    agAssoc         = ag_assn_or_agency_status
  )

# ==============================================================================
# 5. BIND AND DEDUPLICATE
# ==============================================================================
enriched_full <- bind_rows(enriched, disc_only) |>
  arrange(caseNumber, desc(source == "json")) |>
  distinct(caseNumber, .keep_all = TRUE)

cat(sprintf("\n=== Enrichment Summary ===\n"))
cat(sprintf("Patched rows (in)        : %s\n", format(nrow(pat),          big.mark=",")))
cat(sprintf("Disclosure-only added    : %s\n", format(nrow(disc_only),    big.mark=",")))
cat(sprintf("Final enriched rows      : %s\n", format(nrow(enriched_full),big.mark=",")))
cat(sprintf("\nSource breakdown:\n"))
enriched_full |> count(source, sort=TRUE) |> print()

cat("\n--- Field coverage (enriched) ---\n")
key_cols <- c("empFein","empBusinessName","empNaics","socCode","jobWageOffer",
              "jobBeginDate","jobEndDate","jobWrksNeeded","wrksCertified",
              "primaryCrop","caseStatus","worksiteAddr","housingAddr","housingType")
for (col in key_cols) {
  if (col %in% names(enriched_full)) {
    n_ok <- sum(!is.na(enriched_full[[col]]) & enriched_full[[col]] != "")
    cat(sprintf("  %-20s: %s (%.1f%%)\n", col, format(n_ok, big.mark=","),
                n_ok / nrow(enriched_full) * 100))
  }
}

# FY gap coverage check
cat("\nFiling counts Nov 2023 – Mar 2024:\n")
enriched_full |>
  filter(!is.na(dateSubmitted),
         dateSubmitted >= as.Date("2023-10-01"),
         dateSubmitted <= as.Date("2024-03-31")) |>
  mutate(mo = floor_date(dateSubmitted, "month")) |>
  count(mo) |> arrange(mo) |> print()

# ==============================================================================
# 6. SAVE
# ==============================================================================
saveRDS(enriched_full, "processed/text/df_jobs_enriched.rds")
cat("\nSaved: processed/text/df_jobs_enriched.rds\n")
