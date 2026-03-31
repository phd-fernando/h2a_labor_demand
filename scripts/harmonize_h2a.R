# ==============================================================================
# H-2A Disclosure Data Harmonization
# Reads all FY2006-FY2024 waves (xlsx), normalizes column names and types, appends.
# ==============================================================================
library(readr)
library(readxl)
library(dplyr)

# mydir must be set in the calling environment, e.g.:
# mydir <- "C:/Users/Fer/UF Dropbox/.../naws_ces_claude"

# ------------------------------------------------------------------------------
# Rename map: raw column name (upper-cased) → canonical snake_case name
# Covers all naming variants across FY2008-FY2025.
# Only columns with cross-year name variants need to appear here;
# unrecognised names fall through to tolower().
# ------------------------------------------------------------------------------
rename_map <- c(
  # --- Case identifiers -------------------------------------------------------
  "CASE_NO"                          = "case_number",       # FY2008-FY2014, FY2018
  "CASE_NUMBER"                      = "case_number",       # FY2015-17, FY2019-25

  # --- Visa / application type ------------------------------------------------
  "APPLICATION_TYPE"                 = "visa_class",        # FY2008-FY2011
  "VISA_CLASS"                       = "visa_class",        # FY2012-FY2019

  # --- Case status / received / decision --------------------------------------
  "CASE_STATUS"                      = "case_status",
  "CASE_RECEIVED_DATE"               = "case_received_date",  # FY2008-FY2019
  "RECEIVED_DATE"                    = "case_received_date",  # FY2020+
  "DECISION_DATE"                    = "decision_date",

  # --- Requested period -------------------------------------------------------
  "REQUESTED_START_DATE_OF_NEED"     = "requested_start_date",  # FY2009-FY2019
  "REQUESTED_BEGIN_DATE"             = "requested_start_date",  # FY2020+
  "REQUESTED_END_DATE_OF_NEED"       = "requested_end_date",    # FY2009-FY2019
  "REQUESTED_END_DATE"               = "requested_end_date",    # FY2020+

  # --- Certification / employment period --------------------------------------
  "CERTIFICATION_BEGIN_DATE"         = "certification_begin_date",  # FY2008-FY2019
  "EMPLOYMENT_BEGIN_DATE"            = "certification_begin_date",  # FY2020+
  "CERTIFICATION_END_DATE"           = "certification_end_date",    # FY2008-FY2019
  "EMPLOYMENT_END_DATE"              = "certification_end_date",    # FY2020+

  # --- Employer ---------------------------------------------------------------
  "EMPLOYER_NAME"                    = "employer_name",
  "TRADE_NAME_DBA"                   = "trade_name_dba",
  "EMPLOYER_ADDRESS1"                = "employer_address1",   # FY2008-FY2019
  "EMPLOYER_ADDRESS_1"               = "employer_address1",   # FY2020+
  "EMPLOYER_ADDRESS2"                = "employer_address2",   # FY2008-FY2019
  "EMPLOYER_ADDRESS_2"               = "employer_address2",   # FY2020+
  "EMPLOYER_CITY"                    = "employer_city",
  "EMPLOYER_STATE"                   = "employer_state",
  "EMPLOYER_POSTAL_CODE"             = "employer_postal_code",
  "EMPLOYER_COUNTRY"                 = "employer_country",
  "EMPLOYER_PROVINCE"                = "employer_province",
  "EMPLOYER_PHONE"                   = "employer_phone",
  "EMPLOYER_PHONE_EXT"               = "employer_phone_ext",
  "EMPLOYER_FEIN"                    = "employer_fein",

  # --- Industry code ----------------------------------------------------------
  "NAIC_CODE"                        = "naics_code",          # FY2015-FY2016 (source typo)
  "NAICS_CODE"                       = "naics_code",          # FY2017+

  # --- Primary / sub-contractor status ----------------------------------------
  "PRIMARY/SUB"                      = "primary_sub",         # FY2016-FY2017
  "PRMARY/SUB"                       = "primary_sub",         # FY2019 (source typo)
  "PRIMARY_SUB"                      = "primary_sub",         # FY2018
  "TYPE_OF_EMPLOYER_APPLICATION"     = "primary_sub",         # FY2020+ closest equivalent
  "H2A_LABOR_CONTRACTOR"             = "h2a_labor_contractor",
  "AG_ASSN_OR_AGENCY_STATUS"         = "ag_assn_or_agency_status",
  "ORGANIZATION_FLAG"                = "organization_flag",

  # --- Law firm / agent / attorney (early years) ------------------------------
  "LAWFIRM_NAME"                     = "lawfirm_name",        # FY2014-FY2019
  "LAWFIRM_NAME_BUSINESS_NAME"       = "lawfirm_name",        # FY2020+
  "LAWFIRM_BUSINESS_FEIN"            = "lawfirm_business_fein",
  "AGENT_ATTORNEY_NAME"              = "agent_attorney_name",
  "AGENT_ATTORNEY_ADDRESS1"          = "attorney_agent_address1",
  "AGENT_ATTORNEY_ADDRESS2"          = "attorney_agent_address2",
  "AGENT_ATTORNEY_CITY"              = "agent_attorney_city",
  "AGENT_ATTORNEY_STATE"             = "agent_attorney_state",
  "AGENT_ATTORNEY_POSTAL_CODE"       = "agent_attorney_postal_code",

  # --- Attorney / agent (FY2020+ redesigned form) ----------------------------
  "ATTORNEY_AGENT_LAST_NAME"         = "attorney_agent_last_name",
  "ATTORNEY_AGENT_FIRST_NAME"        = "attorney_agent_first_name",
  "ATTORNEY_AGENT_MIDDLE_NAME"       = "attorney_agent_middle_name",
  "ATTORNEY_AGENT_ADDRESS_1"         = "attorney_agent_address1",
  "ATTORNEY_AGENT_ADDRESS_2"         = "attorney_agent_address2",
  "ATTORNEY_AGENT_CITY"              = "attorney_agent_city",
  "ATTORNEY_AGENT_STATE"             = "attorney_agent_state",
  "ATTORNEY_AGENT_POSTAL_CODE"       = "attorney_agent_postal_code",
  "ATTORNEY_AGENT_COUNTRY"           = "attorney_agent_country",
  "ATTORNEY_AGENT_PHONE"             = "attorney_agent_phone",
  "ATTORNEY_AGENT_PHONE_EXT"         = "attorney_agent_phone_ext",
  "ATTORNEY_AGENT_EMAIL"             = "attorney_agent_email",
  "ATTORNEY_AGENT_EMAIL_ADDRESS"     = "attorney_agent_email",     # FY2021 variant
  "AGENT_POC_EMPLOYER_REP_BY_AGENT"  = "employer_rep_by_agent",    # FY2017-FY2018
  "EMPLOYER_REP_BY_AGENT"            = "employer_rep_by_agent",    # FY2019
  "TYPE_OF_REPRESENTATION"           = "type_of_representation",
  "STATE_OF_HIGHEST_COURT"           = "state_of_highest_court",
  "NAME_OF_HIGHEST_STATE_COURT"      = "name_of_highest_state_court",

  # --- Employer point of contact (FY2020+) ------------------------------------
  "EMPLOYER_POC_LAST_NAME"           = "employer_poc_last_name",
  "EMPLOYER_POC_FIRST_NAME"          = "employer_poc_first_name",
  "EMPLOYER_POC_MIDDLE_NAME"         = "employer_poc_middle_name",
  "EMPLOYER_POC_JOB_TITLE"           = "employer_poc_job_title",
  "EMPLOYER_POC_ADDRESS1"            = "employer_poc_address1",
  "EMPLOYER_POC_ADDRESS2"            = "employer_poc_address2",
  "EMPLOYER_POC_CITY"                = "employer_poc_city",
  "EMPLOYER_POC_STATE"               = "employer_poc_state",
  "EMPLOYER_POC_POSTAL_CODE"         = "employer_poc_postal_code",
  "EMPLOYER_POC_COUNTRY"             = "employer_poc_country",
  "EMPLOYER_POC_PROVINCE"            = "employer_poc_province",
  "EMPLOYER_POC_PHONE"               = "employer_poc_phone",
  "EMPLOYER_POC_PHONE_EXT"           = "employer_poc_phone_ext",
  "EMPLOYER_POC_EMAIL"               = "employer_poc_email",

  # --- Preparer (FY2020+) -----------------------------------------------------
  "PREPARER_LAST_NAME"               = "preparer_last_name",
  "PREPARER_FIRST_NAME"              = "preparer_first_name",
  "PREPARER_MIDDLE_INITIAL"          = "preparer_middle_initial",
  "PREPARER_BUSINESS_NAME"           = "preparer_business_name",
  "PREPARER_EMAIL"                   = "preparer_email",
  "PREPARER_FEIN"                    = "preparer_fein",

  # --- Job description --------------------------------------------------------
  "JOB_TITLE"                        = "job_title",
  "SOC_CODE_ID"                      = "soc_code",             # FY2014
  "SOC_CODE"                         = "soc_code",             # FY2015+
  "SOC_TITLE"                        = "soc_title",
  "PRIMARY_CROP"                     = "primary_crop",

  # --- Job order / SWA --------------------------------------------------------
  "SWA_NAME"                         = "swa_name",
  "JOB_IDNUMBER"                     = "job_order_number",      # FY2015-FY2019
  "JOB_ORDER_NUMBER"                 = "job_order_number",      # FY2020+
  "JOB_START_DATE"                   = "job_start_date",
  "JOB_END_DATE"                     = "job_end_date",

  # --- Worker counts ----------------------------------------------------------
  "NBR_WORKERS_REQUSTED"             = "nbr_workers_requested", # FY2008 source typo
  "NBR_WORKERS_REQUESTED"            = "nbr_workers_requested", # FY2009-FY2019
  "TOTAL_WORKERS_H2A_REQUESTED"      = "nbr_workers_requested", # FY2020+
  "TOTAL_WORKERS_NEEDED"             = "total_workers_needed",
  "NBR_WORKERS_CERTIFIED"            = "nbr_workers_certified", # FY2008-FY2019
  "TOTAL_WORKERS_H2A_CERTIFIED"      = "nbr_workers_certified", # FY2020+

  # --- Employment conditions --------------------------------------------------
  "FULL_TIME_POSITION"               = "full_time_position",   # FY2015-17, FY2019
  "FULL_TIME"                        = "full_time_position",   # FY2018
  "NATURE_OF_TEMPORARY_NEED"         = "nature_of_temporary_need",
  "EMERGENCY_FILING"                 = "emergency_filing",
  "ON_CALL_REQUIREMENT"              = "on_call_requirement",

  # --- Hours / schedule -------------------------------------------------------
  "BASIC_NBR_HOURS"                  = "basic_number_of_hours", # FY2008
  "BASIC_HOURS_PER_WEEK"             = "basic_number_of_hours", # FY2009
  "BASIC_NUMBER_OF_HOURS"            = "basic_number_of_hours", # FY2011-FY2019
  "ANTICIPATED_NUMBER_OF_HOURS"      = "basic_number_of_hours", # FY2020+
  "HOURLY_WORK_SCHEDULE_AM"          = "hourly_schedule_start", # FY2015-FY2019
  "HOURLY_WORK_SCHEDULE_START"       = "hourly_schedule_start", # FY2020
  "HOURLY_SCHEDULE_BEGIN"            = "hourly_schedule_start", # FY2021+
  "HOURLY_WORK_SCHEDULE_PM"          = "hourly_schedule_end",   # FY2015-FY2019
  "HOURLY_WORK_SCHEDULE_END"         = "hourly_schedule_end",   # FY2020
  "HOURLY_SCHEDULE_END"              = "hourly_schedule_end",   # FY2021+
  "SUNDAY_HOURS"                     = "sunday_hours",
  "MONDAY_HOURS"                     = "monday_hours",
  "TUESDAY_HOURS"                    = "tuesday_hours",
  "WEDNESDAY_HOURS"                  = "wednesday_hours",
  "THURSDAY_HOURS"                   = "thursday_hours",
  "FRIDAY_HOURS"                     = "friday_hours",
  "SATURDAY_HOURS"                   = "saturday_hours",

  # --- Pay --------------------------------------------------------------------
  "WAGE_RATE_PER"                    = "basic_unit_of_pay",    # FY2008
  "BASIC_UNIT_OF_PAY"                = "basic_unit_of_pay",    # FY2009-FY2019
  "PER"                              = "basic_unit_of_pay",    # FY2020+
  "BASIC_RATE_OF_PAY"                = "basic_rate_of_pay",    # FY2008-FY2019
  "WAGE_OFFER"                       = "basic_rate_of_pay",    # FY2020+
  "OVERTIME_RATE_FROM"               = "overtime_rate_from",
  "OVERTIME_RATE_TO"                 = "overtime_rate_to",
  "OVERTIME_PAY"                     = "overtime_pay",
  "PIECE_RATE_OFFER"                 = "piece_rate_offer",
  "PIECE_RATE_UNIT"                  = "piece_rate_unit",
  "PIECE_RATE_ADDITIONAL_INFO"       = "piece_rate_additional_info",
  "FREQUENCY_OF_PAY"                 = "frequency_of_pay",
  "OTHER_FREQUENCY_OF_PAY"           = "other_frequency_of_pay",
  "DEDUCTIONS_FROM_PAY"              = "deductions_from_pay",
  "MIN_STANDARDS"                    = "min_standards",

  # --- Education / experience / training --------------------------------------
  "EDUCATION_LEVEL"                  = "education_level",
  "OTHER_EDU"                        = "other_edu",
  "MAJOR"                            = "major",
  "SECOND_DIPLOMA"                   = "second_diploma",
  "SECOND_DIPLOMA_MAJOR"             = "second_diploma_major",
  "EMP_EXPERIENCE_REQD"              = "emp_experience_reqd",
  "EMP_EXP_NUM_MONTHS"               = "emp_exp_num_months",
  "WORK_EXPERIENCE_MONTHS"           = "emp_exp_num_months",   # FY2020+
  "TRAINING_REQ"                     = "training_req",
  "NUM_MONTHS_TRAINING"              = "num_months_training",  # FY2008-FY2019
  "TRAINING_MONTHS"                  = "num_months_training",  # FY2020+
  "NAME_REQD_TRAINING"               = "name_reqd_training",
  "CERTIFICATION_REQUIREMENTS"       = "certification_requirements",

  # --- Physical requirements (FY2020+) ----------------------------------------
  "DRIVER_REQUIREMENTS"              = "driver_requirements",
  "CRIMINAL_BACKGROUND_CHECK"        = "criminal_background_check",
  "DRUG_SCREEN"                      = "drug_screen",
  "LIFTING_REQUIREMENTS"             = "lifting_requirements",
  "LIFTING_AMOUNT"                   = "lifting_amount",
  "EXPOSURE_TO_TEMPERATURES"         = "exposure_to_temperatures",
  "EXTENSIVE_PUSHING_PULLING"        = "extensive_pushing_pulling",
  "EXTENSIVE_SITTING_WALKING"        = "extensive_sitting_walking",
  "FREQUENT_STOOPING_BENDING_OVER"   = "frequent_stooping_bending_over",
  "REPETITIVE_MOVEMENTS"             = "repetitive_movements",

  # --- Supervision ------------------------------------------------------------
  "SUPERVISE_OTHER_EMP"              = "supervise_other_emp",
  "SUPERVISE_HOW_MANY"               = "supervise_how_many",
  "ADDITIONAL_JOB_REQUIREMENTS"      = "additional_job_requirements",
  "SPECIAL_REQUIREMENTS"             = "additional_job_requirements",  # FY2021 variant

  # --- Worksite ---------------------------------------------------------------
  "ALIEN_WORK_CITY"                  = "worksite_city",        # FY2008-FY2013
  "WORKSITE_LOCATION_CITY"           = "worksite_city",        # FY2014
  "WORKSITE_CITY"                    = "worksite_city",        # FY2015+
  "ALIEN_WORK_STATE"                 = "worksite_state",       # FY2008-FY2013
  "WORKSITE_LOCATION_STATE"          = "worksite_state",       # FY2014
  "WORKSITE_STATE"                   = "worksite_state",       # FY2015+
  "WORKSITE_COUNTY"                  = "worksite_county",
  "WORKSITE_POSTAL_CODE"             = "worksite_postal_code",
  "WORKSITE_ADDRESS"                 = "worksite_address",
  "ADDENDUM_B_WORKSITE_ATTACHED"     = "addendum_b_worksite_attached",
  "TOTAL_WORKSITE_RECORDS"           = "total_worksite_records",  # FY2024-FY2025
  "TOTAL_WORKSITES_RECORDS"          = "total_worksite_records",  # FY2022-FY2023

  # --- Housing ----------------------------------------------------------------
  "HOUSING_ADDRESS_LOCATION"         = "housing_address",
  "HOUSING_CITY"                     = "housing_city",
  "HOUSING_STATE"                    = "housing_state",
  "HOUSING_POSTAL_CODE"              = "housing_postal_code",
  "HOUSING_COUNTY"                   = "housing_county",
  "TYPE_OF_HOUSING"                  = "type_of_housing",
  "TOTAL_UNITS"                      = "housing_total_units",
  "TOTAL_OCCUPANCY"                  = "housing_total_occupancy",
  "HOUSING_COMPLIANCE_LOCAL"         = "housing_compliance_local",
  "HOUSING_COMPLIANCE_SWA"           = "housing_compliance_swa",
  "HOUSING_COMPLIANCE_STATE"         = "housing_compliance_state",
  "HOUSING_COMPLIANCE_FEDERAL"       = "housing_compliance_federal",
  "HOUSING_COMPLIANCE_OTHER"         = "housing_compliance_other",
  "HOUSING_COMPLIANCE_OTHER_SPECIFY" = "housing_compliance_other_specify",
  "HOUSING_TRANSPORTATION"           = "housing_transportation",
  "ADDENDUM_B_HOUSING_ATTACHED"      = "addendum_b_housing_attached",
  "TOTAL_HOUSING_RECORDS"            = "total_housing_records",
  "EMPLOYER_MSPA_ATTACHED"           = "employer_mspa_attached",
  "SURETY_BOND_ATTACHED"             = "surety_bond_attached",

  # --- Meals ------------------------------------------------------------------
  "MEALS_PROVIDED"                   = "meals_provided",
  "MEALS_CHARGED"                    = "meals_charged",
  "MEALS_CHARGE"                     = "meals_charged",         # FY2021 variant
  "MEAL_REIMBURSEMENT_MINIMUM"       = "meal_reimbursement_min",
  "MEAL_REIMBURSEMENT_MAXIMUM"       = "meal_reimbursement_max",

  # --- Application to apply ---------------------------------------------------
  "PHONE_TO_APPLY"                   = "phone_to_apply",
  "PHONE_EXT_TO_APPLY"               = "phone_ext_to_apply",
  "EMAIL_TO_APPLY"                   = "email_to_apply",
  "WEBSITE_TO_APPLY"                 = "website_to_apply",

  # --- Addenda / attachments --------------------------------------------------
  "APPENDIX_A_ATTACHED"              = "appendix_a_attached",
  "APPENDIX_C_ATTACHED"              = "appendix_c_attached",
  "APPENDIX_D_ATTACHED"              = "appendix_d_attached",
  "ADDENDUM_C_ATTACHED"              = "addendum_c_attached",
  "790A_ADDENDUM_A_ATTACHED"         = "addendum_a_attached",
  "790A_ADDENDUM_B_ATTACHED"         = "addendum_b_attached",
  "JOINT_EMPLOYER_APPENDIX_A_ATTACHED" = "joint_employer_appendix_a_attached",
  "WORK_CONTRACTS_ATTACHED"          = "work_contracts_attached",
  "USING_AGENT_RECRUITER"            = "using_agent_recruiter",
  "AGENT_RECRUITER_AGREEMENT_ATTACHED" = "agent_recruiter_agreement_attached",
  "TOTAL_ADDENDUM_A_RECORDS"         = "total_addendum_a_records"
)

# ------------------------------------------------------------------------------
# Helper: parse fiscal year from filename
# Handles both 4-digit (FY2025) and 2-digit (FY17, FY16) variants.
# ------------------------------------------------------------------------------
extract_fy <- function(path) {
  nm <- basename(path)
  m  <- regmatches(nm, regexpr("FY(\\d{2,4})", nm, ignore.case = TRUE))
  if (length(m) == 0) return(NA_integer_)
  yr <- as.integer(gsub("[^0-9]", "", m))
  if (yr < 100) yr <- 2000L + yr   # FY17 → 2017
  yr
}

# ------------------------------------------------------------------------------
# Helper: coerce to Date
# xlsx dates arrive as Excel numeric serials (e.g. 45123); some cells are text.
# Tries numeric serial first, then common text formats, then ISO.
# ------------------------------------------------------------------------------
coerce_date <- function(x) {
  if (all(is.na(x))) return(base::as.Date(NA_character_))
  n   <- suppressWarnings(as.numeric(x))
  out <- base::as.Date(floor(ifelse(is.na(n), NA_real_, n)), origin = "1899-12-30")
  # for text cells (non-numeric): strip time component and try text formats
  txt <- which(is.na(n) & !is.na(x))
  if (length(txt) > 0) {
    x_clean <- sub("\\s+\\d{1,2}:\\d{2}.*$", "", x[txt])
    d_text  <- tryCatch(
      base::as.Date(x_clean, tryFormats = c("%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y", "%Y-%m-%d")),
      error = function(e) rep(base::as.Date(NA), length(x_clean))
    )
    out[txt] <- d_text
  }
  out
}

# ------------------------------------------------------------------------------
# Helper: strip leading "$" or "," and coerce to numeric
# ------------------------------------------------------------------------------
parse_dollar <- function(x) suppressWarnings(as.numeric(gsub("[$,]", "", x)))

# ------------------------------------------------------------------------------
# Main: read one xlsx, normalize column names + types, tag fiscal year
# ------------------------------------------------------------------------------
process_file <- function(filepath) {
  fy <- extract_fy(filepath)

  # Read all columns as text to avoid type conflicts across waves
  df <- read_excel(filepath, col_types = "text")

  # Normalize raw names → canonical names
  raw       <- toupper(trimws(names(df)))
  canonical <- sapply(raw, function(n) {
    if (n %in% names(rename_map)) rename_map[[n]] else tolower(n)
  }, USE.NAMES = FALSE)
  names(df) <- canonical

  # Drop duplicate columns (keep first occurrence if two mapped to same name)
  dup <- duplicated(names(df))
  if (any(dup)) df <- df[, !dup, drop = FALSE]

  df$fiscal_year <- fy

  # --- Date columns -----------------------------------------------------------
  date_cols <- c("case_received_date", "decision_date",
                 "requested_start_date", "requested_end_date",
                 "certification_begin_date", "certification_end_date",
                 "job_start_date", "job_end_date")
  for (col in intersect(date_cols, names(df))) {
    df[[col]] <- coerce_date(df[[col]])
  }

  # Nullify impossible dates (outside valid H-2A range ~1995-2106)
  min_date <- as.Date("1995-01-01")
  max_date <- as.Date("2106-01-01")
  for (col in intersect(date_cols, names(df))) {
    if (inherits(df[[col]], "Date")) {
      bad <- !is.na(df[[col]]) & (df[[col]] < min_date | df[[col]] > max_date)
      if (any(bad)) df[[col]][bad] <- NA
    }
  }

  # Fallback: use certification dates when requested dates are missing/absent
  if ("certification_begin_date" %in% names(df)) {
    if (!"requested_start_date" %in% names(df)) {
      df$requested_start_date <- df$certification_begin_date
    } else {
      na_rows <- is.na(df$requested_start_date)
      df$requested_start_date[na_rows] <- df$certification_begin_date[na_rows]
    }
  }
  if ("certification_end_date" %in% names(df)) {
    if (!"requested_end_date" %in% names(df)) {
      df$requested_end_date <- df$certification_end_date
    } else {
      na_rows <- is.na(df$requested_end_date)
      df$requested_end_date[na_rows] <- df$certification_end_date[na_rows]
    }
  }

  # --- Dollar-formatted pay / meal columns ------------------------------------
  # These arrive as "$14.83", "$15.46", etc. in CSV files.
  dollar_cols <- c("basic_rate_of_pay", "overtime_rate_from", "overtime_rate_to",
                   "meals_charged", "meal_reimbursement_min", "meal_reimbursement_max")
  for (col in intersect(dollar_cols, names(df))) {
    df[[col]] <- parse_dollar(df[[col]])
  }

  # --- Other numeric columns --------------------------------------------------
  num_cols <- c("nbr_workers_requested", "nbr_workers_certified", "total_workers_needed",
                "basic_number_of_hours",
                "sunday_hours", "monday_hours", "tuesday_hours",
                "wednesday_hours", "thursday_hours", "friday_hours", "saturday_hours",
                "piece_rate_offer",
                "supervise_how_many", "lifting_amount",
                "emp_exp_num_months", "num_months_training",
                "housing_total_units", "housing_total_occupancy",
                "total_worksite_records", "total_housing_records",
                "total_addendum_a_records")
  for (col in intersect(num_cols, names(df))) {
    df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
  }

  # employer_postal_code, worksite_postal_code, housing_postal_code,
  # employer_fein, attorney_agent_postal_code: kept as character
  # (ZIP codes can have leading zeros or dashes; FEINs may include dashes).

  df
}

# ------------------------------------------------------------------------------
# Main function: run harmonization pipeline for a given ddir (disclosure dir).
# Returns the combined data frame; also saves h2a_combined.rds to ddir.
# ------------------------------------------------------------------------------
harmonize_h2a <- function(ddir) {

  files_to_load <- c(
    paste0(ddir, "H-2A_Disclosure_Data_FY2024.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2023.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2022.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2021.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2020.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2019.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2018.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2017.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2016.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2015.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2014.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_H2A_FY2013.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2012.xlsx"),
    paste0(ddir, "H-2A_Disclosure_Data_FY2011.xlsx"),
    paste0(ddir, "H-2A_FY2010.xlsx"),
    paste0(ddir, "H2A_FY2009.xlsx"),
    paste0(ddir, "H2A_FY2008.xlsx"),
    paste0(ddir, "H-2A_FY2007.xlsx"),
    paste0(ddir, "H2a_ FY2006.xlsx")
  )

  cat(sprintf("Processing %d files...\n", length(files_to_load)))

  all_waves <- lapply(files_to_load, function(f) {
    cat("  Reading:", basename(f), "... ")
    result <- tryCatch(
      {
        out <- process_file(f)
        cat(format(nrow(out), big.mark = ","), "rows\n")
        out
      },
      error = function(e) {
        cat("ERROR:", conditionMessage(e), "\n")
        NULL
      }
    )
    result
  })

  all_waves <- Filter(Negate(is.null), all_waves)

  cat("\nAppending all waves (missing columns filled with NA)...\n")
  h2a <- bind_rows(all_waves)

  cat(sprintf("Combined dataset: %s rows x %s columns\n",
              format(nrow(h2a), big.mark = ","), ncol(h2a)))
  cat("Fiscal years covered:",
      paste(sort(unique(h2a$fiscal_year)), collapse = ", "), "\n")

  out_rds <- paste0(ddir, "h2a_combined.rds")
  saveRDS(h2a, out_rds)
  cat("Saved:", out_rds, "\n")

  h2a
}
