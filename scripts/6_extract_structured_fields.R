###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Extract structured fields from H-2A JSON job order files.
# Produces three linked flat tables:
#
#   df_jobs.rds       — one row per job order (wage, SOC, NAICS, employer)
#   df_crops.rds      — one row per crop/activity per job order
#   df_locations.rds  — one row per worksite per job order
#
# Key fields recovered:
#   Tasks    : socCode, socTitle, jobDuties
#   Wages    : jobWageOffer, jobWagePer, jobPieceRate, wage per crop
#   Locations: jobCity/State/County + all addendum-B worksites
#   Crops    : cropsAndActivities (crop name + crop-level wage)
#   Employer : empFein, empBusinessName, empTradeName
###############################################################################

cat("\014"); rm(list = ls())

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
out_dir <- paste0(mydir, "/processed/text")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "jsonlite")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# helper: NULL → NA
nn <- function(x) if (is.null(x) || length(x) == 0) NA else x[[1]]

# ------------------------------------------------------------------------------
# 1. Load JSON files
# ------------------------------------------------------------------------------
path  <- paste0(mydir, "/raw/data/jo/")
files <- sort(dir(path, pattern = "_jo.json", full.names = TRUE))
cat(sprintf("Processing %d JSON files...\n", length(files)))

df_jobs      <- list()
df_crops     <- list()
df_locations <- list()

t0 <- Sys.time()

for (fi in seq_along(files)) {

  raw <- tryCatch(
    fromJSON(files[fi], simplifyDataFrame = FALSE),
    error = function(e) { cat("  ERROR reading", basename(files[fi]), "\n"); NULL }
  )
  if (is.null(raw)) next

  for (jo in raw) {

    case  <- nn(jo$caseNumber)
    dtsub <- nn(jo$dateSubmitted)

    # ------------------------------------------------------------------
    # A. Jobs table — one row per job order
    # ------------------------------------------------------------------
    df_jobs[[length(df_jobs) + 1]] <- tibble(
      caseNumber      = case,
      dateSubmitted   = dtsub,
      # employer
      empFein         = nn(jo$empFein),
      empBusinessName = nn(jo$empBusinessName),
      empTradeName    = nn(jo$empTradeName),
      # occupation / industry
      socCode         = nn(jo$socCode),
      socTitle        = nn(jo$socTitle),
      empNaics        = nn(jo$empNaics),
      # wage
      jobWageOffer    = suppressWarnings(as.numeric(nn(jo$jobWageOffer))),
      jobWagePer      = nn(jo$jobWagePer),
      jobPieceRate    = suppressWarnings(as.numeric(nn(jo$jobPieceRate))),
      # job details
      jobDuties       = nn(jo$jobDuties),
      jobWrksNeeded   = suppressWarnings(as.integer(nn(jo$jobWrksNeeded))),
      jobBeginDate    = nn(jo$jobBeginDate),
      jobEndDate      = nn(jo$jobEndDate),
      jobHoursTotal   = suppressWarnings(as.numeric(nn(jo$jobHoursTotal))),
      # primary worksite
      jobCity         = nn(jo$jobCity),
      jobState        = nn(jo$jobState),
      jobCounty       = nn(jo$jobCounty),
      jobPostcode     = as.character(nn(jo$jobPostcode))
    )

    # ------------------------------------------------------------------
    # B. Crops table — one row per crop/activity per job order
    # ------------------------------------------------------------------
    if (length(jo$cropsAndActivities) > 0) {
      for (ca in jo$cropsAndActivities) {
        crop_name <- nn(ca$addmaCropActivity)
        if (!is.na(crop_name) && nchar(trimws(crop_name)) > 0) {
          df_crops[[length(df_crops) + 1]] <- tibble(
            caseNumber       = case,
            dateSubmitted    = dtsub,
            empFein          = nn(jo$empFein),
            cropActivity     = trimws(toupper(crop_name)),
            cropWageOffer    = suppressWarnings(as.numeric(nn(ca$addmaWageOffer))),
            cropPayPer       = nn(ca$addmaPayPer),
            cropAdditional   = nn(ca$addmaAdditionalInfo)
          )
        }
      }
    }

    # ------------------------------------------------------------------
    # C. Locations table — one row per worksite per job order
    # ------------------------------------------------------------------
    if (length(jo$employmentLocations) > 0) {
      for (loc in jo$employmentLocations) {
        df_locations[[length(df_locations) + 1]] <- tibble(
          caseNumber      = case,
          dateSubmitted   = dtsub,
          empFein         = nn(jo$empFein),
          locBizName      = nn(loc$addmbEmpBuzname),
          locAddr1        = nn(loc$addmbEmpAddr1),
          locCity         = nn(loc$addmbEmpCity),
          locState        = nn(loc$addmbEmpState),
          locPostcode     = as.character(nn(loc$addmbEmpPostcode)),
          locCounty       = nn(loc$addmbEmpCounty),
          locBeginDate    = nn(loc$addmbEmpBeginDate),
          locEndDate      = nn(loc$addmbEmpEndDate),
          locTotalWorkers = suppressWarnings(as.integer(nn(loc$addmbEmpTotalWrks))),
          locAdditional   = nn(loc$addmbEmpAdditionalInfo)
        )
      }
    } else {
      # fall back to primary worksite when addendum B is absent
      df_locations[[length(df_locations) + 1]] <- tibble(
        caseNumber      = case,
        dateSubmitted   = dtsub,
        empFein         = nn(jo$empFein),
        locBizName      = nn(jo$empBusinessName),
        locAddr1        = NA_character_,
        locCity         = nn(jo$jobCity),
        locState        = nn(jo$jobState),
        locPostcode     = as.character(nn(jo$jobPostcode)),
        locCounty       = nn(jo$jobCounty),
        locBeginDate    = nn(jo$jobBeginDate),
        locEndDate      = nn(jo$jobEndDate),
        locTotalWorkers = suppressWarnings(as.integer(nn(jo$jobWrksNeeded))),
        locAdditional   = NA_character_
      )
    }

  } # end job order loop

  cat(sprintf("  [%d/%d] %s  |  jobs: %d  crops: %d  locs: %d  (%.1f min)\n",
              fi, length(files), basename(files[fi]),
              length(df_jobs), length(df_crops), length(df_locations),
              as.numeric(Sys.time() - t0, units = "mins")))
}

# ------------------------------------------------------------------------------
# 2. Bind and clean
# ------------------------------------------------------------------------------
df_jobs      <- bind_rows(df_jobs)
df_crops     <- bind_rows(df_crops)
df_locations <- bind_rows(df_locations)

# parse dates
parse_jo_date <- function(x) {
  suppressWarnings(as.Date(x, tryFormats = c("%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y")))
}
df_jobs      <- df_jobs      %>% mutate(across(c(dateSubmitted, jobBeginDate, jobEndDate), parse_jo_date))
df_crops     <- df_crops     %>% mutate(dateSubmitted = parse_jo_date(dateSubmitted))
df_locations <- df_locations %>% mutate(across(c(dateSubmitted, locBeginDate, locEndDate), parse_jo_date))

# normalize wage to hourly equivalent
df_jobs <- df_jobs %>%
  mutate(
    jobWageHourly = case_when(
      jobWagePer == "Hour"  ~ jobWageOffer,
      jobWagePer == "Week"  ~ jobWageOffer / 40,
      jobWagePer == "Month" ~ jobWageOffer / (52 / 12 * 40),
      jobWagePer == "Year"  ~ jobWageOffer / (52 * 40),
      TRUE                  ~ NA_real_
    )
  )

df_crops <- df_crops %>%
  mutate(
    cropWageHourly = case_when(
      cropPayPer == "Hour"  ~ cropWageOffer,
      cropPayPer == "Week"  ~ cropWageOffer / 40,
      cropPayPer == "Month" ~ cropWageOffer / (52 / 12 * 40),
      cropPayPer == "Year"  ~ cropWageOffer / (52 * 40),
      TRUE                  ~ NA_real_
    )
  )

# ------------------------------------------------------------------------------
# 3. Summary
# ------------------------------------------------------------------------------
cat("\n=== Summary ===\n")
cat(sprintf("Job orders :  %s\n", format(nrow(df_jobs),      big.mark = ",")))
cat(sprintf("Crop rows  :  %s\n", format(nrow(df_crops),     big.mark = ",")))
cat(sprintf("Location rows: %s\n",format(nrow(df_locations), big.mark = ",")))

cat("\n--- Wage coverage ---\n")
cat(sprintf("  Jobs with hourly wage:  %d (%.1f%%)\n",
            sum(!is.na(df_jobs$jobWageHourly)),
            mean(!is.na(df_jobs$jobWageHourly)) * 100))
cat(sprintf("  Jobs with piece rate:   %d (%.1f%%)\n",
            sum(!is.na(df_jobs$jobPieceRate) & df_jobs$jobPieceRate > 0),
            mean(!is.na(df_jobs$jobPieceRate) & df_jobs$jobPieceRate > 0, na.rm = TRUE) * 100))

cat("\n--- Employer coverage ---\n")
cat(sprintf("  Unique employer FEINs:  %d\n", n_distinct(df_jobs$empFein, na.rm = TRUE)))
cat(sprintf("  Jobs with FEIN:         %d (%.1f%%)\n",
            sum(!is.na(df_jobs$empFein)),
            mean(!is.na(df_jobs$empFein)) * 100))

cat("\n--- Crops ---\n")
cat(sprintf("  Job orders with crops listed: %d\n", n_distinct(df_crops$caseNumber)))
cat(sprintf("  Unique crop strings:          %d\n", n_distinct(df_crops$cropActivity)))
cat("\n  Top 20 crops by frequency:\n")
df_crops %>% count(cropActivity, sort = TRUE) %>% head(20) %>% print()

cat("\n--- Locations ---\n")
cat(sprintf("  Unique states: %s\n",
            paste(sort(unique(na.omit(df_locations$locState))), collapse = ", ")))

cat("\n--- SOC codes ---\n")
df_jobs %>% count(socTitle, sort = TRUE) %>% head(10) %>% print()

# ------------------------------------------------------------------------------
# 4. Save
# ------------------------------------------------------------------------------
saveRDS(df_jobs,      paste0(out_dir, "/df_jobs.rds"))
saveRDS(df_crops,     paste0(out_dir, "/df_crops.rds"))
saveRDS(df_locations, paste0(out_dir, "/df_locations.rds"))

cat(sprintf("\nSaved to %s\n", out_dir))
cat(sprintf("Total time: %.1f min\n", as.numeric(Sys.time() - t0, units = "mins")))
