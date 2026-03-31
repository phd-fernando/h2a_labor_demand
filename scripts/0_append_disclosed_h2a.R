###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2025
# Food and Resource Economics Department,
# University of Florida.

# clear all
cat("\014"); rm(list=ls());

# set seed to my phone number
set.seed(2028349824)

# smallest number in this version of R
e =.Machine$double.xmin

# maximum size of datasets in this script
N=Inf

# set directory
mydir = "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"

setwd(mydir)

# load functions and packages for this script
source(paste0(mydir,"/scripts/myfuns_h2ajson.R"))

# load harmonize functions (defines harmonize_h2a() but does not run it)
source(paste0(mydir,"/scripts/harmonize_h2a.R"))

library(readxl)

# xlsx version of load_file: same logic but reads .xlsx by column position.
# Dates in xlsx are Excel numeric serials; converted without mm().
xlsx_to_date <- function(x) {
  n      <- suppressWarnings(as.numeric(x))
  result <- base::as.Date(floor(ifelse(is.na(n), 0, n)), origin = "1899-12-30")
  # for cells that aren't Excel serials, try parsing as text date
  txt <- which(is.na(n))
  if (length(txt) > 0) {
    x_clean <- sub("\\s+\\d{1,2}:\\d{2}.*$", "", x[txt])  # strip time component
    d_text  <- tryCatch(
      base::as.Date(x_clean, tryFormats = c("%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y", "%Y-%m-%d")),
      error = function(e) rep(base::as.Date(NA), length(x_clean))
    )
    result[txt] <- d_text
  }
  result
}

load_file_xlsx <- function(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,x,v14=NA,v15=NA) {
  cat("  Reading:", basename(x), "... ")
  df_raw <- tryCatch(
    read_excel(x, col_types = "text"),
    error = function(e) { cat("ERROR:", conditionMessage(e), "\n"); return(NULL) }
  )
  if (is.null(df_raw)) return(NULL)
  # Pre-compute cert date fallbacks (used inside mutate via env scoping)
  cert_start_fallback <- if (!is.na(v14)) {
    cs <- as.numeric(xlsx_to_date(as.character(df_raw[[v14]])))
    ifelse(cs >= 10000 & cs <= 50000, cs, NA_real_)
  } else rep(NA_real_, nrow(df_raw))
  cert_end_fallback <- if (!is.na(v15)) {
    ce <- as.numeric(xlsx_to_date(as.character(df_raw[[v15]])))
    ifelse(ce >= 10000 & ce <= 50000, ce, NA_real_)
  } else rep(NA_real_, nrow(df_raw))
  cols <- c(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13)
  r    <- as.data.frame(df_raw[, cols])
  names(r) <- c("caseNumber","caseStatus","dateSubmitted","jobWrksRequested",
                "jobCounty","jobState","jobPostcode","startDate","endDate",
                "hoursWeek","codeNaics","codeSoc","jobWrksCertified")
  r <- r %>%
    #filter(str_detect(tolower(caseStatus), "certif")) %>%
    mutate(
      dateSubmitted   = format(xlsx_to_date(dateSubmitted), "%Y-%m"),
      jobWrksRequested = if (v4 == v13) NA_real_ else as.numeric(jobWrksRequested),
      jobWrksCertified = as.numeric(jobWrksCertified),
      codeNaics     = ifelse(codeNaics == caseNumber, NA, as.character(codeNaics)),
      codeSoc       = ifelse(codeSoc   == caseNumber, NA, as.character(codeSoc)),
      caseNumber    = gsub("H", "JO-A", caseNumber),
      jobCounty,
      jobState,
      jobPostcode   = as.character(jobPostcode),
      startDate     = as.numeric(xlsx_to_date(startDate)),
      endDate       = as.numeric(xlsx_to_date(endDate)),
      startDate     = ifelse(startDate >= 10000 & startDate <= 50000, startDate, NA_real_),
      endDate       = ifelse(endDate   >= 10000 & endDate   <= 50000, endDate,   NA_real_),
      startDate     = ifelse(is.na(startDate), cert_start_fallback, startDate),
      endDate       = ifelse(is.na(endDate),   cert_end_fallback,   endDate),
      length        = endDate - startDate,
      hoursWeek     = if (v10 == v13) NA_real_ else
                        if (v4 != v13) ifelse(jobWrksRequested == as.numeric(hoursWeek), 40, as.numeric(hoursWeek))
                        else as.numeric(hoursWeek),
      fte           = (length / 7 * hoursWeek) / (52 * 40) * jobWrksRequested,
      fte_crop      = ifelse(substr(codeNaics,1,3)=="111" | substr(codeNaics,1,4)=="1151", fte, 0),
      fte_crop      = ifelse(is.na(codeNaics), fte * .9, fte_crop),
      .keep = "none"
    ) %>%
    #filter(!is.na(dateSubmitted)) %>%
    #unique() %>%
    as.data.frame() #%>%
    #filter(hoursWeek > 0 & hoursWeek < 70 &
    #       length > 0 & length < (52 * 7))
  cat(format(nrow(r), big.mark=","), "rows (certified)\n")
  r
}

# data directory
ddir <- paste0(mydir, "/raw/data/disclosure/")

# ##############################################################################

# (code starts here)


################################################################################
# METHOD 1: load_file_xlsx (column-position selection from xlsx)
################################################################################

cat("=== METHOD 1: load_file_xlsx (column-position selection) ===\n")
df.jobs = bind_rows(
  load_file_xlsx(1,2,3,77,126,124,125,79,80,84,24,56,78, paste0(ddir,"H-2A_Disclosure_Data_FY2024.xlsx"), v14=81,v15=82),
  load_file_xlsx(1,2,3,69,115,113,114,71,72,76,20,52,70, paste0(ddir,"H-2A_Disclosure_Data_FY2023.xlsx"), v14=73,v15=74),
  load_file_xlsx(1,2,3,69,115,113,114,71,72,76,20,52,70, paste0(ddir,"H-2A_Disclosure_Data_FY2022.xlsx"), v14=73,v15=74),
  load_file_xlsx(1,2,3,69,115,113,114,71,72,76,20,52,70, paste0(ddir,"H-2A_Disclosure_Data_FY2021.xlsx"), v14=73,v15=74),
  load_file_xlsx(1,2,3,69,115,113,114,71,72,76,20,52,70, paste0(ddir,"H-2A_Disclosure_Data_FY2020.xlsx"), v14=73,v15=74),
  load_file_xlsx(1,5,4,30,54,55,56,6,7,34,29,26,31,     paste0(ddir,"H-2A_Disclosure_Data_FY2019.xlsx")),
  load_file_xlsx(1,5,4,30,54,55,56,6,7,34,29,26,31,     paste0(ddir,"H-2A_Disclosure_Data_FY2018.xlsx")),
  load_file_xlsx(1,5,4,30,54,55,56,6,7,34,29,26,31,     paste0(ddir,"H-2A_Disclosure_Data_FY2017.xlsx")),
  load_file_xlsx(1,5,4,28,51,52,53,6,7,32,27,24,29,     paste0(ddir,"H-2A_Disclosure_Data_FY2016.xlsx")),
  load_file_xlsx(1,5,4,27,50,51,52,6,7,31,26,23,28,     paste0(ddir,"H-2A_Disclosure_Data_FY2015.xlsx")),
  load_file_xlsx(1,5,4,23,27,28,15,8,9,24,1,21,23,      paste0(ddir,"H-2A_Disclosure_Data_FY2014.xlsx")),
  load_file_xlsx(1,5,4,20,13,14,15,8,9,21,1,1,20,       paste0(ddir,"H-2A_Disclosure_Data_H2A_FY2013.xlsx")),
  load_file_xlsx(1,5,4,20,13,14,15,8,9,22,1,1,21,       paste0(ddir,"H-2A_Disclosure_Data_FY2012.xlsx")),
  load_file_xlsx(1,5,4,20,13,14,15,8,9,22,1,1,21,       paste0(ddir,"H-2A_Disclosure_Data_FY2011.xlsx")),
  load_file_xlsx(1,4,3,22,12,13,14,7,8,22,1,1,22,       paste0(ddir,"H-2A_FY2010.xlsx")),
  load_file_xlsx(1,4,3,22,12,13,14,7,8,22,1,1,22,       paste0(ddir,"H2A_FY2009.xlsx")),
  load_file_xlsx(1,5,4,18,11,12,13,6,7,18,1,1,18,       paste0(ddir,"H2A_FY2008.xlsx")),
  load_file_xlsx(1,4,3,15,10,11,12,5,6,17,1,1,16,       paste0(ddir,"H-2A_FY2007.xlsx")),
  load_file_xlsx(1,2,3,12,8,9,10,3,4,14,1,1,13,         paste0(ddir,"H2a_ FY2006.xlsx"))
)

df_legacy <- df.jobs
cat(sprintf("Method 1 done: %s rows, %d columns\n", format(nrow(df_legacy), big.mark=","), ncol(df_legacy)))
cat("Columns:", paste(names(df_legacy), collapse=", "), "\n\n")


################################################################################
# METHOD 2: harmonize_h2a (name-based, all columns)
################################################################################

cat("=== METHOD 2: harmonize_h2a (rebuilding h2a_combined.rds from xlsx) ===\n")
df_harmonized <- harmonize_h2a(ddir)
cat(sprintf("Method 2 done: %s rows, %d columns\n\n", format(nrow(df_harmonized), big.mark=","), ncol(df_harmonized)))


################################################################################
# METHOD 3: Charlton's CSV harmonization
################################################################################

cat("=== METHOD 3: Charlton's h2a_combined.csv ===\n")
df_charlton <- read.csv(paste0(ddir, "h2a_combined.csv"), stringsAsFactors = FALSE)
cat(sprintf("Method 3 done: %s rows, %d columns\n", format(nrow(df_charlton), big.mark=","), ncol(df_charlton)))
cat("Columns:", paste(names(df_charlton), collapse=", "), "\n\n")


################################################################################
# COMPARISON: all three methods
################################################################################

cat("=== COMPARISON: legacy vs harmonized (RDS) vs Charlton (CSV) ===\n\n")

# -- Row counts by year --------------------------------------------------------
cat("--- Row counts by year ---\n")
legacy_yr     <- df_legacy     %>% mutate(year = as.integer(substr(dateSubmitted, 1, 4))) %>% count(year, name = "n_legacy")
harmonized_yr <- df_harmonized %>% count(year = fiscal_year, name = "n_harmonized")
charlton_yr   <- df_charlton   %>% count(year = file_year,   name = "n_charlton")

yr_tab <- Reduce(function(a,b) merge(a, b, by="year", all=TRUE),
                 list(legacy_yr, harmonized_yr, charlton_yr))
print(yr_tab, row.names = FALSE)

# -- Non-NA counts for key columns ---------------------------------------------
cat("\n--- Non-NA counts (key variables) ---\n")
col_map <- list(
  workers_requested = list(leg="jobWrksRequested",  harm="nbr_workers_requested", char="nbr_workers_requested"),
  workers_certified = list(leg="jobWrksCertified",  harm="nbr_workers_certified", char="nbr_workers_certified"),
  state             = list(leg="jobState",           harm="worksite_state",        char="worksite_state"),
  postcode          = list(leg="jobPostcode",        harm="worksite_postal_code",  char="worksite_postal_code"),
  naics             = list(leg="codeNaics",          harm="naics_code",            char="naic_code"),
  soc               = list(leg="codeSoc",            harm="soc_code",              char="SOC_code"),
  hours_week        = list(leg="hoursWeek",          harm="basic_number_of_hours", char="basic_number_of_hours")
)
cat(sprintf("  %-20s  %12s  %12s  %12s\n", "variable", "legacy", "harmonized", "charlton"))
for (v in names(col_map)) {
  nm <- col_map[[v]]
  n_leg  <- if (nm$leg  %in% names(df_legacy))     sum(!is.na(df_legacy[[nm$leg]]))    else NA
  n_harm <- if (nm$harm %in% names(df_harmonized)) sum(!is.na(df_harmonized[[nm$harm]])) else NA
  n_char <- if (nm$char %in% names(df_charlton))   sum(!is.na(df_charlton[[nm$char]]) & df_charlton[[nm$char]] != "") else NA
  cat(sprintf("  %-20s  %12s  %12s  %12s\n", v,
              format(n_leg,  big.mark=","),
              format(n_harm, big.mark=","),
              format(n_char, big.mark=",")))
}

# -- Workers requested ---------------------------------------------------------
cat("\n--- Workers requested ---\n")
cat("Legacy:\n");     print(summary(df_legacy$jobWrksRequested))
cat("Harmonized:\n"); print(summary(df_harmonized$nbr_workers_requested))
cat("Charlton:\n");   print(summary(as.numeric(df_charlton$nbr_workers_requested)))

# -- Workers certified ---------------------------------------------------------
cat("\n--- Workers certified ---\n")
cat("Legacy:\n");     print(summary(df_legacy$jobWrksCertified))
cat("Harmonized:\n"); print(summary(df_harmonized$nbr_workers_certified))
cat("Charlton:\n");   print(summary(as.numeric(df_charlton$nbr_workers_certified)))

# -- Start date ----------------------------------------------------------------
harm_start <- as.numeric(df_harmonized$requested_start_date)  # stored as Date → numeric
char_start <- as.numeric(as.Date(df_charlton$requsted_start_date))
cat("\n--- Start date (days since 1970-01-01) ---\n")
cat("Legacy:\n");     print(summary(df_legacy$startDate))
cat("Harmonized:\n"); print(summary(harm_start))
cat("Charlton:\n");   print(summary(char_start))

# -- Duration ------------------------------------------------------------------
harm_dur <- as.numeric(df_harmonized$requested_end_date) - harm_start
char_dur <- as.numeric(as.Date(df_charlton$requsted_end_date)) - char_start
cat("\n--- Duration in days ---\n")
cat("Legacy:\n");     print(summary(df_legacy$length))
cat("Harmonized:\n"); print(summary(harm_dur))
cat("Charlton:\n");   print(summary(char_dur))

# -- Hours per week ------------------------------------------------------------
cat("\n--- Hours per week ---\n")
cat("Legacy:\n");     print(summary(df_legacy$hoursWeek))
cat("Harmonized:\n"); print(summary(as.numeric(df_harmonized$basic_number_of_hours)))
cat("Charlton:\n");   print(summary(as.numeric(df_charlton$basic_number_of_hours)))


stop("=== End of comparison section ===")



################################################################################
# SAVE
################################################################################

# unit of observation is job order ID
df_legacy = df_legacy %>%
            arrange(caseNumber) %>%
            mutate(source = "DOL's disclosure")

write.csv(row.names=F, df_legacy,     paste0(mydir,"/processed/data/df.jobs_legacy.csv"));     gc()
write.csv(row.names=F, df_harmonized, paste0(mydir,"/processed/data/df.jobs_harmonized.csv")); gc()
cat("Saved both outputs to processed/data/\n")
