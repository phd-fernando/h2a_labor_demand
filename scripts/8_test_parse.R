###############################################################################
# Quick validation: test parse_gap_pdf() on 5 sample PDFs
###############################################################################

cat("\014"); rm(list = ls())

mydir   <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
pdf_dir <- paste0(mydir, "/raw/data/pdf_gap")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "pdftools")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a[1])) a[1] else b

get_text <- function(page_df, x_min, x_max, y_min, y_max) {
  page_df %>% as.data.frame() %>%
    filter(x >= x_min*72, x <= x_max*72, y >= y_min*72, y < y_max*72) %>%
    arrange(y, x) %>% pull(text) %>% paste(collapse=" ") %>% str_squish()
}

parse_gap_pdf <- function(filepath) {
  id    <- tools::file_path_sans_ext(basename(filepath))
  pages <- tryCatch(pdftools::pdf_data(filepath), error = function(e) NULL)
  if (is.null(pages) || length(pages) == 0) return(NULL)

  fields <- list(caseNumber = id)

  if (length(pages) >= 1) {
    p1_df <- as.data.frame(pages[[1]])

    # dateSubmitted from case number (YYddd)
    part <- str_extract(id, "(?<=JO-A-300-)[0-9]{5}(?=-)")
    if (!is.na(part)) {
      yy <- as.integer(str_sub(part,1,2)); ddd <- as.integer(str_sub(part,3,5))
      fields$dateSubmitted <- as.Date(paste0("20",sprintf("%02d",yy),"-01-01")) + ddd - 1
    } else fields$dateSubmitted <- NA

    # FEIN
    fields$empFein <- p1_df %>%
      filter(str_detect(text,"^[0-9]{2}-[0-9]{7}$")) %>% pull(text) %>% first() %||% NA

    # Employer name: try y=50-90pt; reject if it's the DOL form header
    emp_raw <- get_text(pages[[1]], 0.75, 7.5, 0.69, 1.25)
    header_phrases <- c("OMB Approval","Expiration Date","H-2A","ETA-790A",
                        "U.S. Department","Departmen","Labor","A. Job")
    is_header <- any(str_detect(emp_raw, paste(header_phrases, collapse="|")))
    fields$empBusinessName <- if (!is_header && nchar(emp_raw) > 2) emp_raw else NA

    # SOC code
    fields$socCode <- p1_df %>%
      filter(str_detect(text,"^[0-9]{2}-[0-9]{4}\\.[0-9]{2}$")) %>% pull(text) %>% first() %||% NA

    # Workers needed (H-2A column at x≈120-165, y≈138-150)
    wrks_h2a <- p1_df %>%
      filter(str_detect(text,"^[0-9]{1,5}$"), x>=115, x<=165, y>=138, y<=150) %>%
      pull(text) %>% as.integer() %>% .[.>0&.<10000] %>% first()
    wrks_tot <- p1_df %>%
      filter(str_detect(text,"^[0-9]{1,5}$"), x>=165, x<=215, y>=138, y<=150) %>%
      pull(text) %>% as.integer() %>% .[.>0&.<10000] %>% first()
    fields$jobWrksNeeded <- (wrks_h2a %||% wrks_tot) %||% NA

    # Dates: strictly y=138-152 to avoid OMB expiration date
    parse_flex_date <- function(d) suppressWarnings(
      as.Date(d, tryFormats=c("%m/%d/%Y","%d-%b-%Y","%Y-%m-%d","%m-%d-%Y")))
    dt_row <- p1_df %>%
      filter(str_detect(text,"[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4}"),
             y>=138, y<=152) %>% arrange(x) %>% pull(text)
    dp <- map(dt_row, parse_flex_date) %>% keep(~!is.na(.x))
    fields$jobBeginDate <- if (length(dp)>=1) dp[[1]] else NA
    fields$jobEndDate   <- if (length(dp)>=2) dp[[2]] else NA

    # Wage (8b): find HOUR/WEEK/MONTH/YEAR row, then grab integer tokens left of center
    pay_row <- p1_df %>%
      filter(str_detect(text, regex("^(HOUR|WEEK|MONTH|YEAR|Hour|Week|Month|Year)$")),
             y>=500, y<=535) %>% arrange(y)
    if (nrow(pay_row) > 0) {
      fields$jobWagePer <- pay_row$text[1]
      pay_y <- pay_row$y[1]
      wage_tok <- p1_df %>%
        filter(abs(y-pay_y)<=8, str_detect(text,"^[0-9]+$"), x>=55, x<=160) %>%
        arrange(x) %>% pull(text)
      if (length(wage_tok)>=2) {
        fields$jobWageOffer <- suppressWarnings(as.numeric(paste(wage_tok[1],wage_tok[2],sep=".")))
      } else if (length(wage_tok)==1) {
        fields$jobWageOffer <- suppressWarnings(as.numeric(wage_tok[1]))
      } else fields$jobWageOffer <- NA
    } else { fields$jobWagePer <- NA; fields$jobWageOffer <- NA }

    # Job title
    fields$jobTitle <- get_text(pages[[1]], 1.2, 7.5, 1.45, 1.62)
  }

  if (length(pages) >= 2) {
    p2_df <- as.data.frame(pages[[2]])
    state_lookup <- setNames(state.abb, toupper(state.name))
    state_lookup <- c(state_lookup, "DISTRICT OF COLUMBIA"="DC","PUERTO RICO"="PR","GUAM"="GU")

    # City/State/ZIP/County row at y≈382-396
    loc_row <- p2_df %>% filter(y>=382, y<=396) %>% arrange(x)
    if (nrow(loc_row) > 0) {
      zip_tok <- loc_row %>% filter(str_detect(text,"^[0-9]{5}")) %>%
        pull(text) %>% str_extract("^[0-9]{5}") %>% first()
      fields$jobPostcode <- zip_tok %||% NA

      # State: match tokens x=150-345 (between city and zip), handles multi-word states
      state_candidate <- loc_row %>%
        filter(x>=150, x<=345, !str_detect(text,"^[0-9]")) %>%
        arrange(x) %>% pull(text) %>% paste(collapse=" ") %>% toupper()
      state_match_val <- state_lookup[state_candidate]
      if (is.na(state_match_val)) {
        st_single <- loc_row %>% filter(x>=150, x<=345) %>% pull(text) %>%
          .[toupper(.) %in% names(state_lookup)] %>% first()
        state_match_val <- if (!is.null(st_single)&&!is.na(st_single))
          state_lookup[toupper(st_single)] else NA
      }
      fields$jobState <- unname(state_match_val) %||% NA

      city_str <- loc_row %>% filter(x<200) %>% arrange(x) %>%
        pull(text) %>% paste(collapse=" ")
      fields$jobCity <- if(nchar(city_str)>0) city_str else NA

      county_str <- loc_row %>% filter(x>350) %>% arrange(x) %>%
        pull(text) %>% paste(collapse=" ")
      fields$jobCounty <- if(nchar(county_str)>0) county_str else NA
    } else {
      fields$jobPostcode <- p2_df %>% filter(str_detect(text,"^[0-9]{5}$")) %>%
        pull(text) %>% first() %||% NA
      fields$jobState <- NA; fields$jobCity <- NA; fields$jobCounty <- NA
    }
  }

  fields
}

# ── run on 5 samples ─────────────────────────────────────────────────────────
test_files <- sort(list.files(pdf_dir, pattern="\\.pdf$", full.names=TRUE))[c(1,2,3,100,500)]
cat("Testing on:\n"); cat(paste(" ", basename(test_files)), sep="\n"); cat("\n")

for (f in test_files) {
  cat(sprintf("\n=== %s ===\n", basename(f)))
  res <- parse_gap_pdf(f)
  for (nm in names(res))
    cat(sprintf("  %-20s: %s\n", nm, paste(as.character(res[[nm]]), collapse=" ")))
}
cat("\nDone.\n")
