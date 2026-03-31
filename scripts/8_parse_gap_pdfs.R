###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Parse locally downloaded gap-period PDFs (Nov 2023 – Feb 2024).
# Inspired by pdf2df / fun() in myfuns_h2ajson.R but reads from disk,
# extracts both text sections AND structured fields (wage, workers,
# dates, employer, location, SOC, NAICS), and processes in memory-safe
# batches.
#
# Outputs (NEVER overwrites originals):
#   processed/text/df_text_gap.rds     — text sections (same schema as df_text_jo.rds)
#   processed/text/df_jobs_gap.rds     — structured fields (same schema as df_jobs.rds)
#   processed/text/df_jobs_patched.rds — df_jobs.rds with gap filled (de-duplicated)
###############################################################################

cat("\014"); rm(list = ls())

mydir   <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
pdf_dir <- paste0(mydir, "/raw/data/pdf_gap")
out_dir <- paste0(mydir, "/processed/text")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "pdftools", "lubridate")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# HELPER FUNCTIONS (coordinate-based text extraction)
# ==============================================================================

# Extract text from a PDF page within a bounding box (in inches)
get_text <- function(page_df, x_min, x_max, y_min, y_max) {
  page_df %>%
    as.data.frame() %>%
    filter(x >= x_min * 72, x <= x_max * 72,
           y >= y_min * 72, y <  y_max * 72) %>%
    arrange(y, x) %>%
    pull(text) %>%
    paste(collapse = " ") %>%
    str_squish()
}

# Find y-coordinate of first token matching a pattern (returns inches)
find_y <- function(page_df, pattern) {
  y <- page_df %>%
    as.data.frame() %>%
    filter(str_detect(text, pattern)) %>%
    pull(y) %>%
    min()
  if (is.infinite(y)) NA_real_ else y / 72
}

# Find y-coordinate after a section header (skip the header line)
find_y_after <- function(page_df, section_pat, field_pat) {
  sec_y <- find_y(page_df, section_pat)
  if (is.na(sec_y)) return(NA_real_)
  y <- page_df %>%
    as.data.frame() %>%
    filter(str_detect(text, field_pat), y / 72 > sec_y) %>%
    pull(y) %>%
    min()
  if (is.infinite(y)) NA_real_ else y / 72
}

# Extract a numeric value following a label (same row ± 5pt)
extract_numeric_near <- function(page_df, label_pat, x_min = 0, x_max = 8.5) {
  label_rows <- page_df %>%
    as.data.frame() %>%
    filter(str_detect(text, label_pat))
  if (nrow(label_rows) == 0) return(NA_real_)
  y_target <- label_rows$y[1]
  val <- page_df %>%
    as.data.frame() %>%
    filter(abs(y - y_target) <= 5,
           x >= x_min * 72, x <= x_max * 72,
           str_detect(text, "^[0-9,\\.]+$")) %>%
    arrange(x) %>%
    pull(text) %>%
    first()
  suppressWarnings(as.numeric(str_remove_all(val, ",")))
}

# ==============================================================================
# CORE FUNCTION: parse one local PDF file
# Returns a list: $text (data.frame of sections), $fields (named list)
# ==============================================================================

parse_gap_pdf <- function(filepath) {

  id <- tools::file_path_sans_ext(basename(filepath))

  pages <- tryCatch(
    pdftools::pdf_data(filepath),
    error = function(e) NULL
  )
  if (is.null(pages) || length(pages) == 0) return(NULL)

  # ── TEXT SECTIONS (replicates fun() logic) ─────────────────────────────────

  null_text <- data.frame(
    caseNumber = character(), addmcSectionDetails = character(),
    addmcSectionNumber = character(), addmcSectionName = character(),
    stringsAsFactors = FALSE
  )

  txt_section <- function(pg, y0, y1, label) {
    if (is.na(y0) || is.na(y1)) return(null_text)
    content <- get_text(pg, x_min = 0.75, x_max = 7.99, y_min = y0, y_max = y1)
    data.frame(caseNumber = id, addmcSectionDetails = content,
               addmcSectionNumber = label, addmcSectionName = label,
               stringsAsFactors = FALSE)
  }

  text_rows <- null_text

  # Page 1: A.8a (job duties), A.11 (deductions)
  if (length(pages) >= 1) {
    p1 <- pages[[1]]
    y_8a_start <- find_y_after(p1, "^A\\.$", "^8[a]\\.$") + (9 + 12) / 72
    y_8a_end   <- find_y_after(p1, "^A\\.$", "^8[bcde]\\.$")
    y_11_start <- find_y_after(p1, "^A\\.$", "^11\\.$") + (9 + 12) / 72
    y_11_end   <- find_y(p1, "^ETA-790A$")
    text_rows  <- bind_rows(text_rows,
                            txt_section(p1, y_8a_start, y_8a_end, "A.8a"),
                            txt_section(p1, y_11_start, y_11_end, "A.11"))
  }

  # Page 2: B.6 (qualifications), C.6 (worksite), D.6 (housing)
  if (length(pages) >= 2) {
    p2 <- pages[[2]]
    y_b6_s <- find_y_after(p2, "^B\\.$", "^6\\.$") + (9 + 12) / 72
    y_b6_e <- find_y(p2, "^C\\.$")
    y_c6_s <- find_y_after(p2, "^C\\.$", "^6\\.$") + 12 / 72
    y_c6_e <- find_y_after(p2, "^C\\.$", "^7\\.$")
    y_d6_s <- find_y_after(p2, "^D\\.$", "^10\\.$") + 12 / 72
    y_d6_e <- find_y_after(p2, "^D\\.$", "^11\\.$")
    text_rows <- bind_rows(text_rows,
                           txt_section(p2, y_b6_s, y_b6_e, "B.6"),
                           txt_section(p2, y_c6_s, y_c6_e, "C.6"),
                           txt_section(p2, y_d6_s, y_d6_e, "D.6"))
  }

  # Page 3: E.1 (meals), F.1 (transport), F.2 (reimbursement)
  if (length(pages) >= 3) {
    p3 <- pages[[3]]
    y_e1_s <- find_y_after(p3, "^E\\.$", "^1\\.$") + (9 + 12 + 12) / 72
    y_e1_e <- find_y_after(p3, "^E\\.$", "^2\\.$")
    y_f1_s <- find_y_after(p3, "^F\\.$", "^1\\.$") + (9 + 12) / 72
    y_f1_e <- find_y_after(p3, "^F\\.$", "^2\\.$")
    y_f2_s <- find_y_after(p3, "^F\\.$", "^2\\.$") + (9 + 12 + 12) / 72
    y_f2_e <- find_y_after(p3, "^F\\.$", "^3\\.$")
    text_rows <- bind_rows(text_rows,
                           txt_section(p3, y_e1_s, y_e1_e, "E.1"),
                           txt_section(p3, y_f1_s, y_f1_e, "F.1"),
                           txt_section(p3, y_f2_s, y_f2_e, "F.2"))
  }

  # Page 4: G.1 (recruitment)
  if (length(pages) >= 4) {
    p4 <- pages[[4]]
    y_g1_s <- find_y_after(p4, "^G\\.$", "^1\\.$") + (9 + 12 + 12 + 12) / 72
    y_g1_e <- find_y_after(p4, "^G\\.$", "^2\\.$")
    text_rows <- bind_rows(text_rows, txt_section(p4, y_g1_s, y_g1_e, "G.1"))
  }

  text_rows <- text_rows %>%
    filter(nchar(str_squish(addmcSectionDetails)) > 0)

  # ── STRUCTURED FIELDS (page 1 of ETA-790A) ─────────────────────────────────

  fields <- list(caseNumber = id)

  if (length(pages) >= 1) {
    p1 <- pages[[1]]
    p1_df <- as.data.frame(p1)

    # --- decode submission date from case number ---
    # Format: JO-A-300-YYddd-NNNNNN  (YY = 2-digit year, ddd = day of year)
    part <- str_extract(id, "(?<=JO-A-300-)[0-9]{5}(?=-)")
    if (!is.na(part)) {
      yy  <- as.integer(str_sub(part, 1, 2))
      ddd <- as.integer(str_sub(part, 3, 5))
      fields$dateSubmitted <- as.Date(paste0("20", sprintf("%02d", yy), "-01-01")) + ddd - 1
    } else {
      fields$dateSubmitted <- NA_Date_
    }

    # --- employer FEIN: pattern XX-XXXXXXX ---
    fein_match <- p1_df %>%
      filter(str_detect(text, "^[0-9]{2}-[0-9]{7}$")) %>%
      pull(text) %>% first()
    fields$empFein <- fein_match %||% NA_character_

    # --- employer name: look for employer name token block above section A ---
    # Section A label "A." is typically at y≈95pt (1.32in). Look for non-form text
    # above that. The DOL portal PDFs often don't include employer name on page 1;
    # try y=50-90pt (0.69-1.25in), exclude known header phrases
    emp_raw <- get_text(p1, x_min = 0.75, x_max = 7.5, y_min = 0.69, y_max = 1.25)
    header_phrases <- c("OMB Approval", "Expiration Date", "H-2A", "ETA-790A",
                        "U.S. Department", "Departmen", "Labor", "A. Job")
    is_header <- any(str_detect(emp_raw, paste(header_phrases, collapse = "|")))
    fields$empBusinessName <- if (!is_header && nchar(emp_raw) > 2) emp_raw else NA_character_

    # --- SOC code: pattern XX-XXXX.XX ---
    soc_match <- p1_df %>%
      filter(str_detect(text, "^[0-9]{2}-[0-9]{4}\\.[0-9]{2}$")) %>%
      pull(text) %>% first()
    fields$socCode <- soc_match %||% NA_character_

    # --- H-2A workers needed (field 2b): at y≈139-150pt, x≈120-200pt ---
    # The form has two columns: 2a (total) at x≈192 and 2b (H-2A) at x≈129
    # We want H-2A workers (x≈120-165), fallback to total (x≈165-210)
    wrks_h2a <- p1_df %>%
      filter(str_detect(text, "^[0-9]{1,5}$"),
             x >= 115, x <= 165,
             y >= 138, y <= 150) %>%
      pull(text) %>% as.integer() %>%
      .[!is.na(.) & . > 0 & . <= 10000] %>% first()
    wrks_tot <- p1_df %>%
      filter(str_detect(text, "^[0-9]{1,5}$"),
             x >= 165, x <= 215,
             y >= 138, y <= 150) %>%
      pull(text) %>% as.integer() %>%
      .[!is.na(.) & . > 0 & . <= 10000] %>% first()
    fields$jobWrksNeeded <- (wrks_h2a %||% wrks_tot) %||% NA_integer_

    # --- begin/end dates: at y≈139-150pt, date pattern tokens ---
    # Filter strictly to the date row (y 138-152) to avoid OMB expiration date
    parse_flex_date <- function(d) suppressWarnings(
      as.Date(d, tryFormats = c("%m/%d/%Y", "%d-%b-%Y", "%Y-%m-%d", "%m-%d-%Y")))
    date_tokens_row <- p1_df %>%
      filter(str_detect(text,
        "[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4}"),
        y >= 138, y <= 152) %>%
      arrange(x) %>% pull(text)
    dates_parsed <- map(date_tokens_row, parse_flex_date) %>% keep(~!is.na(.x))
    fields$jobBeginDate <- if (length(dates_parsed) >= 1) dates_parsed[[1]] else NA_Date_
    fields$jobEndDate   <- if (length(dates_parsed) >= 2) dates_parsed[[2]] else NA_Date_

    # --- wage offer (8b): at y≈510-525pt, x < 150pt ---
    # The wage amount is printed as two tokens: integer part + cents part
    # e.g., "13" at x=69 and "67" at x=111 on the same y row (~517pt)
    # Find the y of the "HOUR/WEEK/MONTH/YEAR" checkbox row
    pay_row <- p1_df %>%
      filter(str_detect(text, regex("^(HOUR|WEEK|MONTH|YEAR|Hour|Week|Month|Year)$")),
             y >= 500, y <= 535) %>%
      arrange(y) %>% first()

    if (!is.null(pay_row) && nrow(pay_row) > 0) {
      fields$jobWagePer <- pay_row$text[1]
      pay_y <- pay_row$y[1]
      # grab numeric tokens on same row (±8pt), left half of page
      wage_tokens <- p1_df %>%
        filter(abs(y - pay_y) <= 8,
               str_detect(text, "^[0-9]+$"),
               x >= 55, x <= 160) %>%
        arrange(x) %>% pull(text)
      if (length(wage_tokens) >= 2) {
        fields$jobWageOffer <- suppressWarnings(
          as.numeric(paste(wage_tokens[1], wage_tokens[2], sep = ".")))
      } else if (length(wage_tokens) == 1) {
        fields$jobWageOffer <- suppressWarnings(as.numeric(wage_tokens[1]))
      } else {
        fields$jobWageOffer <- NA_real_
      }
    } else {
      fields$jobWagePer   <- NA_character_
      fields$jobWageOffer <- NA_real_
    }
  }

  # page 2: city, state (full name), zip, county from section C row
  # Values appear at y≈388pt in the order: city(x≈57), state_name(x≈264), zip(x≈318), county(x≈399)
  if (length(pages) >= 2) {
    p2 <- pages[[2]]
    p2_df <- as.data.frame(p2)

    # build state name → abbreviation lookup
    state_lookup <- setNames(state.abb, toupper(state.name))
    state_lookup <- c(state_lookup,
                      "DISTRICT OF COLUMBIA" = "DC",
                      "PUERTO RICO" = "PR",
                      "GUAM" = "GU")

    # The city/state/zip row is the row just below the "City | State | Postal Code | County" label row
    # Label row is at y≈376; values are at y≈386-392
    loc_row <- p2_df %>%
      filter(y >= 382, y <= 396) %>%
      arrange(x)

    if (nrow(loc_row) > 0) {
      # zip: 5-digit token
      zip_tok <- loc_row %>%
        filter(str_detect(text, "^[0-9]{5}(-[0-9]{4})?$")) %>%
        pull(text) %>% str_extract("^[0-9]{5}") %>% first()
      fields$jobPostcode <- zip_tok %||% NA_character_

      # state: match tokens between city (x<200) and zip (x>305) against state names
      # Handles both single-word ("Georgia") and two-word ("North Dakota") states
      state_candidate <- loc_row %>%
        filter(x >= 150, x <= 345,
               !str_detect(text, "^[0-9]")) %>%  # exclude numeric (zip)
        arrange(x) %>% pull(text) %>% paste(collapse = " ") %>% toupper()
      # try exact match first, then first word
      state_match_val <- state_lookup[state_candidate]
      if (is.na(state_match_val)) {
        # try matching each individual token
        state_tok_single <- loc_row %>% filter(x>=150, x<=345) %>% pull(text) %>%
          .[toupper(.) %in% names(state_lookup)] %>% first()
        state_match_val <- if (!is.null(state_tok_single) && !is.na(state_tok_single))
          state_lookup[toupper(state_tok_single)] else NA_character_
      }
      fields$jobState <- unname(state_match_val) %||% NA_character_

      # city: leftmost token(s) before state column (x < 200)
      city_tok <- loc_row %>%
        filter(x < 200) %>%
        arrange(x) %>% pull(text) %>% paste(collapse = " ")
      fields$jobCity <- if (nchar(city_tok) > 0) city_tok else NA_character_

      # county: rightmost token(s) (x > 350)
      county_tok <- loc_row %>%
        filter(x > 350) %>%
        arrange(x) %>% pull(text) %>% paste(collapse = " ")
      fields$jobCounty <- if (nchar(county_tok) > 0) county_tok else NA_character_
    } else {
      # fallback: zip code anywhere on page 2
      zip_match <- p2_df %>%
        filter(str_detect(text, "^[0-9]{5}$")) %>%
        pull(text) %>% first()
      fields$jobPostcode <- zip_match %||% NA_character_
      fields$jobState    <- NA_character_
      fields$jobCity     <- NA_character_
      fields$jobCounty   <- NA_character_
    }
  }

  list(text = text_rows, fields = fields)
}

# ==============================================================================
# BATCH PROCESSING — memory-safe, saves every 200 files
# ==============================================================================

pdf_files  <- sort(list.files(pdf_dir, pattern = "\\.pdf$", full.names = TRUE))
batch_dir  <- paste0(out_dir, "/gap_batches")
dir.create(batch_dir, showWarnings = FALSE)

n_files    <- length(pdf_files)
batch_size <- 200
n_batches  <- ceiling(n_files / batch_size)

cat(sprintf("Parsing %d PDFs in %d batches of %d...\n", n_files, n_batches, batch_size))
t0 <- Sys.time()

for (b in seq_len(n_batches)) {

  batch_rds <- paste0(batch_dir, sprintf("/batch_%03d.rds", b))
  if (file.exists(batch_rds)) {
    cat(sprintf("  Batch %d/%d already done — skipping\n", b, n_batches))
    next
  }

  idx   <- ((b - 1) * batch_size + 1):min(b * batch_size, n_files)
  files <- pdf_files[idx]

  batch_text   <- list()
  batch_fields <- list()

  for (i in seq_along(files)) {
    result <- tryCatch(parse_gap_pdf(files[i]), error = function(e) NULL)
    if (!is.null(result)) {
      batch_text[[length(batch_text) + 1]]     <- result$text
      batch_fields[[length(batch_fields) + 1]] <- result$fields
    }
    if (i %% 50 == 0)
      cat(sprintf("    batch %d/%d  file %d/%d  (%.1f min)\n",
                  b, n_batches, i, length(files),
                  as.numeric(Sys.time() - t0, units = "mins")))
  }

  saveRDS(list(text = bind_rows(batch_text),
               fields = batch_fields),
          batch_rds)

  rm(batch_text, batch_fields); gc()
  cat(sprintf("  Batch %d/%d saved  (%.1f min total)\n",
              b, n_batches, as.numeric(Sys.time() - t0, units = "mins")))
}

# ==============================================================================
# COMBINE ALL BATCHES
# ==============================================================================
cat("\nCombining batches...\n")

all_text   <- list()
all_fields <- list()

for (b in seq_len(n_batches)) {
  batch <- readRDS(paste0(batch_dir, sprintf("/batch_%03d.rds", b)))
  all_text[[b]]   <- batch$text
  all_fields[[b]] <- batch$fields
  rm(batch); gc()
}

df_text_gap <- bind_rows(all_text)
df_fields   <- bind_rows(lapply(unlist(all_fields, recursive = FALSE),
                                function(f) as_tibble(f)))

cat(sprintf("Text rows  : %s\n", format(nrow(df_text_gap), big.mark = ",")))
cat(sprintf("Field rows : %s\n", format(nrow(df_fields),   big.mark = ",")))

# ==============================================================================
# BUILD df_jobs_gap — structured fields with wage normalization
# ==============================================================================
df_jobs_gap <- df_fields %>%
  mutate(
    caseNumber      = as.character(caseNumber),
    dateSubmitted   = as.Date(dateSubmitted),
    jobBeginDate    = as.Date(jobBeginDate),
    jobEndDate      = as.Date(jobEndDate),
    jobWageOffer    = suppressWarnings(as.numeric(jobWageOffer)),
    jobWrksNeeded   = suppressWarnings(as.integer(jobWrksNeeded)),
    jobWageHourly   = case_when(
      jobWagePer == "Hour"  ~ jobWageOffer,
      jobWagePer == "Week"  ~ jobWageOffer / 40,
      jobWagePer == "Month" ~ jobWageOffer / (52 / 12 * 40),
      jobWagePer == "Year"  ~ jobWageOffer / (52 * 40),
      TRUE                  ~ NA_real_
    ),
    # jobWagePer: normalize to title case to match JSON schema
    jobWagePer    = str_to_title(jobWagePer),
    # fields not available from PDFs — set NA to match schema
    empTradeName  = NA_character_,
    socTitle      = NA_character_,
    empNaics      = NA_integer_,
    jobPieceRate  = NA_real_,
    jobDuties     = NA_character_,
    jobHoursTotal = NA_real_,
    # jobCity, jobCounty, jobState, jobPostcode extracted from page 2
    jobCity       = as.character(jobCity),
    jobCounty     = as.character(jobCounty),
    jobState      = as.character(jobState),
    jobPostcode   = as.character(jobPostcode),
    source        = "pdf_gap"
  )

saveRDS(df_text_gap, paste0(out_dir, "/df_text_gap.rds"))
saveRDS(df_jobs_gap, paste0(out_dir, "/df_jobs_gap.rds"))
cat("Saved: df_text_gap.rds and df_jobs_gap.rds\n")

# ==============================================================================
# PATCH df_jobs — fill the gap, deduplicate overlaps
# ==============================================================================
cat("\nPatching df_jobs.rds...\n")
df_jobs <- readRDS(paste0(out_dir, "/df_jobs.rds")) %>%
  mutate(source = "json")

# align columns
common_cols <- intersect(names(df_jobs), names(df_jobs_gap))
df_jobs_gap_aligned <- df_jobs_gap %>% select(all_of(common_cols))
df_jobs_aligned     <- df_jobs     %>% select(all_of(common_cols))

# stack, deduplicate: prefer JSON over PDF when both exist for same caseNumber
df_patched <- bind_rows(df_jobs_aligned, df_jobs_gap_aligned) %>%
  arrange(caseNumber, desc(source == "json")) %>%   # JSON rows first
  distinct(caseNumber, .keep_all = TRUE)            # keep first (JSON preferred)

cat(sprintf("\n=== Patching Summary ===\n"))
cat(sprintf("Original df_jobs rows  : %s\n", format(nrow(df_jobs),    big.mark = ",")))
cat(sprintf("Gap PDF rows           : %s\n", format(nrow(df_jobs_gap),big.mark = ",")))
cat(sprintf("Overlap (in both)      : %s\n",
    format(sum(df_jobs_gap$caseNumber %in% df_jobs$caseNumber), big.mark = ",")))
cat(sprintf("Net new cases added    : %s\n",
    format(sum(!df_jobs_gap$caseNumber %in% df_jobs$caseNumber), big.mark = ",")))
cat(sprintf("Final patched rows     : %s\n", format(nrow(df_patched), big.mark = ",")))

# check gap coverage after patching
cat("\nFiling counts around gap period (dateSubmitted):\n")
df_patched %>%
  filter(!is.na(dateSubmitted),
         dateSubmitted >= as.Date("2023-10-01"),
         dateSubmitted <= as.Date("2024-03-31")) %>%
  mutate(month_yr = floor_date(dateSubmitted, "month")) %>%
  count(month_yr) %>%
  arrange(month_yr) %>%
  print()

saveRDS(df_patched, paste0(out_dir, "/df_jobs_patched.rds"))
cat(sprintf("\nSaved: df_jobs_patched.rds\n"))
cat(sprintf("Total time: %.1f min\n", as.numeric(Sys.time() - t0, units = "mins")))
