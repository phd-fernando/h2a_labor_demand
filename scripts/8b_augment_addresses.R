###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Supplementary pass: extract worksite street address + housing fields
# from the gap-period PDFs (page 2 only). Augments existing batch RDS files
# without re-parsing everything. Then re-runs combine + patch.
#
# New fields added:
#   worksiteAddr  — C.1 worksite street address
#   housingAddr   — D.1 housing street address
#   housingCity   — D.2 city
#   housingState  — D.3 state (2-letter)
#   housingZip    — D.4 postal code
#   housingCounty — D.5 county
#   housingType   — D.6 type: "Employer-provided" | "Rental or public" | NA
###############################################################################

cat("\014"); rm(list = ls())

mydir    <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
pdf_dir  <- paste0(mydir, "/raw/data/pdf_gap")
out_dir  <- paste0(mydir, "/processed/text")
batch_dir <- paste0(out_dir, "/gap_batches")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "pdftools")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# state name → abbreviation lookup
state_lookup <- setNames(state.abb, toupper(state.name))
state_lookup <- c(state_lookup,
                  "DISTRICT OF COLUMBIA" = "DC",
                  "PUERTO RICO" = "PR", "GUAM" = "GU")

# ── Helper: extract address row (street tokens at given y band) ───────────────
addr_row <- function(p2_df, y_min, y_max) {
  toks <- p2_df %>%
    filter(y >= y_min, y <= y_max, x >= 54, x <= 520) %>%
    arrange(x) %>% pull(text) %>%
    str_squish() %>% paste(collapse = " ") %>% str_squish()
  if (nchar(toks) > 1) toks else NA_character_
}

# ── Helper: extract city/state/zip/county row ─────────────────────────────────
loc_fields <- function(p2_df, y_min, y_max) {
  loc <- p2_df %>% filter(y >= y_min, y <= y_max) %>% arrange(x)

  zip <- loc %>% filter(str_detect(text, "^[0-9]{5}")) %>%
    pull(text) %>% str_extract("^[0-9]{5}") %>% first()

  state_candidate <- loc %>%
    filter(x >= 150, x <= 345, !str_detect(text, "^[0-9]")) %>%
    arrange(x) %>% pull(text) %>% paste(collapse = " ") %>% toupper()
  st <- state_lookup[state_candidate]
  if (is.na(st)) {
    st_tok <- loc %>% filter(x >= 150, x <= 345) %>% pull(text) %>%
      .[toupper(.) %in% names(state_lookup)] %>% first()
    st <- if (!is.null(st_tok) && !is.na(st_tok))
      state_lookup[toupper(st_tok)] else NA_character_
  }

  city <- loc %>% filter(x < 200) %>% arrange(x) %>%
    pull(text) %>% paste(collapse = " ") %>% str_squish()

  county <- loc %>% filter(x > 350) %>% arrange(x) %>%
    pull(text) %>% paste(collapse = " ") %>% str_squish()

  list(
    city   = if (nchar(city) > 0) city else NA_character_,
    state  = unname(st) %||% NA_character_,
    zip    = zip %||% NA_character_,
    county = if (nchar(county) > 0) county else NA_character_
  )
}

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a[1])) a[1] else b

# ── Extract new fields for one PDF (page 2 only) ──────────────────────────────
extract_p2_extras <- function(filepath) {
  id    <- tools::file_path_sans_ext(basename(filepath))
  pages <- tryCatch(pdftools::pdf_data(filepath), error = function(e) NULL)
  if (is.null(pages) || length(pages) < 2) {
    return(tibble(caseNumber = id,
                  worksiteAddr = NA_character_,
                  housingAddr = NA_character_, housingCity = NA_character_,
                  housingState = NA_character_, housingZip = NA_character_,
                  housingCounty = NA_character_, housingType = NA_character_))
  }

  p2 <- as.data.frame(pages[[2]])

  # Worksite street: y≈362–374 (between "Address/Location *" label at y≈355
  # and the city/state labels at y≈376)
  ws_street <- addr_row(p2, 360, 374)

  # Housing street: y≈538–550 (between D.1 label at y≈532 and city labels at y≈553)
  h_street  <- addr_row(p2, 538, 551)

  # Housing city/state/zip/county: y≈560–572
  h_loc <- loc_fields(p2, 560, 572)

  # Housing type: find which checkbox is ticked near y≈586–592
  # "Employer-provided" is left (~x=82), "Rental or public" is right (~x=238)
  # A ✔ mark appears at the selected option
  checkmark_x <- p2 %>%
    filter(str_detect(text, "^[✔✓☑]$"), y >= 582, y <= 595) %>%
    pull(x) %>% first()

  housing_type <- if (is.na(checkmark_x)) {
    NA_character_
  } else if (checkmark_x < 160) {
    "Employer-provided"
  } else {
    "Rental or public"
  }

  tibble(
    caseNumber   = id,
    worksiteAddr = ws_street,
    housingAddr  = h_street,
    housingCity  = h_loc$city,
    housingState = h_loc$state,
    housingZip   = h_loc$zip,
    housingCounty = h_loc$county,
    housingType  = housing_type
  )
}

# ==============================================================================
# QUICK VALIDATION on 5 samples before running full pass
# ==============================================================================
cat("=== Validation on 5 samples ===\n")
test_files <- sort(list.files(pdf_dir, pattern = "\\.pdf$", full.names = TRUE))[c(1,2,3,100,500)]
test_result <- map_dfr(test_files, extract_p2_extras)
print(test_result, width = 120)
cat("\nValidation OK? Review above then the full pass will run.\n\n")

# ==============================================================================
# FULL SUPPLEMENTARY PASS — read page 2 of every PDF, build augment table
# ==============================================================================
pdf_files <- sort(list.files(pdf_dir, pattern = "\\.pdf$", full.names = TRUE))
n         <- length(pdf_files)
cat(sprintf("Extracting address fields from %d PDFs...\n", n))
t0 <- Sys.time()

augment_list <- vector("list", n)
for (i in seq_along(pdf_files)) {
  augment_list[[i]] <- tryCatch(
    extract_p2_extras(pdf_files[[i]]),
    error = function(e) tibble(caseNumber = tools::file_path_sans_ext(basename(pdf_files[[i]])))
  )
  if (i %% 500 == 0)
    cat(sprintf("  %d/%d  (%.1f min)\n", i, n,
                as.numeric(Sys.time() - t0, units = "mins")))
}

df_augment <- bind_rows(augment_list)
cat(sprintf("Done. %.1f min\n", as.numeric(Sys.time() - t0, units = "mins")))

cat("\n--- Coverage summary ---\n")
for (col in names(df_augment)[-1])
  cat(sprintf("  %-15s: %d non-NA (%.1f%%)\n", col,
              sum(!is.na(df_augment[[col]])),
              mean(!is.na(df_augment[[col]])) * 100))

# ==============================================================================
# REBUILD df_jobs_gap WITH NEW FIELDS + RE-PATCH
# ==============================================================================
cat("\nMerging augmented fields into df_jobs_gap...\n")
df_jobs_gap <- readRDS(paste0(out_dir, "/df_jobs_gap.rds"))
df_jobs_gap <- df_jobs_gap %>%
  left_join(df_augment, by = "caseNumber")

saveRDS(df_jobs_gap, paste0(out_dir, "/df_jobs_gap.rds"))
cat("df_jobs_gap.rds updated with address fields.\n")

# Re-patch
cat("\nRe-patching df_jobs.rds...\n")
df_jobs <- readRDS(paste0(out_dir, "/df_jobs.rds")) %>%
  mutate(source = "json",
         worksiteAddr = NA_character_,
         housingAddr = NA_character_, housingCity = NA_character_,
         housingState = NA_character_, housingZip = NA_character_,
         housingCounty = NA_character_, housingType = NA_character_)

common_cols <- intersect(names(df_jobs), names(df_jobs_gap))
df_patched <- bind_rows(
  df_jobs     %>% select(all_of(common_cols)),
  df_jobs_gap %>% select(all_of(common_cols))
) %>%
  arrange(caseNumber, desc(source == "json")) %>%
  distinct(caseNumber, .keep_all = TRUE)

cat(sprintf("Patched rows: %s\n", format(nrow(df_patched), big.mark = ",")))

# Gap coverage check
cat("\nFiling counts Nov 2023 – Mar 2024:\n")
df_patched %>%
  filter(!is.na(dateSubmitted),
         dateSubmitted >= as.Date("2023-10-01"),
         dateSubmitted <= as.Date("2024-03-31")) %>%
  mutate(month_yr = lubridate::floor_date(dateSubmitted, "month")) %>%
  count(month_yr) %>% arrange(month_yr) %>% print()

saveRDS(df_patched, paste0(out_dir, "/df_jobs_patched.rds"))
cat(sprintf("Saved: df_jobs_patched.rds\nTotal time: %.1f min\n",
            as.numeric(Sys.time() - t0, units = "mins")))
