###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Full re-parse of all 10,046 gap-period PDFs using the fun() approach from
# myfuns_h2ajson.R — ported to use local files instead of URL download.
#
# Extracts ALL sections (vs. the partial extraction in 8_parse_gap_pdfs.R):
#   Page 1 : A.8a (job duties), A.11 (deductions/fringe)       ← A.11 NEW
#   Page 2 : B.6 (qualifications), C.6 (worksite), D.6 (housing)
#   Page 3 : E.1 (meals), F.1 (transport), F.2 (reimbursement)
#   Page 4 : G.1 (recruitment)                                 ← G.1 NEW
#   Pages 5-8 : skipped (continuation of ETA-790A boilerplate)
#   Pages >8 with "H." at (x=36, y=88-93):
#             Addendum C of ETA-790A — "H. Additional Material Terms and
#             Conditions of the Job Offer" — contains the ACTUAL job duties
#             for cases where A.8a says "See Addendum C"        ← NEW
#
# Output: processed/text/df_text_gap_full.rds
#         (replaces df_text_gap.rds for all downstream text analysis)
###############################################################################

cat("\014"); rm(list = ls())

mydir     <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
pdf_dir   <- paste0(mydir, "/raw/data/pdf_gap")
out_dir   <- paste0(mydir, "/processed/text")
batch_dir <- paste0(out_dir, "/gap_batches_full")
setwd(mydir)

dir.create(batch_dir, showWarnings = FALSE, recursive = TRUE)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "pdftools")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# HELPER FUNCTIONS  (ported from myfuns_h2ajson.R)
# ==============================================================================

null.df <- function() {
  data.frame(
    caseNumber          = character(),
    addmcSectionDetails = character(),
    addmcSectionNumber  = character(),
    addmcSectionName    = character(),
    stringsAsFactors    = FALSE
  )
}

# Extract text from a standard-width region (0.75–7.99 in)
txt.pdf <- function(x, y0, y1, label, id) {
  data.frame(
    caseNumber = id,
    addmcSectionDetails = x |> as.data.frame() |>
      filter(x >= 0.75*72, x <= 7.99*72, y >= y0*72, y < y1*72) |>
      pull(text) |> paste0(collapse = " "),
    addmcSectionNumber = label,
    addmcSectionName   = label,
    stringsAsFactors   = FALSE
  )
}

# Extract text from a wide region (0.5–10.5 in) — used for Addendum H rows
txt.pdfw <- function(x, y0, y1, label, id) {
  data.frame(
    caseNumber = id,
    addmcSectionDetails = x |> as.data.frame() |>
      filter(x >= 0.5*72, x <= 10.5*72, y >= y0*72, y < y1*72) |>
      pull(text) |> paste0(collapse = " "),
    addmcSectionNumber = str_split(label, " ")[[1]][5],
    addmcSectionName   = str_split(label, "\\*\\ ")[[1]][3],
    stringsAsFactors   = FALSE
  )
}

# Return raw text string from a wide region (used as label argument to txt.pdfw)
add.pdf <- function(x, y0, y1) {
  x |> as.data.frame() |>
    filter(x >= 0.5*72, x <= 10.5*72, y >= y0*72, y < y1*72) |>
    pull(text) |> paste0(collapse = " ")
}

# Find y (in inches) of pattern1 that appears AFTER pattern2 on the same page
str.xyz <- function(x, pattern2, pattern1) {
  a2 <- x[str_detect(x$text, pattern2), ] |> pull(y) |> min()
  if (is.infinite(a2)) a2 <- 0
  a1 <- x[str_detect(x$text, pattern1), ] |>
    filter(y > a2) |> pull(y) |> min()
  return(a1 / 72)
}

# Find y (in inches) of the row where EXACTLY all words of pattern1 appear,
# after the row where all words of pattern2 appear (multi-word anchor version)
str.xyz2 <- function(x, pattern2, pattern1) {
  p1  <- str_split(pattern1, " ")[[1]]; n1 <- length(p1)
  p2  <- str_split(pattern2, " ")[[1]]; n2 <- length(p2)
  re1 <- paste0("(", paste0(p1, collapse = ")|("), ")")
  re2 <- paste0("(", paste0(p2, collapse = ")|("), ")")

  a2 <- x[str_detect(x$text, re2), ] |>
    group_by(y) |> summarise(n = n(), .groups = "drop") |>
    filter(n == n2) |> pull(y) |> min()
  if (is.infinite(a2)) a2 <- 0

  a1 <- x[str_detect(x$text, re1), ] |>
    group_by(y) |> summarise(n = n(), .groups = "drop") |>
    filter(n == n1, y > a2) |> pull(y) |> min()
  return(a1 / 72)
}

# Process one page — mirrors fun() from myfuns_h2ajson.R exactly.
# page is a global counter reset to 0 before each PDF.
fun <- function(x, id) {
  df0_temp <- null.df()
  page <<- page + 1

  if (page == 1) {

    df0_temp <- bind_rows(
      txt.pdf(x,
              str.xyz(x, "^A\\.$", "^8[a]\\.$") + (9+12)/72,
              str.xyz(x, "^A\\.$", "^8[bcde]\\.$"),
              "A.8a", id),
      txt.pdf(x,
              str.xyz(x, "^A\\.$", "^11\\.$") + (9+12)/72,
              str.xyz(x, "^A\\.$", "^ETA-790A$"),
              "A.11", id)
    )

  } else if (page == 2) {

    df0_temp <- bind_rows(
      txt.pdf(x,
              str.xyz(x, "^B\\.$", "^6\\.$") + (9+12)/72,
              str.xyz(x, "^B\\.$", "^C\\.$"),
              "B.6", id),
      txt.pdf(x,
              str.xyz(x, "^C\\.$", "^6\\.$") + 12/72,
              str.xyz(x, "^C\\.$", "^7\\.$"),
              "C.6", id),
      txt.pdf(x,
              str.xyz(x, "^D\\.$", "^10\\.$") + 12/72,
              str.xyz(x, "^D\\.$", "^11\\.$"),
              "D.6", id)
    )

  } else if (page == 3) {

    df0_temp <- bind_rows(
      txt.pdf(x,
              str.xyz(x, "^E\\.$", "^1\\.$") + (9+12+12)/72,
              str.xyz(x, "^E\\.$", "^2\\.$"),
              "E.1", id),
      txt.pdf(x,
              str.xyz(x, "^F\\.$", "^1\\.$") + (9+12)/72,
              str.xyz(x, "^F\\.$", "^2\\.$"),
              "F.1", id),
      txt.pdf(x,
              str.xyz(x, "^F\\.$", "^2\\.$") + (9+12+12)/72,
              str.xyz(x, "^F\\.$", "^3\\.$"),
              "F.2", id)
    )

  } else if (page == 4) {

    df0_temp <- txt.pdf(x,
                        str.xyz(x, "^G\\.$", "^1\\.$") + (9+12+12+12)/72,
                        str.xyz(x, "^G\\.$", "^2\\.$"),
                        "G.1", id)

  } else if (page > 8) {

    # Detect addendum letter at x=36, y=88-93 (top-left of addendum page)
    addendum <- x |> as.data.frame() |>
      filter(x == 36, y >= 88, y <= 93) |>
      pull(text)
    if (length(addendum) == 0) addendum <- ""

    # "H. Additional Material Terms and Conditions" — this is the Addendum C
    # form (ETA-790A Addendum C) which carries the actual job duties when
    # Section A.8a on the main form says "See Addendum C".
    if (identical(addendum, "H.")) {
      df0_temp <- bind_rows(
        txt.pdfw(x,
                 str.xyz2(x, "Job Offer Information", "3\\. Details"),
                 str.xyz2(x, "3\\. Details", "Job Offer Information"),
                 add.pdf(x,
                         str.xyz2(x, "H\\.", "Job Offer Information") + 12/72,
                         str.xyz2(x, "Job Offer Information", "3\\. Details")),
                 id),
        txt.pdfw(x,
                 str.xyz2(x, "3\\. Details", "3\\. Details"),
                 str.xyz2(x, "3\\. Details", "FOR DEPARTMENT OF LABOR USE ONLY"),
                 add.pdf(x,
                         str.xyz2(x, "Job Offer Information", "Job Offer Information") + 12/72,
                         str.xyz2(x, "3\\. Details", "3\\. Details")),
                 id)
      )
    }
  }

  return(df0_temp)
}

# ==============================================================================
# VALIDATION — run on 5 samples before full pass
# ==============================================================================
pdf_files <- sort(list.files(pdf_dir, pattern = "[.]pdf$", full.names = TRUE))
n         <- length(pdf_files)

cat(sprintf("=== Validation on 5 samples ===\n"))
test_idx <- c(1, 2, 3, 100, 500)

for (i in test_idx) {
  fp  <- pdf_files[i]
  id  <- tools::file_path_sans_ext(basename(fp))
  pages_raw <- pdftools::pdf_data(fp)
  page <- 0
  res  <- tryCatch(
    do.call(bind_rows, lapply(pages_raw, fun, id)),
    error = function(e) { cat("  ERROR:", conditionMessage(e), "\n"); null.df() }
  )
  cat(sprintf("\n%s  (%d pages)\n", id, length(pages_raw)))
  res |>
    mutate(preview = substr(addmcSectionDetails, 1, 60)) |>
    select(addmcSectionNumber, preview) |>
    as.data.frame() |> print()
}
cat("\nValidation complete — review above before committing to full run.\n\n")

# ==============================================================================
# FULL PASS — resume-safe batches of 100 PDFs each
# ==============================================================================
batch_size <- 100
batches    <- split(seq_len(n), ceiling(seq_len(n) / batch_size))
cat(sprintf("Starting full pass: %d PDFs | %d batches of %d\n\n",
            n, length(batches), batch_size))
t0 <- Sys.time()

for (b in seq_along(batches)) {
  out_file <- sprintf("%s/batch_%04d.rds", batch_dir, b)
  if (file.exists(out_file)) next   # skip already-done batches

  idx        <- batches[[b]]
  batch_list <- vector("list", length(idx))

  for (k in seq_along(idx)) {
    fp <- pdf_files[idx[k]]
    id <- tools::file_path_sans_ext(basename(fp))

    batch_list[[k]] <- tryCatch({
      pdf_pages <- pdftools::pdf_data(fp)
      page      <- 0              # reset before each PDF
      do.call(bind_rows, lapply(pdf_pages, fun, id))
    }, error = function(e) null.df())
  }

  saveRDS(bind_rows(batch_list), out_file)
  elapsed <- as.numeric(Sys.time() - t0, units = "mins")
  cat(sprintf("  Batch %3d/%d  |  PDFs %d–%d  |  %.1f min\n",
              b, length(batches), idx[1], idx[length(idx)], elapsed))
  gc()
}

# ==============================================================================
# COMBINE & CLEAN
# ==============================================================================
cat("\nCombining all batches...\n")
batch_files  <- sort(list.files(batch_dir, pattern = "^batch_[0-9]+[.]rds$",
                                full.names = TRUE))
df_text_full <- bind_rows(lapply(batch_files, readRDS))

# Drop rows where section boundary search returned no content (Inf y → empty string)
df_text_full <- df_text_full |>
  filter(!is.na(caseNumber),
         nchar(trimws(coalesce(addmcSectionDetails, ""))) > 0)

cat(sprintf("\nTotal rows       : %s\n", format(nrow(df_text_full), big.mark = ",")))
cat(sprintf("Unique caseNumbers: %s\n", format(n_distinct(df_text_full$caseNumber), big.mark = ",")))
cat("\nSection counts:\n")
df_text_full |> count(addmcSectionNumber, sort = TRUE) |> as.data.frame() |> print()

# How many cases now have Addendum C / H content?
cat("\nCases with Addendum H. section (= Addendum C job duties):\n")
df_text_full |>
  filter(!is.na(addmcSectionNumber), str_detect(addmcSectionNumber, "^[A-Z0-9]")) |>
  filter(!addmcSectionNumber %in% c("A.8a","A.11","B.6","C.6","D.6","E.1","F.1","F.2","G.1")) |>
  count(addmcSectionNumber, sort = TRUE) |> as.data.frame() |> print()

# Compare A.8a "See Addendum C" vs. now recoverable
n_see_addc <- df_text_full |>
  filter(addmcSectionNumber == "A.8a",
         str_detect(tolower(addmcSectionDetails), "see addendum c")) |>
  n_distinct("caseNumber")

cat(sprintf("\nA.8a still showing 'See Addendum C': %d cases\n", n_see_addc))
cat(sprintf("  (these should now have H-section rows with actual duties)\n"))

# ==============================================================================
# SAVE
# ==============================================================================
saveRDS(df_text_full, paste0(out_dir, "/df_text_gap_full.rds"))
cat(sprintf("\nSaved: processed/text/df_text_gap_full.rds\n"))
cat(sprintf("Total time: %.1f min\n", as.numeric(Sys.time() - t0, units = "mins")))
