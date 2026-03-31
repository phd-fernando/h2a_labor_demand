###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Download PDFs for gap-period job orders (Nov 2023 – Feb 2024) missing from
# the DOL JSON feed. Saves one PDF per case number to:
#   raw/data/pdf_gap/{caseNumber}.pdf
#
# Design for polite scraping:
#   - Random delay 1–3 sec between requests
#   - Longer pause (10–20 sec) every 50 downloads
#   - Resume-safe: skips already-downloaded files
#   - Saves a log (pdf_gap_log.csv) with status per case
#   - Retries once on transient failures (5xx / timeout)
###############################################################################

cat("\014"); rm(list = ls())

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "httr")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

pdf_dir  <- paste0(mydir, "/raw/data/pdf_gap")
log_file <- paste0(pdf_dir, "/pdf_gap_log.csv")
base_url <- "https://seasonaljobs.dol.gov/api/job-order/"

# polite headers
ua <- user_agent("Mozilla/5.0 (research; h2a dissertation; contact: f.britogonzalez@ufl.edu)")

# ------------------------------------------------------------------------------
# 1. Load gap case list
# ------------------------------------------------------------------------------
gap_cases <- read_csv(paste0(pdf_dir, "/gap_cases.csv"), show_col_types = FALSE) %>%
  pull(caseNumber)

cat(sprintf("Gap cases to download: %d\n", length(gap_cases)))

# ------------------------------------------------------------------------------
# 2. Resume: load existing log, skip already done
# ------------------------------------------------------------------------------
if (file.exists(log_file)) {
  log <- read_csv(log_file, show_col_types = FALSE)
} else {
  log <- tibble(caseNumber = character(), status = character(),
                bytes = integer(), timestamp = as.POSIXct(character()))
}

done     <- log %>% filter(status == "ok") %>% pull(caseNumber)
todo     <- setdiff(gap_cases, done)
cat(sprintf("Already downloaded: %d  |  Remaining: %d\n", length(done), length(todo)))

# ------------------------------------------------------------------------------
# 3. Download loop
# ------------------------------------------------------------------------------
download_one <- function(j, retry = TRUE) {
  url     <- paste0(base_url, j)
  outfile <- paste0(pdf_dir, "/", j, ".pdf")

  r <- tryCatch(
    GET(url, ua, timeout(30), write_disk(outfile, overwrite = TRUE)),
    error = function(e) list(error = e$message)
  )

  if (!is.null(r$error)) {
    return(tibble(caseNumber = j, status = paste0("error: ", r$error),
                  bytes = 0L, timestamp = Sys.time()))
  }

  code  <- status_code(r)
  bytes <- file.info(outfile)$size

  if (code == 200 && bytes > 1000) {
    return(tibble(caseNumber = j, status = "ok",
                  bytes = as.integer(bytes), timestamp = Sys.time()))
  }

  # retry once on transient failure
  if (retry && code >= 500) {
    Sys.sleep(runif(1, 5, 10))
    return(download_one(j, retry = FALSE))
  }

  file.remove(outfile)
  return(tibble(caseNumber = j, status = paste0("http_", code),
                bytes = 0L, timestamp = Sys.time()))
}

t0    <- Sys.time()
n     <- length(todo)
batch <- 50   # pause every N downloads

for (i in seq_along(todo)) {
  j      <- todo[i]
  result <- download_one(j)

  # append to log
  log <- bind_rows(log, result)
  write_csv(log, log_file)

  elapsed <- as.numeric(Sys.time() - t0, units = "mins")
  rate    <- i / elapsed
  eta     <- (n - i) / rate

  cat(sprintf("  [%d/%d] %s  →  %-12s  %s MB  |  %.1f min elapsed  ETA %.0f min\n",
              i, n, j, result$status,
              round(result$bytes / 1e6, 2),
              elapsed, eta))

  # polite delay
  if (i %% batch == 0) {
    pause <- runif(1, 10, 20)
    cat(sprintf("  --- batch pause %.0f sec ---\n", pause))
    Sys.sleep(pause)
  } else {
    Sys.sleep(runif(1, 1, 3))
  }
}

# ------------------------------------------------------------------------------
# 4. Summary
# ------------------------------------------------------------------------------
cat("\n=== Download Summary ===\n")
log %>% count(status, sort = TRUE) %>% print()
cat(sprintf("Total PDFs on disk : %d\n",
            length(list.files(pdf_dir, pattern = "\\.pdf$"))))
cat(sprintf("Total size         : %.1f MB\n",
            sum(file.info(list.files(pdf_dir, pattern="\\.pdf$", full.names=TRUE))$size,
                na.rm=TRUE) / 1e6))
cat(sprintf("Total time         : %.1f min\n",
            as.numeric(Sys.time() - t0, units = "mins")))
