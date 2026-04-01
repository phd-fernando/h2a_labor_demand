###############################################################################
# Date fix v2: debug the join failure + investigate the Nov-1 hardcoding artifact
###############################################################################
cat("\014"); rm(list = ls())

library(dplyr); library(jsonlite); library(lubridate); library(stringr)

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)
zip_dir <- "C:/Data/DOL_SeasonalJobs/zip_backfill_15day"
tmp_dir <- file.path(zip_dir, "_extracted_jo")
parse_dol_date <- function(x) suppressWarnings(as.Date(x, format = "%d-%b-%Y"))

# ── 1. Peek at actual JSON content for a known case ───────────────────────────
cat("=== PEEK AT RAW JSON FIELD NAMES ===\n")
snap_dir <- file.path(tmp_dir, "2025-06-04")
jf <- list.files(snap_dir, pattern="\\.json$", full.names=TRUE)
raw1 <- fromJSON(jf[1], simplifyDataFrame=FALSE)
cat("Fields in first JO:", paste(names(raw1[[1]]), collapse=", "), "\n\n")
cat("Sample caseNumber:", raw1[[1]]$caseNumber, "\n")
cat("Sample dateSubmitted:", raw1[[1]]$dateSubmitted, "\n\n")

# ── 2. Load ONE zip flat to check column names ─────────────────────────────────
cat("=== FLAT PARSE COLUMN CHECK ===\n")
sample_flat <- fromJSON(jf[1], flatten=TRUE) |> as_tibble()
cat("Columns:", paste(names(sample_flat)[1:25], collapse=", "), "\n")
cat("dateSubmitted present:", "dateSubmitted" %in% names(sample_flat), "\n")
if ("dateSubmitted" %in% names(sample_flat)) {
  cat("Sample values:", paste(head(sample_flat$dateSubmitted, 5), collapse=", "), "\n")
}
cat("caseNumber sample:", paste(head(sample_flat$caseNumber, 3), collapse=", "), "\n\n")

# ── 3. Load patched and check caseNumber format ───────────────────────────────
cat("=== PATCHED FILE caseNumber CHECK ===\n")
patched <- readRDS("processed/text/df_jobs_patched.rds")
cat("caseNumber class:", class(patched$caseNumber), "\n")
cat("Sample:", paste(head(patched$caseNumber, 3), collapse=", "), "\n")
cat("dateSubmitted class:", class(patched$dateSubmitted), "\n\n")

# ── 4. SHUTDOWN ARTIFACT: check Nov-1 spike ───────────────────────────────────
cat("=== NOVEMBER 1 HARDCODING CHECK ===\n")
cat("Daily counts around Oct-Nov 2025 transition:\n")
patched |>
  filter(!is.na(dateSubmitted),
         dateSubmitted >= as.Date("2025-09-15"),
         dateSubmitted <= as.Date("2025-11-15")) |>
  count(dateSubmitted) |>
  arrange(dateSubmitted) |>
  print(n=60)

nov1_count <- sum(patched$dateSubmitted == as.Date("2025-11-01"), na.rm=TRUE)
cat(sprintf("\nJOs with dateSubmitted == 2025-11-01 exactly: %d\n", nov1_count))

# Compare Oct and Nov monthly totals
cat("\nOct/Nov 2025 monthly counts:\n")
patched |>
  filter(!is.na(dateSubmitted),
         year(dateSubmitted)==2025,
         month(dateSubmitted) %in% c(9,10,11,12)) |>
  count(ym=floor_date(dateSubmitted,"month")) |>
  print()

# ── 5. Build correct dateSubmitted from backfill JSONs directly ───────────────
# Read each JO record-by-record to capture dateSubmitted as a raw field
cat("\n=== BUILDING AUTHORITATIVE dateSubmitted FROM BACKFILL JSONs ===\n")
all_zips <- list.files(zip_dir, pattern="^sjCaseData_jo_202[56].*\\.zip$",
                       full.names=TRUE)

bf_records <- lapply(all_zips, function(zpath) {
  snap <- str_extract(basename(zpath), "\\d{4}-\\d{2}-\\d{2}")
  udir <- file.path(tmp_dir, snap)
  jfiles <- list.files(udir, pattern="\\.json$", full.names=TRUE)
  if (length(jfiles)==0) return(NULL)
  raw <- tryCatch(fromJSON(jfiles[1], simplifyDataFrame=FALSE), error=function(e) NULL)
  if (is.null(raw)) return(NULL)
  lapply(raw, function(jo) {
    tibble(caseNumber    = as.character(jo$caseNumber %||% NA),
           dateSubmitted_bf = parse_dol_date(jo$dateSubmitted %||% NA),
           snap_date     = as.Date(snap))
  }) |> bind_rows()
}) |> bind_rows()

cat(sprintf("Total records loaded: %d\n", nrow(bf_records)))
cat(sprintf("Unique caseNumbers:   %d\n", n_distinct(bf_records$caseNumber)))
cat("dateSubmitted_bf NA rate:", mean(is.na(bf_records$dateSubmitted_bf)), "\n")
cat("Sample caseNumbers:", paste(head(bf_records$caseNumber, 3), collapse=", "), "\n\n")

# Per case: use the earliest-snapshot record's dateSubmitted
bf_dates <- bf_records |>
  arrange(caseNumber, snap_date) |>
  distinct(caseNumber, .keep_all=TRUE) |>
  select(caseNumber, dateSubmitted_bf, snap_date_first=snap_date)

# ── 6. Join and diagnose ──────────────────────────────────────────────────────
cat("=== JOIN DIAGNOSIS ===\n")
joined <- patched |>
  select(caseNumber, dateSubmitted_old=dateSubmitted) |>
  left_join(bf_dates, by="caseNumber")

cat(sprintf("Rows with backfill match: %d\n", sum(!is.na(joined$dateSubmitted_bf))))
cat(sprintf("Rows with different dates: %d\n",
    sum(!is.na(joined$dateSubmitted_bf) & !is.na(joined$dateSubmitted_old) &
        joined$dateSubmitted_bf != joined$dateSubmitted_old, na.rm=TRUE)))

# ── 7. Nov-1 specific: how many Nov-1 JOs get a different date from backfill ──
nov1_cases <- patched |>
  filter(dateSubmitted == as.Date("2025-11-01")) |>
  pull(caseNumber)

cat(sprintf("\nNov-1 cases in patched:           %d\n", length(nov1_cases)))
nov1_in_bf <- bf_dates |> filter(caseNumber %in% nov1_cases)
cat(sprintf("Nov-1 cases found in backfill:    %d\n", nrow(nov1_in_bf)))

if (nrow(nov1_in_bf) > 0) {
  cat("\nBackfill dateSubmitted for Nov-1 cases:\n")
  nov1_in_bf |> count(dateSubmitted_bf) |> arrange(dateSubmitted_bf) |> print(n=20)
}

# ── 8. Apply fix ──────────────────────────────────────────────────────────────
cat("\n=== APPLYING DATE FIX ===\n")
patched_fixed <- patched |>
  left_join(bf_dates |> select(caseNumber, dateSubmitted_bf), by="caseNumber") |>
  mutate(
    dateSubmitted = case_when(
      !is.na(dateSubmitted_bf) ~ dateSubmitted_bf,
      TRUE                     ~ dateSubmitted
    )
  ) |>
  select(-dateSubmitted_bf)

n_changed <- sum(
  coalesce(patched_fixed$dateSubmitted, as.Date("1900-01-01")) !=
  coalesce(patched$dateSubmitted,       as.Date("1900-01-01")),
  na.rm=TRUE)
cat(sprintf("Dates changed: %d\n", n_changed))

cat("\nMonthly counts 2025 — BEFORE vs AFTER:\n")
b25 <- patched       |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |>
       count(ym=floor_date(dateSubmitted,"month"), name="before")
a25 <- patched_fixed |> filter(!is.na(dateSubmitted), year(dateSubmitted)==2025) |>
       count(ym=floor_date(dateSubmitted,"month"), name="after")
full_join(b25, a25, by="ym") |>
  mutate(delta=coalesce(after,0L)-coalesce(before,0L)) |>
  arrange(ym) |> print()

saveRDS(patched_fixed, "processed/text/df_jobs_patched.rds")
cat("Saved corrected df_jobs_patched.rds\nDone.\n")
