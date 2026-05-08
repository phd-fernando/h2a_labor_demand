###############################################################################
# Pipeline Step 1f v2 — streamlined.
# Order:
#   1. read classified sentences
#   2. build per-JO concatenated text (Spanish-stripped, incl. boilerplate)
#   3. crop extraction (text + Addendum A) using vectorized stringi::stri_detect_regex
#   4. drop boilerplate sentences
#   5. bigram count on the smaller post-boilerplate set (using stringi)
#   6. drop sentences with n_tc>=4 & n_farm==0
#   7. drop JOs with zero surviving sentences
#   8. join metadata + crops_str → output
#
# Streamlines applied:
#   #1 vectorize over patterns (str_detect once per pattern, not per text)
#   #2 drop boilerplate BEFORE bigram counting (3.1M instead of 5.6M)
#   #3 stringi::stri_detect_regex (compiled regex)
#   #4 single combined block for bigram-count + filter (was Steps 1f-A + 1f-C)
#   #5 cache per-JO concatenated text (jo_text_clean.parquet)
#   #6 reuse jo_meta upstream
###############################################################################
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(tidyr); library(stringi)
  library(tidytext); library(lubridate); library(readr)
})
DATA <- "output/text"
CORE <- file.path(DATA, "core")
DICT <- file.path(DATA, "dictionaries")
JO_TEXT_CACHE <- file.path(CORE, "jo_text_clean.parquet")

# ---- Vectorized + compiled-regex crop extraction ----
extract_crops_long <- function(text_vec, ids, crops_df) {
  text_low <- stri_trans_tolower(text_vec)
  parts <- vector("list", nrow(crops_df))
  for (k in seq_len(nrow(crops_df))) {
    hits <- stri_detect_regex(text_low, crops_df$pattern[k])
    if (any(hits, na.rm = TRUE)) {
      parts[[k]] <- tibble(id = ids[which(hits)],
                           crop_canonical = crops_df$crop[k])
    }
  }
  bind_rows(parts) |>
    distinct(id, crop_canonical)
}

# ---- Suppress generic canonical when a specific one is present (same JO) ----
# Keeps both as distinct categories; removes only the substring-match artifact.
suppress_generic <- function(df) {
  pairs <- tibble::tribble(
    ~generic,  ~specific,
    "potato",  "sweet potato",
    "corn",    "sweet corn",
    "pepper",  "pepper bell",
    "pepper",  "pepper chile",
    "bean",    "lima bean",
    "bean",    "navy bean",
    "bean",    "pinto bean",
    "bean",    "kidney bean",
    "bean",    "snap bean",
    "bean",    "dry bean",
    "pea",     "blackeye pea"
  )
  drops <- df |>
    inner_join(pairs, by = c("crop_canonical" = "specific")) |>
    distinct(caseNumber, crop_canonical = generic)
  df |> anti_join(drops, by = c("caseNumber", "crop_canonical"))
}

# ---- 1. Read classified sentences ----
sent_class <- arrow::read_parquet(file.path(CORE, "sentence_classification.parquet")) |>
  mutate(sid = row_number())
N0 <- nrow(sent_class); J0 <- n_distinct(sent_class$caseNumber)
cat("sentences loaded (incl. boilerplate): ", N0, " | unique JOs: ", J0, "\n", sep="")

# ---- 2. Per-JO concatenated text (cache) ----
if (file.exists(JO_TEXT_CACHE)) {
  jo_text <- arrow::read_parquet(JO_TEXT_CACHE)
  cat("Loaded cached jo_text_clean.parquet (", nrow(jo_text), " JOs)\n", sep="")
} else {
  jo_text <- sent_class |>
    group_by(caseNumber) |>
    summarise(text_full = paste(s, collapse = " "), .groups = "drop")
  arrow::write_parquet(jo_text, JO_TEXT_CACHE)
  cat("Cached jo_text_clean.parquet (", nrow(jo_text), " JOs)\n", sep="")
}

# ---- 3. Crop extraction (vectorized) ----
crops <- readRDS(file.path(DICT, "nass_crop_dictionary.rds"))
cat("Canonical commodities: ", nrow(crops), "\n", sep="")

cat("Extracting crops from per-JO text...\n")
t1 <- Sys.time()
crops_text <- extract_crops_long(jo_text$text_full, jo_text$caseNumber, crops) |>
  rename(caseNumber = id) |> mutate(source = "text")
cat("  ", round(as.numeric(Sys.time()-t1, units="mins"),1), " min, ",
    nrow(crops_text), " (JO,crop) rows\n", sep="")

ca <- arrow::read_parquet(file.path(CORE, "crops_activities_long.parquet")) |>
  filter(!is.na(addmaCropActivity), nchar(addmaCropActivity) > 0)
cat("Extracting crops from Addendum A activity strings...\n")
t2 <- Sys.time()
# Concatenate per JO so we get one text per caseNumber
ca_join <- ca |> group_by(caseNumber) |>
  summarise(addA = paste(addmaCropActivity, collapse = " | "), .groups="drop")
crops_add <- extract_crops_long(ca_join$addA, ca_join$caseNumber, crops) |>
  rename(caseNumber = id) |> mutate(source = "addendum")
cat("  ", round(as.numeric(Sys.time()-t2, units="mins"),1), " min, ",
    nrow(crops_add), " (JO,crop) rows\n", sep="")

jo_crops <- bind_rows(crops_text, crops_add) |>
  arrange(caseNumber, crop_canonical) |>
  distinct(caseNumber, crop_canonical, .keep_all = TRUE)

n_before <- nrow(jo_crops)
jo_crops <- suppress_generic(jo_crops)
cat("Suppressed generic when specific present: dropped ",
    n_before - nrow(jo_crops), " (JO,crop) rows\n", sep="")

arrow::write_parquet(jo_crops, file.path(CORE, "jo_crops.parquet"))
cat("Union: ", nrow(jo_crops), " (JO,crop) rows | ",
    n_distinct(jo_crops$caseNumber), " JOs with >=1 commodity\n", sep="")

# ---- 4. Drop boilerplate ----
sent <- sent_class |> filter(!is_boilerplate)
cat("After dropping boilerplate: ", nrow(sent), " (",
    round(100*nrow(sent)/N0,1), "%) | JOs: ",
    n_distinct(sent$caseNumber), "\n", sep="")

# ---- 5. Bigram count on the smaller set ----
# Use the manual_v3 dictionary: labels are "tc" / "farm" / "other".
dict <- read.delim(file.path(DICT, "bigram_dictionary_manual.tsv"),
                   stringsAsFactors = FALSE)
tc   <- dict |> filter(label == "tc")   |> pull(bigram)
farm <- dict |> filter(label == "farm") |> pull(bigram)

cat("Counting bigrams on post-boilerplate sentences...\n")
t3 <- Sys.time()
bg <- sent |> select(sid, s) |>
  unnest_tokens(bigram, s, token = "ngrams", n = 2)
counts <- bg |> mutate(is_tc = bigram %in% tc, is_farm = bigram %in% farm) |>
  group_by(sid) |>
  summarise(n_tc = sum(is_tc), n_farm = sum(is_farm), .groups = "drop")
sent <- sent |> left_join(counts, by = "sid") |>
  mutate(n_tc = coalesce(n_tc, 0L), n_farm = coalesce(n_farm, 0L))
cat("  ", round(as.numeric(Sys.time()-t3, units="mins"),2), " min\n", sep="")

# ---- 6. Keep only n_tc == 0 & n_farm >= 1 ----
sent <- sent |> filter(n_tc == 0 & n_farm >= 1)
cat("After keep [n_tc==0 & n_farm>=1]: ", nrow(sent), " | JOs: ",
    n_distinct(sent$caseNumber), "\n", sep="")

# ---- 7. (implicit) JOs with zero surviving sentences are absent already ----

# ---- 8. Metadata join + crops string ----
jo_meta <- arrow::read_parquet(file.path(CORE, "jo_full.parquet")) |>
  mutate(subm = as.Date(dateSubmitted),
         year  = year(subm),
         month = month(subm)) |>
  select(caseNumber, socCode, jobState, year, month)

crops_str <- jo_crops |>
  group_by(caseNumber) |>
  summarise(crops = paste(sort(unique(crop_canonical)), collapse = ";"),
            n_crops = n(), .groups = "drop")

out <- sent |>
  arrange(caseNumber, sid) |>
  select(caseNumber, sid, sentence = s, n_tc, n_farm) |>
  left_join(crops_str, by = "caseNumber") |>
  left_join(jo_meta,    by = "caseNumber") |>
  mutate(crops   = coalesce(crops, ""),
         n_crops = coalesce(n_crops, 0L))

cat("\nFinal sentence file rows: ", nrow(out), "\n", sep="")
cat("  unique JOs:               ", n_distinct(out$caseNumber), "\n", sep="")
cat("  JOs with >=1 crop:        ",
    n_distinct(out$caseNumber[out$n_crops > 0]), "\n", sep="")
cat("  JOs with NO crop:         ",
    n_distinct(out$caseNumber[out$n_crops == 0]), "\n", sep="")

arrow::write_parquet(out, file.path(CORE, "sentences.parquet"))
write_tsv(out, file.path(CORE, "sentences.tsv"))
cat("\nSaved sentences.parquet and .tsv\n")
cat("Saved jo_crops.parquet\n")
