###############################################################################
# Sentence-level Spanish-text cleaning pipeline:
#   1. Split each JO's duties_full into sentences using the splitter
#      (bullets, ;, " - ", ":<Cap>", and . ! ? + whitespace).
#   2. For each sentence, count Spanish-marker hits.  If hits >= 3, trim that
#      sentence: keep prefix (before first hit) + suffix (after last hit).
#   3. Drop sentences <15 chars after trim.
#   4. Flag boilerplate (>=1% verbatim within year OR overall).
###############################################################################
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(tidyr); library(lubridate)
})

DATA <- "output/text"
CORE <- file.path(DATA, "core")

jo <- arrow::read_parquet(file.path(CORE, "jo_full.parquet")) |>
  mutate(subm = as.Date(dateSubmitted), yr = year(subm)) |>
  filter(subm >= as.Date("2019-10-01"), source %in% c("json","pdf_gap"),
         !is.na(duties_full), nchar(duties_full) > 50)
cat("In-scope JOs:", nrow(jo), "\n")

# Spanish marker pattern: function words + morphology + content words.
es_function <- c("los","las","del","una","uno","para","por","sin","con","hasta",
                 "desde","que","porque","cuando","donde","aunque","sus","ella",
                 "ellos","ustedes","nosotros","como","esta","este","estos","estas",
                 "tambien","esto","eso","aqui","alli","muy","mas","menos")
es_content  <- c("trabajadores","empleador","empleadores","vivienda","cosecha",
                 "siembra","salario","herramientas","operador","empleado",
                 "trabajador","ranchero","cuidador","tareas","pastoreo")
es_words <- unique(c(es_function, es_content))
# Pattern: word match (literal) OR -ción suffix OR -mente suffix OR contains ñ or accents
pat <- paste0(
  "\\b(?:", paste(es_words, collapse="|"), ")\\b",
  "|\\b\\w+ci[oó]n\\b",
  "|\\b\\w+mente\\b",
  "|[ñáéíóú]"
)
cat("Pattern length:", nchar(pat), "\n")

# Sentence splitter
preprocess <- function(x) {
  x <- gsub("[•·◦‣⁃*]", "\n", x, perl = TRUE)
  x <- gsub(";", "\n", x, fixed = TRUE)
  x <- gsub(":\\s+([A-Za-z])", "\n\\1", x, perl = TRUE)
  x <- gsub(" - ", "\n", x, fixed = TRUE)
  x <- gsub("[ \\t]+", " ", x, perl = TRUE)
  x
}
custom_split <- function(text) {
  parts <- unlist(strsplit(text, "\n+", perl = TRUE))
  parts <- unlist(lapply(parts, function(p)
    unlist(strsplit(p, "(?<=[.!?])\\s+", perl = TRUE))))
  parts <- str_squish(parts)
  parts[nchar(parts) > 0]
}

# ---- Step 1: split into sentences ----
cat("Splitting sentences...\n")
t1 <- Sys.time()
sent <- jo |> select(caseNumber, yr, duties_full) |>
  filter(nchar(duties_full) > 0) |>
  mutate(s = lapply(preprocess(duties_full), custom_split)) |>
  select(caseNumber, yr, s) |> tidyr::unnest(s) |>
  filter(nchar(s) >= 15)
cat("  ", round(as.numeric(Sys.time()-t1, units="mins"),1),
    " min | sentences before Spanish trim: ",
    nrow(sent), "\n", sep="")

# ---- Step 2: per-sentence Spanish detect & trim ----
cat("Per-sentence Spanish detection (>=3 hits triggers trim)...\n")
t2 <- Sys.time()
hits_per_sent <- str_locate_all(str_to_lower(sent$s), pat)
n_hits_sent <- vapply(hits_per_sent, nrow, integer(1))
trim_idx <- which(n_hits_sent >= 3)
cat("  Sentences with >=3 Spanish hits: ", length(trim_idx), " (",
    round(100*length(trim_idx)/nrow(sent),3), "%)\n", sep="")

sent$s_orig <- sent$s
if (length(trim_idx) > 0) {
  x1 <- vapply(hits_per_sent[trim_idx], function(m) min(m[,"start"]), integer(1))
  x2 <- vapply(hits_per_sent[trim_idx], function(m) max(m[,"end"]),   integer(1))
  txt <- sent$s[trim_idx]
  n_chars <- nchar(txt)
  prefix <- ifelse(x1 > 1, substr(txt, 1, x1 - 1), "")
  suffix <- ifelse(x2 < n_chars, substr(txt, x2 + 1, n_chars), "")
  sent$s[trim_idx] <- str_squish(paste(prefix, suffix))
}
cat("  ", round(as.numeric(Sys.time()-t2, units="mins"),1), " min\n", sep="")

# Drop sentences that became <15 chars after trim
n_before_drop <- nrow(sent)
sent <- sent |> filter(nchar(s) >= 15) |>
  mutate(s_norm = str_squish(str_to_lower(str_remove_all(s, "[^a-z0-9 ]"))))
cat("Sentences kept after Spanish trim + length filter: ", nrow(sent),
    " (dropped ", n_before_drop - nrow(sent), ")\n", sep="")

# Boilerplate = repeated >=1% within a year OR >=1% overall
n_jos_total <- nrow(jo)
n_by_yr <- jo |> count(yr, name = "n_jos_yr")

boil_yr <- sent |> distinct(caseNumber, yr, s_norm) |>
  count(yr, s_norm, name = "n_jos_with") |>
  inner_join(n_by_yr, by = "yr") |>
  mutate(pct = 100 * n_jos_with / n_jos_yr) |>
  filter(pct >= 1)
cat("Year-boilerplate flagged:", nrow(boil_yr), "\n")

boil_overall <- sent |> distinct(caseNumber, s_norm) |>
  count(s_norm, name = "n_jos_with") |>
  mutate(pct = 100 * n_jos_with / n_jos_total) |>
  filter(pct >= 1)
cat("Overall-boilerplate flagged:", nrow(boil_overall), "unique norms\n")

sent$is_boilerplate_yr <- paste(sent$yr, sent$s_norm) %in%
                          paste(boil_yr$yr, boil_yr$s_norm)
sent$is_boilerplate_overall <- sent$s_norm %in% boil_overall$s_norm
sent$is_boilerplate <- sent$is_boilerplate_yr | sent$is_boilerplate_overall
cat("Sentences flagged year-only:",
    sum(sent$is_boilerplate_yr & !sent$is_boilerplate_overall), "\n")
cat("Sentences flagged overall-only:",
    sum(sent$is_boilerplate_overall & !sent$is_boilerplate_yr), "\n")
cat("Sentences flagged both:",
    sum(sent$is_boilerplate_yr & sent$is_boilerplate_overall), "\n")
kept <- sent |> filter(!is_boilerplate)
cat("Kept sentences (non-boilerplate):", nrow(kept), "\n")

cat("\n=== Top 5 longest surviving sentences ===\n")
top <- kept |> arrange(desc(nchar(s))) |> head(5)
for (i in 1:5) {
  cat("[", i, "] (", nchar(top$s[i]), " chars):\n", top$s[i], "\n\n", sep="")
}

arrow::write_parquet(sent |> select(caseNumber, yr, s, s_norm, is_boilerplate),
        file.path(CORE, "sentence_classification.parquet"))
cat("Saved sentence_classification.parquet\n")
