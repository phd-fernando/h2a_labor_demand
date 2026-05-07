###############################################################################
# Export sentences for Webb 2020 replication.
# Filter: n_tc < 3 AND n_farm > 0 (mostly-farm content with limited T&C).
###############################################################################
suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(tidyr); library(tidytext); library(lubridate); library(readr)
})
DATA <- "h2a_labor_demand/processed/text"

sent <- readRDS(file.path(DATA, "core", "sentence_classification_v3.rds")) |>
  filter(!is_boilerplate) |> mutate(sid = row_number())
dict <- read.csv(file.path(DATA, "dictionaries", "bigram_dictionary.csv"), stringsAsFactors = FALSE)
tc <- dict |> filter(label == 0) |> pull(bigram)
farm <- dict |> filter(label == 1) |> pull(bigram)

bg <- sent |> select(sid, s) |> unnest_tokens(bigram, s, token = "ngrams", n = 2)
counts <- bg |> mutate(is_tc = bigram %in% tc, is_farm = bigram %in% farm) |>
  group_by(sid) |> summarise(n_tc = sum(is_tc), n_farm = sum(is_farm), .groups = "drop")
sent <- sent |> left_join(counts, by = "sid") |>
  mutate(n_tc = coalesce(n_tc, 0L), n_farm = coalesce(n_farm, 0L))

keep <- sent |> filter(n_tc < 3, n_farm > 0)
cat("Sentences kept (n_tc<3, n_farm>0):", nrow(keep), "\n")
cat("Distinct JOs:", n_distinct(keep$caseNumber), "\n")

# Pull soc, state, year, primary crop from jo_full + crops_activities_long
jo <- readRDS(file.path(DATA, "core", "jo_full.rds")) |>
  mutate(year_subm = year(as.Date(dateSubmitted))) |>
  select(caseNumber, socCode, jobState, year_subm)
ca <- readRDS(file.path(DATA, "core", "crops_activities_long.rds")) |>
  filter(!is.na(addmaCropActivity), nchar(addmaCropActivity) > 0) |>
  arrange(caseNumber, entry_idx) |>
  group_by(caseNumber) |>
  summarise(crop = first(addmaCropActivity), .groups = "drop")

out <- keep |>
  left_join(jo, by = "caseNumber") |>
  left_join(ca, by = "caseNumber") |>
  transmute(doc_id = caseNumber,
            sentence = s,
            soc_code = socCode,
            crop = crop,
            state = jobState,
            year = year_subm)

OUT <- "h2a_labor_demand/webb_replication/data/tasks/h2a_sentences.tsv"
write_tsv(out, OUT)
cat("Saved:", OUT, "\nRows:", nrow(out),
    " | with crop:", sum(!is.na(out$crop)),
    " | with soc:", sum(!is.na(out$soc_code)), "\n")
