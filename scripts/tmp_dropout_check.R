library(dplyr); library(stringr); library(lubridate)

enr      <- readRDS("processed/text/df_jobs_enriched.rds")
txt_full <- readRDS("processed/text/df_text_gap_full.rds")
df_crops <- readRDS("processed/text/df_crops.rds")

addc_prefix <- regex(
  paste0("^3[.]\\s*Details\\s+of\\s+Material\\s+Term\\s+or\\s+Condition",
         "\\s*[(]up\\s+to\\s+3[,.]500\\s+characters?[)]\\s*"),
  ignore_case = TRUE
)

duties_tbl <- txt_full |>
  filter(addmcSectionNumber == "A.8a") |>
  mutate(text_clean = str_remove(addmcSectionDetails, addc_prefix) |> str_squish(),
         is_see     = str_detect(tolower(text_clean), "^see addendum c")) |>
  group_by(caseNumber) |>
  summarise(
    duties_pdf = if_else(
      any(!is_see & nchar(text_clean) > 5),
      paste(text_clean[!is_see & nchar(text_clean) > 5], collapse = " "),
      paste(text_clean[nchar(text_clean) > 5], collapse = " ")
    ) |> str_squish(), .groups = "drop"
  ) |> mutate(duties_pdf = na_if(duties_pdf, ""))

df_all <- enr |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf)) |>
  filter(!is.na(empFein), !is.na(dateSubmitted),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30"))

n_total   <- nrow(df_all)
n_usable  <- sum(!is.na(df_all$duties) & nchar(trimws(df_all$duties)) > 20)
n_missing <- sum(is.na(df_all$duties))
n_short   <- sum(!is.na(df_all$duties) & nchar(trimws(df_all$duties)) <= 20)

cat("=== OVERALL DROPOUT AUDIT ===\n")
cat(sprintf("Total JOs (valid dates + empFein): %d\n", n_total))
cat(sprintf("  Usable duties (> 20 chars):      %d  (%.1f%%)\n", n_usable, n_usable/n_total*100))
cat(sprintf("  Missing duties entirely:          %d  (%.1f%%)\n", n_missing, n_missing/n_total*100))
cat(sprintf("  Duties present but too short:     %d  (%.1f%%)\n", n_short, n_short/n_total*100))
cat(sprintf("  Total dropout:                    %d  (%.1f%%)\n\n", n_total - n_usable, (n_total-n_usable)/n_total*100))

cat("=== DUTIES SOURCE ===\n")
df_all |>
  mutate(source = case_when(
    !is.na(jobDuties) & nchar(trimws(jobDuties)) > 20 ~ "jobDuties (JSON/disclosure)",
    is.na(jobDuties)  & !is.na(duties_pdf) & nchar(trimws(duties_pdf)) > 20 ~ "PDF gap fill only",
    TRUE ~ "None / too short"
  )) |>
  count(source, sort = TRUE) |>
  mutate(pct = round(n / sum(n) * 100, 1)) |>
  print()

cat("\n=== DROPOUT BY YEAR ===\n")
df_all |>
  mutate(year = year(dateSubmitted),
         usable = !is.na(duties) & nchar(trimws(duties)) > 20) |>
  group_by(year) |>
  summarise(n_total     = n(),
            n_usable    = sum(usable),
            n_dropout   = n() - sum(usable),
            pct_dropout = round((1 - mean(usable)) * 100, 1),
            .groups = "drop") |>
  print()

cat("\n=== CROP-LEVEL DROPOUT (12 focal crops) ===\n")
crops <- tribble(
  ~crop,        ~pattern,
  "Strawberry", "strawberr",
  "Citrus",     "citrus|orange|grapefruit|tangerine|lemon|lime|mandarin|clementine",
  "Blueberry",  "blueberr",
  "Tobacco",    "tobacco",
  "Tomato",     "tomato",
  "Lettuce",    "lettuce|romaine",
  "Watermelon", "watermelon",
  "Apple",      "apple",
  "Grape",      "grape|vineyard|raisin",
  "Cherry",     "cherr",
  "Brassica",   "broccoli|cauliflower|cabbage|kale|kohlrabi",
  "Cucumber",   "cucumber|squash|zucchini"
)

# Build farmer_crop lookup: caseNumber -> crops it matches
ca_lower <- tolower(coalesce(df_crops$cropActivity, ""))

results <- lapply(seq_len(nrow(crops)), function(i) {
  patt <- crops$pattern[i]
  # Cases where farmer listed this crop
  farmer_cases <- df_crops$caseNumber[str_detect(ca_lower, patt)]
  # All JOs matching crop (farmer OR duties)
  sub <- df_all |>
    filter(caseNumber %in% farmer_cases |
           str_detect(tolower(coalesce(duties, "")), patt))
  tibble(
    crop        = crops$crop[i],
    n_matched   = nrow(sub),
    n_usable    = sum(!is.na(sub$duties) & nchar(trimws(sub$duties)) > 20),
    n_dropout   = nrow(sub) - sum(!is.na(sub$duties) & nchar(trimws(sub$duties)) > 20),
    pct_dropout = round((nrow(sub) - sum(!is.na(sub$duties) & nchar(trimws(sub$duties)) > 20)) / nrow(sub) * 100, 1)
  )
}) |> bind_rows() |> arrange(desc(n_matched))

print(results, n = 12)

cat("\n=== MECHANIZATION RATE AMONG DROPOUT JOs (spot check: do dropped JOs skew?) ===\n")
# For the full corpus: among dropped JOs, do jobDuties say anything about mechanization?
# Check if jobDuties (before PDF supplement) hints at automation for dropped ones
auto_pat <- paste0("machine.*harvest|harvest.*machine|mechanical.*harvest|",
                   "harvest.*mechanical|self.propelled|\\bconveyor\\b")
df_all |>
  mutate(usable   = !is.na(duties) & nchar(trimws(duties)) > 20,
         has_auto = str_detect(tolower(coalesce(duties, jobDuties, "")), auto_pat)) |>
  group_by(usable) |>
  summarise(n = n(), n_auto = sum(has_auto, na.rm=TRUE),
            pct_auto = round(mean(has_auto, na.rm=TRUE)*100, 1), .groups="drop") |>
  mutate(usable = if_else(usable, "In analysis", "Dropped (no/short duties)")) |>
  print()
