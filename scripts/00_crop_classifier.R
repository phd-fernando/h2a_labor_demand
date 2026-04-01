###############################################################################
# 00_crop_classifier.R
# Primary crop assignment for H-2A job orders.
#
# Strategy (two-tier):
#   Tier 1 — cropActivity field (employer-declared, from df_crops.rds).
#             Most reliable. One caseNumber may have multiple cropActivity rows;
#             we take the activity with the highest repeat count per case.
#   Tier 2 — Within-document term frequency on jobDuties text.
#             For each JO missing a Tier 1 match, count occurrences of each
#             crop pattern (word-boundary regex) and assign to the crop with
#             the highest count. Ties → NA (unclassified).
#
# Output: crop_map  tibble(caseNumber, primary_crop, crop_source)
#   crop_source: "cropActivity" | "duties_tfidf" | NA
#
# Usage:
#   source("scripts/00_crop_classifier.R")
#   df <- df |> left_join(crop_map, by = "caseNumber")
###############################################################################

library(dplyr)
library(stringr)

# ==============================================================================
# 1. CROP PATTERN DICTIONARY  (word-boundary regex, lower-case targets)
# ==============================================================================
crop_patterns <- list(
  corn        = "\\bcorn\\b|\\bmaize\\b|\\bdetassel",
  tobacco     = "\\btobacco\\b",
  citrus      = "\\bcitrus\\b|\\borange\\b|\\bgrapefruit\\b|\\btangerine\\b|\\bclementine\\b|\\blemon\\b|\\blime\\b",
  apple       = "\\bapple\\b|\\borchard\\b",
  strawberry  = "\\bstrawberr",
  blueberry   = "\\bblueberr",
  tomato      = "\\btomato",
  watermelon  = "\\bwatermelon\\b|\\bmelon\\b",
  lettuce     = "\\blettuce\\b|\\bromaine\\b",
  grape       = "\\bgrape\\b|\\bvineyard\\b|\\braisin\\b",
  cherry      = "\\bcherr",
  peach       = "\\bpeach\\b|\\bnectarine\\b",
  hay         = "\\bhay\\b|\\balfalfa\\b|\\bforage\\b",
  cotton      = "\\bcotton\\b",
  soybean     = "\\bsoybean\\b|\\bsoybeans\\b",
  potato      = "\\bpotato\\b",
  sweet_potato= "\\bsweet potato\\b|\\bsweet potatoes\\b",
  rice        = "\\brice\\b",
  wheat       = "\\bwheat\\b",
  sugarcane   = "\\bsugarcane\\b|\\bsugar cane\\b",
  celery      = "\\bcelery\\b",
  broccoli    = "\\bbroccoli\\b|\\bcauliflower\\b|\\bcabbage\\b|\\bkale\\b",
  cucumber    = "\\bcucumber\\b|\\bsquash\\b|\\bzucchini\\b",
  pepper      = "\\bpepper\\b|\\bchile\\b|\\bjalape",
  nursery     = "\\bnursery\\b|\\bgreenhouse\\b",
  crawfish    = "\\bcrawfish\\b|\\bcrayfish\\b",
  sheep       = "\\bsheep\\b|\\blamb\\b|\\bewe\\b",
  cattle      = "\\bcattle\\b|\\bbovine\\b|\\bdairy\\b",
  mushroom    = "\\bmushroom\\b"
)

# ==============================================================================
# 2. TIER-1: cropActivity → canonical crop name
#    Map the most frequent cropActivity string per case to a crop_patterns key.
# ==============================================================================
map_activity_to_crop <- function(activity_str) {
  a <- tolower(trimws(activity_str))
  case_when(
    str_detect(a, "\\bcorn\\b|\\bmaize\\b|\\bdetassel")                      ~ "corn",
    str_detect(a, "\\btobacco\\b")                                            ~ "tobacco",
    str_detect(a, "\\bcitrus\\b|\\borange\\b|\\bgrapefruit\\b|\\btangerine\\b|\\bclementine\\b|\\blemon\\b|\\blime\\b") ~ "citrus",
    str_detect(a, "\\bapple\\b|\\borchard\\b")                               ~ "apple",
    str_detect(a, "\\bstrawberr")                                             ~ "strawberry",
    str_detect(a, "\\bblueberr")                                              ~ "blueberry",
    str_detect(a, "\\btomato")                                                ~ "tomato",
    str_detect(a, "\\bwatermelon\\b|\\bmelon\\b")                            ~ "watermelon",
    str_detect(a, "\\blettuce\\b|\\bromaine\\b|\\bbutterleaf\\b|\\bbutter leaf\\b|\\bred leaf\\b|\\bgreen leaf\\b|\\biceberg\\b") ~ "lettuce",
    str_detect(a, "\\bgrape\\b|\\bvineyard\\b|\\braisin\\b")                 ~ "grape",
    str_detect(a, "\\bcherr")                                                 ~ "cherry",
    str_detect(a, "\\bpeach\\b|\\bnectarine\\b")                             ~ "peach",
    str_detect(a, "\\bhay\\b|\\balfalfa\\b|\\bforage\\b")                    ~ "hay",
    str_detect(a, "\\bcotton\\b")                                             ~ "cotton",
    str_detect(a, "\\bsoybean\\b|\\bsoybeans\\b")                            ~ "soybean",
    str_detect(a, "\\bsweet potato\\b|\\bsweet potatoes\\b")                 ~ "sweet_potato",
    str_detect(a, "\\bpotato\\b")                                             ~ "potato",
    str_detect(a, "\\brice\\b")                                               ~ "rice",
    str_detect(a, "\\bwheat\\b")                                              ~ "wheat",
    str_detect(a, "\\bsugarcane\\b|\\bsugar cane\\b")                        ~ "sugarcane",
    str_detect(a, "\\bcelery\\b")                                             ~ "celery",
    str_detect(a, "\\bbroccoli\\b|\\bcauliflower\\b|\\bcabbage\\b|\\bkale\\b") ~ "broccoli",
    str_detect(a, "\\bcucumber\\b|\\bsquash\\b|\\bzucchini\\b")              ~ "cucumber",
    str_detect(a, "\\bpepper\\b|\\bchile\\b|\\bjalape")                      ~ "pepper",
    str_detect(a, "\\bnursery\\b|\\bgreenhouse\\b")                          ~ "nursery",
    str_detect(a, "\\bcrawfish\\b|\\bcrayfish\\b")                           ~ "crawfish",
    str_detect(a, "\\bsheep\\b|\\blamb\\b|\\bewe\\b")                        ~ "sheep",
    str_detect(a, "\\bcattle\\b|\\bbovine\\b|\\bdairy\\b")                   ~ "cattle",
    str_detect(a, "\\bmushroom\\b")                                           ~ "mushroom",
    TRUE                                                                       ~ NA_character_
  )
}

# ==============================================================================
# 3. BUILD CROP MAP
# ==============================================================================
build_crop_map <- function(df_crops_rds_path, duties_tbl) {
  # --- Tier 1: from cropActivity ---
  df_crops_raw <- readRDS(df_crops_rds_path)

  tier1 <- df_crops_raw |>
    filter(!is.na(cropActivity)) |>
    mutate(crop_key = map_activity_to_crop(cropActivity)) |>
    filter(!is.na(crop_key)) |>
    # per case: most frequent crop_key wins
    count(caseNumber, crop_key, sort = TRUE) |>
    group_by(caseNumber) |>
    slice_max(n, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(caseNumber, primary_crop = crop_key) |>
    mutate(crop_source = "cropActivity")

  cat(sprintf("Tier 1 (cropActivity): %s unique cases assigned\n",
              format(nrow(tier1), big.mark = ",")))

  # --- Tier 2: within-doc term frequency on duties text ---
  tier2_candidates <- duties_tbl |>
    filter(!caseNumber %in% tier1$caseNumber,
           !is.na(duties), nchar(trimws(duties)) > 20)

  if (nrow(tier2_candidates) > 0) {
    count_mat <- sapply(crop_patterns, function(p)
      str_count(tolower(tier2_candidates$duties), p))

    if (is.vector(count_mat)) count_mat <- matrix(count_mat, nrow = 1,
                                                   dimnames = list(NULL, names(crop_patterns)))

    row_max <- apply(count_mat, 1, max)
    primary <- apply(count_mat, 1, function(row) {
      if (max(row) == 0) return(NA_character_)
      winners <- names(row)[row == max(row)]
      if (length(winners) == 1) winners else NA_character_  # ties → unclassified
    })

    tier2 <- tier2_candidates |>
      select(caseNumber) |>
      mutate(primary_crop = primary,
             crop_source  = if_else(!is.na(primary), "duties_freq", NA_character_)) |>
      filter(!is.na(primary_crop))

    cat(sprintf("Tier 2 (duties freq):  %s unique cases assigned\n",
                format(nrow(tier2), big.mark = ",")))
  } else {
    tier2 <- tibble(caseNumber=character(), primary_crop=character(), crop_source=character())
  }

  crop_map <- bind_rows(tier1, tier2)
  cat(sprintf("Total assigned:        %s\n\n", format(nrow(crop_map), big.mark = ",")))

  cat("Crop distribution:\n")
  crop_map |> count(primary_crop, sort = TRUE) |> print(n = 30)

  crop_map
}
