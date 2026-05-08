###############################################################################
# Pipeline Step 1d — build crops_activities long-format table.
#
# Source 1: h2a_labor_demand/processed/text/disclosure_crops_activities.csv
#           (DOL Addendum A FY2020+ + FY2019 PRIMARY_CROP from disclosure)
# Source 2 (fallback): jo_full.parquet rows whose caseNumber doesn't appear in
#           Source 1. Synthesizes ONE row per missing JO from main-table
#           wage fields (jobWageOffer, jobWagePer); activity = NA.
#
# Output: h2a_labor_demand/processed/text/crops_activities_long.parquet
###############################################################################

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(readr)
})

DATA  <- "output/text"
INPUT <- file.path(DATA, "analysis")
SRC   <- file.path(INPUT, "disclosure_crops_activities.csv")
OUT   <- file.path(DATA, "core", "crops_activities_long.parquet")

cat("Reading disclosure ...\n")
disc <- read_csv(SRC, show_col_types = FALSE,
                 col_types = cols(addmaWageOffer = col_double(),
                                  entry_idx = col_integer(),
                                  .default = "c")) |>
  select(caseNumber, entry_idx, addmaCropId, addmaCropActivity,
         addmaWageOffer, addmaPayPer, addmaAdditionalInfo, source)
cat("Disclosure rows:", nrow(disc), " | unique cases:", n_distinct(disc$caseNumber), "\n")

cat("Reading JSON cropsAndActivities ...\n")
js <- read_csv(file.path(INPUT, "json_crops_activities.csv"), show_col_types = FALSE,
               col_types = cols(addmaWageOffer = col_double(),
                                entry_idx = col_integer(),
                                .default = "c")) |>
  arrange(caseNumber, entry_idx, desc(snap_date)) |>
  distinct(caseNumber, entry_idx, .keep_all = TRUE) |>
  mutate(source = "json") |>
  select(caseNumber, entry_idx, addmaCropId, addmaCropActivity,
         addmaWageOffer, addmaPayPer, addmaAdditionalInfo, source)
cat("JSON rows:", nrow(js), " | unique cases:", n_distinct(js$caseNumber), "\n")

# Combine: prefer disclosure rows; for cases NOT in disclosure, use JSON
disc_cases <- unique(disc$caseNumber)
js_only <- js |> filter(!caseNumber %in% disc_cases)
cat("JSON-only cases (not in disclosure):", n_distinct(js_only$caseNumber), "\n")

disc <- bind_rows(disc, js_only)
cat("Combined sources rows:", nrow(disc), " | unique cases:", n_distinct(disc$caseNumber), "\n")

# Fallback for JOs in jo_full but not in disclosure
jo <- arrow::read_parquet(file.path(DATA, "core", "jo_full.parquet"))
cat("\njo_full.parquet rows:", nrow(jo), "\n")
missing <- jo |>
  anti_join(disc |> distinct(caseNumber), by = "caseNumber") |>
  transmute(caseNumber, entry_idx = 0L,
            addmaCropId = NA_character_, addmaCropActivity = NA_character_,
            addmaWageOffer = suppressWarnings(as.numeric(jobWageOffer)),
            addmaPayPer = jobWagePer,
            addmaAdditionalInfo = NA_character_,
            source = "main_table_fallback")
cat("JOs needing main-table fallback:", nrow(missing), "\n")

out <- bind_rows(disc, missing) |> arrange(caseNumber, entry_idx)
arrow::write_parquet(out, OUT)

cat("\n=== SUMMARY ===\n")
cat("Total rows:", nrow(out), "\n")
cat("Unique caseNumbers:", n_distinct(out$caseNumber), "\n")
cat("By source:\n")
print(out |> count(source))

cat("\n--- Reach into jo_full ---\n")
n_jo <- nrow(jo)
n_disc <- jo |> semi_join(disc |> distinct(caseNumber), by = "caseNumber") |> nrow()
cat("JOs in jo_full:                          ", n_jo, "\n")
cat("JOs with disclosure crop entries:        ", n_disc,
    " (", round(100*n_disc/n_jo, 1), "%)\n", sep="")
cat("JOs covered (disclosure + fallback):     ", n_distinct(out$caseNumber),
    " (", round(100*n_distinct(out$caseNumber)/n_jo, 1), "%)\n", sep="")
cat("\nSaved:", OUT, "\n")
