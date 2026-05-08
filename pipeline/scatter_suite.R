###############################################################################
# scatter_suite.R — produce the locked-in suite of scatter plots.
#
# Locked filters:
#   * A01 patents only (already true via patent_metadata.parquet upstream)
#   * Patent year 2018-2023
#   * Citation-weighted (1 + log(1 + fwd_cites_2yr))
#   * Single-crop JOs
#   * JOs FY2020-2025
#   * group_by = "crop"
#
# For each AI domain, every patent's pair contribution is multiplied by its
# ai_score_<domain> (continuous, in [0,1]). The "baseline" run multiplies by
# 1.0 (i.e., citation-weighted only, no AI overlay).
#
# Generates 9 scatters (full FY) + 9 scatters (Q1+Q2 only) = 18 PNGs.
#
# Run: Rscript pipeline/scatter_suite.R
###############################################################################

suppressPackageStartupMessages({
  library(arrow); library(data.table)
})

MAKE_SCATTER_NO_DISPATCH <- TRUE
source("pipeline/make_scatter.R")  # loads META, PAIRS, TP, LK, FWD; defines filter_and_plot

AI_CACHE <- "output/cache/ai_predictions_full.parquet"
ai <- as.data.table(read_parquet(AI_CACHE))
ai[, doc_id := as.character(doc_id)]

DOMAINS <- c("ml","evo","nlp","speech","vision","planning","kr","hardware")
SCORE_COLS <- paste0("ai_score_", DOMAINS)

PATENT_FILTER <- "year >= 2018 & year <= 2023"

run_one <- function(domain, q1q2_only) {
  if (domain == "baseline") {
    lookup <- NULL
    tag <- "baseline"
  } else {
    score_col <- paste0("ai_score_", domain)
    lookup <- ai[, c("doc_id", score_col), with = FALSE]
    setnames(lookup, c("doc_id", score_col), c("patent_id", "ai_score"))
    tag <- paste0("ai_", domain)
  }
  q_tag <- if (q1q2_only) "q1q2" else "fullfy"
  out_name <- sprintf("suite_%s_%s.png", tag, q_tag)
  cat(sprintf("\n=== %s | %s ===\n", tag, q_tag))
  filter_and_plot(
    filter_expr_text = PATENT_FILTER,
    out_name        = out_name,
    label           = sprintf("A01 2018-2023, cite-wgt, %s", tag),
    single_crop     = TRUE,
    jo_year_min     = 2020L,
    jo_year_max     = 2025L,
    weight_cites    = TRUE,
    q1q2_only       = q1q2_only,
    group_by        = "crop",
    ai_score_lookup = lookup)
}

t0 <- Sys.time()
for (q1q2 in c(FALSE, TRUE)) {
  for (d in c("baseline", DOMAINS)) {
    invisible(run_one(d, q1q2))
  }
}
cat(sprintf("\nSuite done in %.1f min. 18 scatters in output/results/suite_*.png\n",
            as.numeric(Sys.time() - t0, units = "mins")))
