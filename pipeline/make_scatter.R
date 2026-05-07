# Fast filter-and-plot: any subset of master A01 patents -> F2-style scatter.
# Re-runs in seconds (no spaCy, no patent re-pull).
#
# Usage from command line:
#   Rscript 44_filter_and_plot.R "<filter_expr_text>" [<output_name>]
#
# filter_expr_text is an R expression on metadata columns:
#   patent_id, year, patent_title, patent_abstract, cpc_list, n_cpc
# Examples:
#   "TRUE"                                    -> all A01 patents
#   "grepl('A01D', cpc_list)"                 -> harvesters
#   "year >= 2015"                            -> recent only
#   "grepl('drone', tolower(patent_abstract))"-> drone in abstract
#   "year %in% 2020:2025 & grepl('A01D', cpc_list)"
#
# Or call filter_and_plot() interactively.

library(data.table)
library(ggplot2)
library(ggrepel)
library(ggforce)
library(scales)
library(cluster)
library(arrow)

ROOT <- "output"
OUT  <- file.path(ROOT, "results")

# ---- Load master inputs ONCE ---------------------------------------
# Surface (verb, noun) match. IDF is pre-computed in extract_task_pairs.R
# and lives in task_pairs.parquet.
KEY_COLS <- c("verb","noun")

PIPE <- "pipeline"
PATH_PAT_PAIRS <- file.path(ROOT, "filtered/patent_pairs.parquet")
PATH_TASK_PAIRS <- file.path(ROOT, "pairs/task_pairs.parquet")

# Bootstrap: rebuild missing dependencies.
if (!file.exists(PATH_PAT_PAIRS))  source(file.path(PIPE, "extract_patent_pairs.R"))
if (!file.exists(PATH_TASK_PAIRS)) source(file.path(PIPE, "extract_task_pairs.R"))

META  <- as.data.table(read_parquet(file.path(ROOT, "filtered/patent_metadata.parquet")))
PAIRS <- as.data.table(read_parquet(PATH_PAT_PAIRS))
TP    <- as.data.table(read_parquet(PATH_TASK_PAIRS))
LK    <- fread(file.path(ROOT, "pairs/task_lookup.tsv"))
setnames(PAIRS, "doc_id", "patent_id")
PAIRS[, patent_id := as.character(patent_id)]

# FTE side computed once (FY2020-2025, multi-crop split, Farmworker-Crop)
fte_summ <- {
  jof   <- as.data.table(arrow::read_parquet("output/text/core/jo_full.parquet"))
  crops <- as.data.table(arrow::read_parquet("output/text/core/jo_crops.parquet"))
  jof   <- jof[fiscalYear >= 2020 & fiscalYear <= 2025]
  jof[, contract_days := as.integer(jobEndDate - jobBeginDate)]
  jof[, hrs_per_wk    := suppressWarnings(as.numeric(jobHoursTotal))]
  jof[, fte_per_jo    := wrksCertified * hrs_per_wk * (contract_days / 7) / 2080]
  jof_soc <- jof[grepl("^45-2092", socCode)]
  npj <- crops[, .(n_crops_jo = uniqueN(crop_canonical)), by = caseNumber]
  jc <- merge(jof_soc[, .(caseNumber, fte_per_jo)],
              crops[, .(caseNumber, crop_canonical)],
              by = "caseNumber", allow.cartesian = TRUE)
  jc <- merge(jc, npj, by = "caseNumber")
  jc[, fte_share := fte_per_jo / n_crops_jo]
  s <- jc[, .(fte_total = sum(fte_share, na.rm = TRUE),
              n_jos_fte = uniqueN(caseNumber)),
          by = crop_canonical]
  non_crops <- c("date","prune","cattle","sheep","hog","chicken","turkey","goat",
                 "dairy","duck","poultry","aquaculture","apicultura")
  s <- s[!crop_canonical %in% non_crops & fte_total > 0]
  setorder(s, fte_total)
  s[, fte_pct := 100 * cumsum(n_jos_fte) / sum(n_jos_fte)]
  s
}
cat(sprintf("Master loaded: %d patents, %d pairs, %d crops in FTE side.\n",
            nrow(META), nrow(PAIRS), nrow(fte_summ)))

# ---- Core function -------------------------------------------------
filter_and_plot <- function(filter_expr_text = "TRUE",
                            out_name = NULL,
                            label = filter_expr_text) {
  t0 <- Sys.time()
  expr <- parse(text = filter_expr_text)
  keep_ids <- META[eval(expr), patent_id]
  cat(sprintf("Filter '%s' kept %d / %d patents.\n",
              filter_expr_text, length(keep_ids), nrow(META)))
  if (length(keep_ids) < 100) {
    warning("Fewer than 100 patents kept; scores may be noisy.")
  }

  # Patent pair frequency at the chosen match level
  pp <- PAIRS[patent_id %in% keep_ids]
  total_pat <- nrow(pp)
  pf <- pp[, .(pat_count = .N), by = c(KEY_COLS)]
  pf[, pat_share := pat_count / total_pat]

  # Score task pairs -> sentence -> JO x crop -> crop.
  # Pair-level score = pat_share * idf  (IDF suppresses boilerplate).
  scored <- merge(TP, pf[, c(KEY_COLS, "pat_share"), with = FALSE],
                  by = KEY_COLS, all.x = TRUE)
  scored[is.na(pat_share), pat_share := 0]
  scored[, weighted := pat_share * idf]
  ss <- scored[, .(sent_score = mean(weighted)), by = .(sent_uid = doc_id)]
  ss <- merge(ss, LK[, .(sent_uid, caseNumber, crops, year)], by = "sent_uid")
  long <- ss[!is.na(crops) & crops != "" & year >= 2020 & year <= 2025,
              .(crop_canonical = unlist(strsplit(crops, ";"))),
              by = .(sent_uid, caseNumber, sent_score)]
  jo_sc <- long[, .(jo_score = mean(sent_score)), by = .(caseNumber, crop_canonical)]
  cs <- jo_sc[, .(n_jos = uniqueN(caseNumber),
                   mean_score = mean(jo_score)), by = crop_canonical]
  setorder(cs, mean_score)
  cs[, exp_pct := 100 * cumsum(n_jos) / sum(n_jos)]

  dt <- merge(fte_summ, cs[, .(crop_canonical, exp_pct, n_jos)],
              by = "crop_canonical")
  dt[, log_fte := log10(fte_total)]
  target <- c("apple","strawberry","wheat")
  dt[, target := crop_canonical %in% target]

  # k-means hulls (k=5) on (exp_pct, log_fte)
  X <- as.matrix(dt[, .(exp_pct, log_fte)])
  set.seed(7)
  km <- kmeans(X, centers = min(5, nrow(dt) - 1), nstart = 25)
  dt[, cluster := factor(km$cluster)]

  fig <- ggplot(dt, aes(x = exp_pct, y = fte_total)) +
    geom_mark_hull(aes(group = cluster, fill = cluster),
                   expand = unit(3, "mm"),
                   alpha = 0.10, colour = NA, concavity = 5) +
    geom_point(aes(colour = target), size = 2.2, alpha = .8) +
    geom_text_repel(aes(label = crop_canonical, colour = target),
                    size = 2.7, max.overlaps = Inf,
                    segment.size = 0.2, segment.alpha = 0.4,
                    min.segment.length = 0.1, force = 1.2,
                    fontface = "plain") +
    scale_color_manual(values = c("FALSE"="grey45","TRUE"="firebrick"),
                       guide = "none") +
    scale_fill_brewer(palette = "Set2", guide = "none") +
    scale_y_log10(labels = label_comma()) +
    labs(title    = sprintf("Crop's H-2A FTE workers vs mechanization-patent exposure"),
         subtitle = sprintf("Filter: %s   |   %d patents, %d crops, %d total pat-pairs",
                             label, length(keep_ids), nrow(dt), total_pat),
         x = "Exposure-to-mechanization-patents percentile",
         y = "H-2A FTE workers (log scale)") +
    theme_minimal(base_size = 11)

  if (is.null(out_name)) {
    safe <- gsub("[^A-Za-z0-9_]+", "_", filter_expr_text)
    safe <- substr(safe, 1, 60)
    out_name <- paste0("scatter_", safe, ".png")
  }
  out_path <- file.path(OUT, out_name)
  ggsave(out_path, fig, width = 10, height = 6.5, dpi = 150)
  cat(sprintf("Saved: %s   (%.1fs)\n", out_path,
              as.numeric(Sys.time() - t0, units = "secs")))
  invisible(dt)
}

# ---- CLI dispatch --------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1) {
  filter_and_plot(filter_expr_text = args[1],
                  out_name = if (length(args) >= 2) args[2] else NULL)
}
