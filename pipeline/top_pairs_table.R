# Top-N (verb, noun) pairs per crop under a given patent filter.
# Wide TSV: one row per crop, columns top1..topN with contribution.
#
# Usage:
#   Rscript 48_top_pairs_per_crop.R "<filter_expr>" "<out_tsv>" [top_n]
# Examples:
#   Rscript 48_top_pairs_per_crop.R "TRUE" "top_pairs_all_a01.tsv" 5
#   Rscript 48_top_pairs_per_crop.R "grepl('A01D', cpc_list)" "top_pairs_harvesters.tsv" 5

library(data.table)
library(arrow)

ROOT <- "output"
OUT  <- file.path(ROOT, "results")

# Surface (verb, noun) match only.
KEY <- c("verb","noun")

META  <- as.data.table(read_parquet(file.path(ROOT, "filtered/patent_metadata.parquet")))
LK    <- fread(file.path(ROOT, "pairs/task_lookup.tsv"))
PAIRS <- as.data.table(read_parquet(file.path(ROOT, "filtered/patent_pairs.parquet")))
TP    <- as.data.table(read_parquet(file.path(ROOT, "pairs/task_pairs.parquet")))
setnames(PAIRS, "doc_id", "patent_id"); PAIRS[, patent_id := as.character(patent_id)]
# IDF is pre-computed in extract_task_pairs.R; column 'idf' is in TP.

# FTE side (FY2020-2025, multi-crop split, Farmworker-Crop) -- matches script 44
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

build_table <- function(filter_expr_text = "TRUE",
                         out_tsv = file.path(OUT, "top_pairs.tsv"),
                         top_n = 5L) {
  t0 <- Sys.time()
  expr <- parse(text = filter_expr_text)
  keep_ids <- META[eval(expr), patent_id]

  # Patent pair freq + score task pairs
  pp <- PAIRS[patent_id %in% keep_ids]
  total_pat <- nrow(pp)
  pf <- pp[, .(pat_count = .N), by = c(KEY)]
  pf[, pat_share := pat_count / total_pat]
  scored <- merge(TP, pf[, c(KEY, "pat_share"), with = FALSE],
                  by = KEY, all.x = TRUE)
  scored[is.na(pat_share), pat_share := 0]
  scored[, weighted := pat_share * idf]

  # Sentence -> crop expansion (one row per (sent, crop))
  ss <- scored[, .(doc_id, verb, noun, pat_share = weighted)]
  setnames(ss, "doc_id", "sent_uid")
  ss <- merge(ss, LK[, .(sent_uid, crops, year)], by = "sent_uid")
  long <- ss[!is.na(crops) & crops != "" & year >= 2020 & year <= 2025,
              .(crop_canonical = unlist(strsplit(crops, ";"))),
              by = .(sent_uid, verb, noun, pat_share)]

  # Per-crop crop-level exposure (for sorting rows in the wide output)
  jo_meta <- merge(LK[, .(sent_uid, caseNumber)],
                   ss[, .(sent_uid, sent_score = pat_share)],
                   by = "sent_uid", all = FALSE)
  # we re-aggregate score per crop to sort:
  per_pair <- long[pat_share > 0,
                   .(contrib  = sum(pat_share),
                     n_sent   = uniqueN(sent_uid)),
                   by = .(crop_canonical, verb, noun)]
  setorder(per_pair, crop_canonical, -contrib)
  per_pair[, rk := seq_len(.N), by = crop_canonical]
  topn <- per_pair[rk <= top_n]

  # crop-level summaries (mean_score per crop = mean of pair contribs over sent×crop)
  crop_sc <- long[, .(mean_score = mean(pat_share),
                       n_jos = uniqueN(sent_uid)),
                   by = crop_canonical]
  setorder(crop_sc, mean_score)
  crop_sc[, exp_pct := 100 * cumsum(n_jos) / sum(n_jos)]
  crop_sc <- merge(crop_sc, fte_summ[, .(crop_canonical, fte_total, fte_pct)],
                    by = "crop_canonical")

  # Pivot wide: one row per crop, top1..topN columns
  topn[, pair_str := sprintf("%s+%s", verb, noun)]
  topn[, contrib  := round(contrib, 3)]
  topn_wide <- dcast(topn, crop_canonical ~ rk,
                      value.var = c("pair_str", "contrib"))

  # Reorder cols so it reads top1_pair, top1_contrib, top2_pair, top2_contrib, ...
  rk_seq <- 1:top_n
  pair_cols <- paste0("pair_str_", rk_seq)
  ctr_cols  <- paste0("contrib_", rk_seq)
  # Some crops may have <top_n contributing pairs; fill missing cols
  for (c in c(pair_cols, ctr_cols)) {
    if (!c %in% names(topn_wide)) topn_wide[, (c) := NA]
  }
  ordered_cols <- c(rbind(pair_cols, ctr_cols))

  out <- merge(crop_sc[, .(crop_canonical,
                            exp_pct = round(exp_pct, 1),
                            fte_total = round(fte_total, 0),
                            fte_pct = round(fte_pct, 1))],
               topn_wide[, c("crop_canonical", ordered_cols), with = FALSE],
               by = "crop_canonical")
  setorder(out, -exp_pct)

  fwrite(out, out_tsv, sep = "\t")
  cat(sprintf("Filter: %s   patents kept: %d   crops: %d\n",
              filter_expr_text, length(keep_ids), nrow(out)))
  cat(sprintf("Saved: %s   (%.1fs)\n", out_tsv,
              as.numeric(Sys.time() - t0, units = "secs")))
  invisible(out)
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1) {
  build_table(filter_expr_text = args[1],
              out_tsv = if (length(args) >= 2) args[2]
                       else file.path(OUT, "top_pairs.tsv"),
              top_n   = if (length(args) >= 3) as.integer(args[3]) else 5L)
}
