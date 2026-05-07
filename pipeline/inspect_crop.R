# Show which task pairs (and sentences) drive a crop's exposure score
# under a given patent filter. Prints top contributing pairs + example sentences.
#
# Usage:
#   Rscript 45_inspect_crop_contributions.R "<crop>" "<filter_expr_text>" [top_N]
# Examples:
#   Rscript 45_inspect_crop_contributions.R "strawberry" "TRUE" 20
#   Rscript 45_inspect_crop_contributions.R "strawberry" "grepl('A01D', cpc_list)" 30
#   Rscript 45_inspect_crop_contributions.R "apple" "grepl('drone', tolower(patent_abstract))"

library(data.table)
library(arrow)

ROOT <- "output"

# Surface (verb, noun) match only.
KEY <- c("verb","noun")

META  <- as.data.table(read_parquet(file.path(ROOT, "filtered/patent_metadata.parquet")))
LK    <- fread(file.path(ROOT, "pairs/task_lookup.tsv"))
PAIRS <- as.data.table(read_parquet(file.path(ROOT, "filtered/patent_pairs.parquet")))
TP    <- as.data.table(read_parquet(file.path(ROOT, "pairs/task_pairs.parquet")))

# Sentences for extraction; join by (caseNumber, position) since
# sentences_for_extraction uses integer sid != LK's padded sent_uid.
SENTS_RDS <- "output/text/core/sentences.parquet"
sent_lookup <- if (file.exists(SENTS_RDS)) {
  s <- as.data.table(arrow::read_parquet(SENTS_RDS))[, .(caseNumber, sentence)]
  s[, idx := rowid(caseNumber)]
  lk <- copy(LK[, .(caseNumber, sent_uid)])
  lk[, idx := rowid(caseNumber)]
  merge(lk, s, by = c("caseNumber","idx"))[, .(sent_uid, sentence)]
} else NULL

inspect <- function(crop, filter_expr_text = "TRUE", top_n = 20) {
  cat(sprintf("Crop: %s   Filter: %s   Top N: %d\n", crop, filter_expr_text, top_n))

  # 1. Patents kept by filter -> pair freq
  expr <- parse(text = filter_expr_text)
  keep_ids <- META[eval(expr), patent_id]
  pp <- PAIRS[doc_id %in% keep_ids]
  total_pat <- nrow(pp)
  pf <- pp[, .(pat_count = .N), by = c(KEY)]
  pf[, pat_share := pat_count / total_pat]
  cat(sprintf("  patents: %d   patent-pairs: %d   unique pairs: %d\n",
              length(keep_ids), total_pat, nrow(pf)))

  # 2. Sentences for this crop
  crop_sents <- LK[grepl(paste0("(^|;)", crop, "(;|$)"), crops), sent_uid]
  cat(sprintf("  crop sentences: %d\n", length(crop_sents)))

  # 3. Task pairs from those sentences, scored by pair-level pat_share
  tp_c_cols <- c("doc_id","verb","noun", KEY)
  tp_c_cols <- intersect(tp_c_cols, names(TP))
  tp_c <- TP[doc_id %in% crop_sents, ..tp_c_cols]
  setnames(tp_c, "doc_id", "sent_uid")
  tp_c <- merge(tp_c, pf[, c(KEY, "pat_share"), with = FALSE],
                by = KEY, all.x = TRUE)
  tp_c[is.na(pat_share), pat_share := 0]

  # 4. Aggregate by match-level keys + show example surface form
  by_cols <- KEY
  contrib <- tp_c[pat_share > 0, .(
              n_sent_with_pair = uniqueN(sent_uid),
              total_contrib    = sum(pat_share),
              avg_pair_share   = mean(pat_share),
              ex_verb          = verb[1],
              ex_noun          = noun[1]),
              by = by_cols]
  setorder(contrib, -total_contrib)
  cat(sprintf("\nTop %d pairs by total contribution to %s's score:\n",
              top_n, crop))
  out <- copy(contrib[1:min(top_n, nrow(contrib))])
  out[, share   := signif(avg_pair_share, 3)]
  out[, contrib := signif(total_contrib, 3)]
  setnames(out, "n_sent_with_pair", "n_sent")
  print(out[, c(KEY, "ex_verb","ex_noun","n_sent","share","contrib"),
            with = FALSE], nrows = 50)

  # 5. Example sentences for top 5 pairs
  if (!is.null(sent_lookup)) {
    cat("\nExample sentences for top 5 contributing pairs:\n")
    for (i in 1:min(5, nrow(contrib))) {
      k1 <- contrib[[KEY[1]]][i]; k2 <- contrib[[KEY[2]]][i]
      ex_sids <- tp_c[get(KEY[1]) == k1 & get(KEY[2]) == k2, sent_uid][1:3]
      cat(sprintf("\n  [%d] %s=%s, %s=%s (ex: %s/%s)  contrib=%s\n",
                  i, KEY[1], k1, KEY[2], k2,
                  contrib$ex_verb[i], contrib$ex_noun[i],
                  signif(contrib$total_contrib[i], 3)))
      ex_text <- sent_lookup[sent_uid %in% ex_sids, sentence]
      for (s in ex_text) cat("    -", substr(s, 1, 180), "\n")
    }
  }
  invisible(contrib)
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1) {
  crop <- args[1]
  filter_expr <- if (length(args) >= 2) args[2] else "TRUE"
  topN <- if (length(args) >= 3) as.integer(args[3]) else 20L
  inspect(crop, filter_expr, topN)
}
