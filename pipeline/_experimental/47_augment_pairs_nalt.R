# Augment task_pairs and master_a01_pairs with NALT concept columns.
# Lookup: stem(surface_word) -> NALT concept_id  (and pref_label).
# Saves augmented parquet files for fast re-reads.

library(data.table)
library(arrow)
library(SnowballC)

ROOT <- "h2a_labor_demand/webb_replication"
NALT <- as.data.table(readRDS(file.path(ROOT, "data/cache/nalt_lookup_stemmed.rds")))
setnames(NALT, c("stem","concept_id","pref_label","label_lc"),
              c("stem","nalt_id","nalt_label","nalt_surface"))

augment_pairs <- function(in_path, out_path, has_concept_cols = TRUE) {
  cat("Augmenting:", in_path, "\n")
  pp <- if (grepl("\\.parquet$", in_path))
    as.data.table(read_parquet(in_path)) else fread(in_path)

  pp[, verb_stem := wordStem(verb, "en")]
  pp[, noun_stem := wordStem(noun, "en")]
  v <- merge(pp[, .(verb_stem)], NALT, by.x = "verb_stem", by.y = "stem", all.x = TRUE)
  setnames(v, c("nalt_id","nalt_label"), c("verb_nalt","verb_nalt_label"))
  pp[, verb_nalt       := v$verb_nalt]
  pp[, verb_nalt_label := v$verb_nalt_label]

  n <- merge(pp[, .(noun_stem)], NALT, by.x = "noun_stem", by.y = "stem", all.x = TRUE)
  setnames(n, c("nalt_id","nalt_label"), c("noun_nalt","noun_nalt_label"))
  pp[, noun_nalt       := n$noun_nalt]
  pp[, noun_nalt_label := n$noun_nalt_label]

  cat(sprintf("  rows: %d  verb_nalt non-NA: %d (%.0f%%)  noun_nalt non-NA: %d (%.0f%%)\n",
              nrow(pp),
              sum(!is.na(pp$verb_nalt)), 100*mean(!is.na(pp$verb_nalt)),
              sum(!is.na(pp$noun_nalt)), 100*mean(!is.na(pp$noun_nalt))))
  cat(sprintf("  both NALT non-NA: %d (%.0f%%)\n",
              sum(!is.na(pp$verb_nalt) & !is.na(pp$noun_nalt)),
              100*mean(!is.na(pp$verb_nalt) & !is.na(pp$noun_nalt))))

  pp[, c("verb_stem","noun_stem") := NULL]
  write_parquet(pp, out_path)
  cat("  saved:", out_path, "\n\n")
}

# 1. Master patent pairs
augment_pairs(file.path(ROOT, "data/filtered/master_a01_pairs.parquet"),
              file.path(ROOT, "data/filtered/master_a01_pairs_nalt.parquet"))

# 2. Task pairs (H-2A side)
augment_pairs(file.path(ROOT, "data/pairs/task_pairs_v4_spacy_wn.tsv"),
              file.path(ROOT, "data/filtered/task_pairs_v4_nalt.parquet"))
