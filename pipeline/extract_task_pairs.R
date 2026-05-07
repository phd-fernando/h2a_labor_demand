# Build the H-2A task-side files from sentences.parquet:
#   data/pairs/task_lookup.tsv     (sent_uid + metadata)
#   data/pairs/task_pairs.tsv      (sent_uid, verb, noun)         <- Python output
#   data/pairs/task_pairs.parquet  (sent_uid, verb, noun, idf)    <- consumed by analysis
#
# Surface-only.  IDF (across H-2A JOs) pre-computed and baked into the parquet
# so make_scatter / inspect_crop / top_pairs_table just load it.

library(data.table)
library(arrow)

DATA <- "output/text"
WEBB <- "output"
PIPE <- "pipeline"

SENT_RDS <- file.path(DATA, "core", "sentences.parquet")

# Bootstrap: build sentences.parquet if missing (Stage A).
if (!file.exists(SENT_RDS)) {
  cat("Missing", SENT_RDS, "-> running Stage A scripts\n")
  source(file.path(PIPE, "filter_language.R"))
  source(file.path(PIPE, "tag_crops.R"))
  source(file.path(PIPE, "split_sentences.R"))
}
TMP_TSV  <- file.path(WEBB, "pairs/_h2a_sentences.tsv")
LOOKUP   <- file.path(WEBB, "pairs/task_lookup.tsv")
PAIRS    <- file.path(WEBB, "pairs/task_pairs.tsv")
PAIRS_PQ <- file.path(WEBB, "pairs/task_pairs.parquet")

PY  <- "C:/Users/Fer/AppData/Local/Programs/Python/Python310/python.exe"
EXTRACT_PY <- file.path(PIPE, "_extract_pairs.py")

# ---- 1. Load sentences and assign sent_uid in row order -------------
cat("Loading sentences.parquet...\n")
s <- as.data.table(arrow::read_parquet(SENT_RDS))
cat("  rows:", nrow(s), "\n")
s[, sent_uid := sprintf("S%08d", .I)]

# ---- 2. Save lookup table (sent_uid + metadata) ---------------------
lookup_cols <- c("sent_uid","caseNumber","socCode","jobState","year","month",
                  "crops","n_crops","n_tc","n_farm")
fwrite(s[, ..lookup_cols], LOOKUP, sep = "\t")
cat("Wrote:", LOOKUP, "  rows:", nrow(s), "\n")

# ---- 3. Save text TSV consumed by Python ----------------------------
fwrite(s[, .(sent_uid, sentence)], TMP_TSV, sep = "\t", quote = FALSE)

# ---- 4. Run spaCy verb-dobj extraction ------------------------------
t0 <- Sys.time()
cmd1 <- sprintf('"%s" "%s" "%s" "%s" --text-col sentence --id-col sent_uid --batch-size 500 --n-process 8',
                PY, EXTRACT_PY, TMP_TSV, PAIRS)
cat("Running spaCy:\n", cmd1, "\n")
status <- system(cmd1)
if (status != 0) stop("Pair extraction failed")
cat("  done in", round(as.numeric(Sys.time() - t0, units = "mins"), 1), "min\n")

# ---- 5. Pre-compute IDF and save as parquet ------------------------
# IDF = log(total_jos / n_jos_containing_pair).  Boilerplate killer.
out <- fread(PAIRS)
LK <- fread(LOOKUP, select = c("sent_uid","caseNumber"))
m <- merge(out[, .(doc_id, verb, noun)],
           LK, by.x = "doc_id", by.y = "sent_uid")
total_jos <- uniqueN(m$caseNumber)
idf_tab <- m[, .(n_jos_with_pair = uniqueN(caseNumber)), by = .(verb, noun)]
idf_tab[, idf := log(total_jos / n_jos_with_pair)]
out <- merge(out, idf_tab[, .(verb, noun, idf)], by = c("verb","noun"), all.x = TRUE)
out[is.na(idf), idf := 0]
write_parquet(out, PAIRS_PQ)

cat("\n=== Done ===\n")
cat("Final pairs:", nrow(out), "\n")
cat("Unique sentences with pairs:", uniqueN(out$doc_id), "\n")
cat("Unique (verb,noun):", uniqueN(out[, .(verb, noun)]), "\n")
cat("IDF computed across", total_jos, "JOs; max IDF:", round(max(out$idf), 2), "\n")
cat("Saved:", PAIRS_PQ, "\n")
