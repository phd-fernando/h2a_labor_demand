# Task-side pair extraction using spaCy via Python helper, parallel-process safe
# Input  : data/tasks/h2a_sentences_clean2.tsv (doc_id, sentence, ...)
# Output : data/pairs/task_pairs_spacy.tsv     (sent_uid, verb, noun)
#          data/pairs/task_uid_lookup.tsv      (sent_uid -> doc_id, soc_code, ...)

library(data.table)

ROOT      <- "h2a_labor_demand/webb_replication"
INPUT     <- file.path(ROOT, "data/tasks/h2a_sentences_clean2.tsv")
PREP      <- file.path(ROOT, "data/tasks/_h2a_sentences_with_uid.tsv")
OUTPUT    <- file.path(ROOT, "data/pairs/task_pairs_spacy.tsv")
LOOKUP    <- file.path(ROOT, "data/pairs/task_uid_lookup.tsv")
PY_SCRIPT <- "h2a_labor_demand/pipeline/_extract_pairs.py"
PYTHON    <- "C:/Users/Fer/AppData/Local/Programs/Python/Python310/python.exe"

N_LIMIT   <- 100000L   # NULL for full corpus; integer for sample
N_PROCESS <- 24L
BATCH     <- 500L
PREFIX    <- "The worker will "

# ---- 1. Prep: add sent_uid + dump intermediate TSV ----------------------
sent <- fread(INPUT, sep = "\t", quote = "")
cat("Sentences in:", nrow(sent), "\n")
if (!is.null(N_LIMIT) && nrow(sent) > N_LIMIT) {
  set.seed(1)
  sent <- sent[sample(.N, N_LIMIT)]
  cat("Random sample (seed=1):", nrow(sent), "\n")
}
sent <- sent[nchar(trimws(sentence)) > 0]
sent[, sent_uid := sprintf("S%07d", .I)]

# Lookup table: uid -> doc / soc / crop / state / year
fwrite(sent[, .(sent_uid, doc_id, soc_code, crop, state, year)],
       LOOKUP, sep = "\t")

# Intermediate file consumed by Python
fwrite(sent[, .(sent_uid, sentence)], PREP, sep = "\t",
       quote = FALSE)

# ---- 2. Call spaCy Python helper ----------------------------------------
t0 <- Sys.time()
cmd_args <- c(shQuote(PY_SCRIPT),
              shQuote(PREP),
              shQuote(OUTPUT),
              "--text-col", "sentence",
              "--id-col",   "sent_uid",
              "--prepend",  shQuote(PREFIX),
              "--n-process", as.character(N_PROCESS),
              "--batch-size", as.character(BATCH))
status <- system2(PYTHON, cmd_args, stdout = "", stderr = "")
elapsed <- round(difftime(Sys.time(), t0, units = "secs"), 1)
if (status != 0) stop("Python helper failed.")

# ---- 3. Report ----------------------------------------------------------
out <- fread(OUTPUT)
cat("\n=== Done ===\n")
cat("Wall time         :", elapsed, "s\n")
cat("Pairs out         :", nrow(out), "\n")
cat("Unique (verb,noun):", uniqueN(out[, .(verb, noun)]), "\n")
cat("\nTop 20 pairs:\n")
print(out[, .N, by = .(verb, noun)][order(-N)][1:20])
