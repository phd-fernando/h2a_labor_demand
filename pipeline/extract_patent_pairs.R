# Extract verb-dobj pairs from title + abstract for the A01 master metadata.
# Saves patent_pairs.parquet (one row per pair, linked by patent_id).

library(data.table)
library(arrow)

ROOT <- "output"
PIPE <- "pipeline"
META <- file.path(ROOT, "filtered/patent_metadata.parquet")

# Bootstrap: build patent_metadata.parquet if missing.
if (!file.exists(META)) {
  cat("Missing", META, "-> running pull_patents.R\n")
  source(file.path(PIPE, "pull_patents.R"))
}
TMP_TSV <- file.path(ROOT, "filtered/_master_for_extraction.tsv")
RAW_OUT <- file.path(ROOT, "pairs/patent_pairs.tsv")
PARQ_OUT <- file.path(ROOT, "filtered/patent_pairs.parquet")

PY <- "C:/Users/Fer/AppData/Local/Programs/Python/Python310/python.exe"

# 1. Build extraction input: patent_id + (title + " " + abstract)
m <- as.data.table(read_parquet(META))
m[, text := paste(patent_title, patent_abstract, sep = " ")]
cat("Patents to process:", nrow(m), "\n")
cat("Mean text length (chars):", round(mean(nchar(m$text)), 0), "\n")
fwrite(m[, .(patent_id, text)], TMP_TSV, sep = "\t")

# 2. spaCy extraction
PIPE <- "pipeline"
cmd1 <- sprintf('"%s" "%s/_extract_pairs.py" "%s" "%s" --text-col text --id-col patent_id --batch-size 200 --n-process 8',
                PY, PIPE, TMP_TSV, RAW_OUT)
cat("Running:\n", cmd1, "\n")
t0 <- Sys.time()
status1 <- system(cmd1)
cat("Pair extraction done in",
    round(as.numeric(Sys.time() - t0, units = "mins"), 1), "min, status:", status1, "\n")
if (status1 != 0) stop("Pair extraction failed")

# 3. Save as parquet (surface only; no WordNet aggregation)
pairs <- fread(RAW_OUT)
write_parquet(pairs, PARQ_OUT)
cat("\nFinal pairs:", nrow(pairs), "\n")
cat("Unique patents with pairs:", uniqueN(pairs$doc_id), "\n")
cat("Unique (verb,noun):", uniqueN(pairs[, .(verb,noun)]), "\n")
cat("Saved:", PARQ_OUT, "\n")
