# Build master A01-only metadata: patent_id, year, title, abstract, cpc_list.
# Saved as parquet for fast re-reads.

library(data.table)
library(duckdb); library(DBI)
if (!requireNamespace("arrow", quietly = TRUE))
  install.packages("arrow", repos = "https://cloud.r-project.org")
library(arrow)

ROOT <- "output"
OUT  <- file.path(ROOT, "filtered")
CACHE <- file.path(ROOT, "cache")
dir.create(OUT,   recursive = TRUE, showWarnings = FALSE)
dir.create(CACHE, recursive = TRUE, showWarnings = FALSE)

PAT_CACHE <- file.path(CACHE, "patent_table.parquet")
CPC_CACHE <- file.path(CACHE, "cpc_flags.parquet")
PAT_ZIP   <- "data/g_patent.tsv.zip"
CPC_ZIP   <- "data/g_cpc_current.tsv.zip"
ABS_TSV   <- "data/g_patent_abstract.tsv"
OUT_PARQ  <- file.path(OUT, "patent_metadata.parquet")

# ---- 0a. Build patent-year cache from g_patent.tsv.zip if missing ----
# Keep all patents (1976 onwards in the PatentsView dump).
if (!file.exists(PAT_CACHE)) {
  cat("Building patent cache from", PAT_ZIP, "(slow first time)...\n")
  t0 <- Sys.time()
  raw <- fread(cmd = sprintf('unzip -p "%s"', PAT_ZIP),
               select = c("patent_id","patent_date","patent_title"),
               quote = '"')
  raw[, year := as.integer(substr(patent_date, 1, 4))]
  raw <- raw[!is.na(year), .(patent_id, year, patent_title)]
  arrow::write_parquet(raw, PAT_CACHE,
                       compression = "zstd", compression_level = 7)
  cat("  wrote", PAT_CACHE, ":", nrow(raw), "rows in",
      round(as.numeric(Sys.time() - t0, units = "secs"), 1), "s\n")
  rm(raw); gc()
}

# ---- 0b. Build CPC cache: A01 patent_ids only ------------------------
# Only A01* patents are kept.  The full CPC list per A01 patent is
# recovered later in step 3.  No B25J/A61/B01 dead Webb-era flags.
if (!file.exists(CPC_CACHE)) {
  cat("Building CPC cache (A01 only) from", CPC_ZIP, "(slow first time)...\n")
  t0 <- Sys.time()
  raw <- fread(cmd = sprintf('unzip -p "%s"', CPC_ZIP),
               select = c("patent_id","cpc_subclass"),
               quote = '"')
  a01_ids_dt <- raw[grepl("^A01", cpc_subclass), .(patent_id = unique(patent_id))]
  arrow::write_parquet(a01_ids_dt, CPC_CACHE,
                       compression = "zstd", compression_level = 7)
  cat("  wrote", CPC_CACHE, ":", nrow(a01_ids_dt), "A01 patents in",
      round(as.numeric(Sys.time() - t0, units = "secs"), 1), "s\n")
  rm(raw, a01_ids_dt); gc()
}

# ---- 1. Patent table (cached) + A01 filter --------------------------
cat("Loading patent + CPC caches...\n")
pat <- as.data.table(arrow::read_parquet(PAT_CACHE))
a01_ids <- as.data.table(arrow::read_parquet(CPC_CACHE))$patent_id
pat[, patent_id := as.character(patent_id)]
a01_ids <- as.character(a01_ids)
pat <- pat[patent_id %in% a01_ids]
cat("A01 patents:", nrow(pat), "\n")

# ---- 2. Abstracts (DuckDB, filtered) --------------------------------
cat("Pulling abstracts via DuckDB...\n")
t0 <- Sys.time()
con <- dbConnect(duckdb::duckdb())
DBI::dbWriteTable(con, "ids", data.frame(patent_id = a01_ids), overwrite = TRUE)
q <- sprintf("SELECT a.patent_id, a.patent_abstract
              FROM read_csv('%s', delim='\t', quote='\"', header=true,
                             ignore_errors=true,
                             columns={'patent_id':'VARCHAR',
                                      'patent_abstract':'VARCHAR'}) a
              JOIN ids USING (patent_id)", ABS_TSV)
abs <- as.data.table(dbGetQuery(con, q))
dbDisconnect(con, shutdown = TRUE)
cat("  abstracts retrieved:", nrow(abs),
    "  elapsed:", round(as.numeric(Sys.time()-t0, units="secs"), 1), "s\n")
abs[, patent_id := as.character(patent_id)]
pat <- merge(pat, abs, by = "patent_id", all.x = TRUE)
pat[is.na(patent_abstract), patent_abstract := ""]
cat("  patents missing abstract:", sum(pat$patent_abstract == ""), "\n")

# ---- 3. ALL CPCs per patent (read from zip, filter to A01 patents) --
cat("Pulling all CPCs for A01 patents...\n")
t0 <- Sys.time()
cpc_full <- fread(cmd = sprintf('unzip -p "%s"', CPC_ZIP),
                  select = c("patent_id","cpc_subclass"),
                  quote = '"')
cpc_full[, patent_id := as.character(patent_id)]
cpc_full <- cpc_full[patent_id %in% a01_ids]
cpc_lists <- cpc_full[, .(cpc_list = paste(unique(cpc_subclass), collapse = ";"),
                           n_cpc = uniqueN(cpc_subclass)),
                       by = patent_id]
cat("  CPC rows for A01 patents:", nrow(cpc_full),
    "  unique patents:", nrow(cpc_lists),
    "  elapsed:", round(as.numeric(Sys.time()-t0, units="secs"), 1), "s\n")
pat <- merge(pat, cpc_lists, by = "patent_id", all.x = TRUE)

# ---- 4. Save parquet -------------------------------------------------
out <- pat[, .(patent_id, year, patent_title, patent_abstract, cpc_list, n_cpc)]
write_parquet(out, OUT_PARQ)
cat("\nSaved:", OUT_PARQ, " (", nrow(out), "rows)\n")

cat("\nQuick sanity check (first 3 rows):\n")
print(out[1:3, .(patent_id, year, n_cpc,
                 title_short = substr(patent_title, 1, 60),
                 abs_short   = substr(patent_abstract, 1, 60),
                 cpc_short   = substr(cpc_list, 1, 50))])
cat("\nYear distribution:\n")
print(out[, .N, by = year][order(year)])
