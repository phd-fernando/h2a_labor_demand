###############################################################################
# run_all.R - cascade orchestrator for the H-2A patent-exposure pipeline.
#
# Stages (each rebuilds only if input is newer than output):
#   A  H-2A text pipeline                  sentences.parquet
#   B  H-2A task pair extraction           task_pairs.tsv
#   C  Patent metadata + pair extraction   patent_metadata.parquet
#                                          patent_pairs.parquet
#   D  Master figure (default filter)      scatter_all_a01.png
#   E  Top-pairs companion table           top_pairs_all_a01.tsv
#
# Force-rebuild a stage by passing its letter on the command line:
#   Rscript h2a_labor_demand/scripts/run_all.R           # incremental
#   Rscript h2a_labor_demand/scripts/run_all.R B C       # force B and C
#   Rscript h2a_labor_demand/scripts/run_all.R ALL       # force everything
###############################################################################

suppressPackageStartupMessages({ library(dplyr); library(data.table) })

args      <- commandArgs(trailingOnly = TRUE)
FORCE_ALL <- "ALL" %in% toupper(args)
FORCE_SET <- toupper(args)

DATA <- "output/text"
WEBB <- "output"
PIPE <- "pipeline"

needs_rebuild <- function(output, inputs, force = FALSE) {
  if (force) return(TRUE)
  if (!file.exists(output)) return(TRUE)
  out_mtime <- file.mtime(output)
  for (pat in inputs) {
    files <- Sys.glob(pat)
    if (length(files) == 0) next
    if (any(file.mtime(files) > out_mtime)) return(TRUE)
  }
  FALSE
}

run_stage <- function(name, output, inputs, scripts, args_extra = NULL) {
  force <- FORCE_ALL || (name %in% FORCE_SET)
  if (!needs_rebuild(output, inputs, force)) {
    cat(sprintf("[SKIP ] Stage %s  (output is fresh)\n", name)); return(invisible(FALSE))
  }
  cat(sprintf("[RUN  ] Stage %s  ->  %s\n", name, output))
  for (s in scripts) {
    if (!file.exists(s)) { cat(sprintf("        WARNING: missing %s\n", s)); next }
    cat(sprintf("        sourcing: %s\n", s))
    t0 <- Sys.time()
    if (!is.null(args_extra) && s == tail(scripts, 1)) {
      # Run last script with CLI args (for stages D/E which take filter args)
      cmd <- sprintf('Rscript "%s" %s', s,
                     paste(shQuote(args_extra), collapse = " "))
      system(cmd)
    } else {
      tryCatch(source(s, echo = FALSE),
               error = function(e) stop("Stage ", name, " failed in ", s, ": ", e$message))
    }
    cat(sprintf("        %.1f min\n",
                as.numeric(difftime(Sys.time(), t0, units = "mins"))))
  }
  invisible(TRUE)
}

# ---- Stage A: H-2A text pipeline -------------------------------------------
run_stage("A",
  output = file.path(DATA, "core", "sentences.parquet"),
  inputs = c(file.path(DATA, "core", "jo_full.parquet"),
             file.path(PIPE, "filter_language.R"),
             file.path(PIPE, "tag_crops.R"),
             file.path(PIPE, "split_sentences.R")),
  scripts = c(file.path(PIPE, "filter_language.R"),
              file.path(PIPE, "tag_crops.R"),
              file.path(PIPE, "split_sentences.R")))

# ---- Stage B: H-2A task pair extraction ------------------------------------
run_stage("B",
  output = file.path(WEBB, "pairs", "task_pairs.tsv"),
  inputs = c(file.path(DATA, "core", "sentences.parquet"),
             file.path(PIPE, "_extract_pairs.py"),
             file.path(PIPE, "extract_task_pairs.R")),
  scripts = file.path(PIPE, "extract_task_pairs.R"))

# ---- Stage C: Patent metadata + pairs --------------------------------------
run_stage("C",
  output = file.path(WEBB, "filtered", "patent_pairs.parquet"),
  inputs = c("data/g_patent.tsv",
             "data/g_cpc_current.tsv",
             "data/g_patent_abstract.tsv",
             file.path(PIPE, "pull_patents.R"),
             file.path(PIPE, "extract_patent_pairs.R")),
  scripts = c(file.path(PIPE, "pull_patents.R"),
              file.path(PIPE, "extract_patent_pairs.R")))

# ---- Stage D: Master figure (filter = TRUE = all A01) ----------------------
run_stage("D",
  output = file.path(WEBB, "results", "scatter_all_a01.png"),
  inputs = c(file.path(WEBB, "filtered", "patent_pairs.parquet"),
             file.path(WEBB, "pairs", "task_pairs.tsv"),
             file.path(PIPE, "make_scatter.R")),
  scripts = file.path(PIPE, "make_scatter.R"),
  args_extra = c("TRUE", "scatter_all_a01.png"))

# ---- Stage E: Top-pairs companion table ------------------------------------
run_stage("E",
  output = file.path(WEBB, "results", "top_pairs_all_a01.tsv"),
  inputs = c(file.path(WEBB, "filtered", "patent_pairs.parquet"),
             file.path(WEBB, "pairs", "task_pairs.tsv"),
             file.path(PIPE, "top_pairs_table.R")),
  scripts = file.path(PIPE, "top_pairs_table.R"),
  args_extra = c("TRUE",
                 file.path(WEBB, "results", "top_pairs_all_a01.tsv"),
                 "5"))

cat("\n=== CASCADE COMPLETE ===\n")
