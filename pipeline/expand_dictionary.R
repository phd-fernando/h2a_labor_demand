###############################################################################
# expand_dictionary.R — re-expand the bigram dictionary from manual seeds only.
#
# Differences from _archive/scripts/expand_dict_v3_rounds.R:
#   1. Resets to manual-only seeds (drops the prior majority labels).
#   2. Adds Rule (*): a candidate unclassified bigram is rejected if it shares
#      ANY word with a classified bigram (T&C or farm) elsewhere in the same
#      sentence. Prevents fragment-leakage from existing labels.
#
# Output: output/text/dictionaries/bigram_dictionary.csv (overwritten)
###############################################################################

suppressPackageStartupMessages({
  library(dplyr); library(stringr); library(tidyr); library(tidytext); library(arrow); library(data.table)
})

DATA <- "output/text"
DICT <- file.path(DATA, "dictionaries", "bigram_dictionary.csv")
SENT <- file.path(DATA, "core", "sentence_classification.parquet")

# ---- 0. Reset dictionary to manual-only ------------------------------------
cur <- read.csv(DICT, stringsAsFactors = FALSE)
cat(sprintf("Current dict: %d rows (manual=%d, majority=%d)\n",
            nrow(cur),
            sum(cur$method == "manual"),
            sum(cur$method == "majority")))
manual <- cur |> filter(method == "manual")
write.csv(manual, DICT, row.names = FALSE)
cat(sprintf("Reset to manual-only: %d rows\n", nrow(manual)))

# ---- 1. Tokenize once ------------------------------------------------------
sent <- as.data.table(read_parquet(SENT))[is_boilerplate == FALSE]
sent[, sid := .I]
cat("Sentences (non-boilerplate):", nrow(sent), "\n")

extra_stop <- c("worker","workers","employer","employee","employees",
                "employment","job","jobs","work","working","required","must",
                "shall","will","may","etc","perform","performed","performing",
                "na","n","h2a","h-2a","ust","include","including","wage",
                "hours","hour","day","days","week","time")

cat("Tokenizing all bigrams (one-time)...\n")
t0 <- Sys.time()
bg <- sent[, .(sid, s)] |>
  unnest_tokens(bigram, s, token = "ngrams", n = 2) |>
  separate(bigram, c("w1","w2"), sep = " ", fill = "right", remove = FALSE) |>
  filter(!is.na(w1), !is.na(w2),
         !w1 %in% stop_words$word, !w2 %in% stop_words$word,
         !w1 %in% extra_stop, !w2 %in% extra_stop,
         str_detect(w1, "^[a-z]{3,}$"), str_detect(w2, "^[a-z]{3,}$")) |>
  select(sid, bigram, w1, w2) |> distinct()
setDT(bg)
cat(sprintf("  %.1f min, %d (sid, bigram) rows\n",
            as.numeric(Sys.time()-t0, units="mins"), nrow(bg)))

# ---- 2. Round runner with Rule (*) -----------------------------------------
run_round <- function(side, n_unc_target) {
  dict <- read.csv(DICT, stringsAsFactors = FALSE)
  if (!"method" %in% names(dict)) dict$method <- "manual"
  tc      <- dict |> filter(label == 0) |> pull(bigram)
  farm    <- dict |> filter(label == 1) |> pull(bigram)
  labeled <- dict$bigram

  iter <- 0; total_new <- 0
  cat(sprintf("\n=== Round: side=%s, n_unc=%d ===\n", side, n_unc_target))

  repeat {
    iter <- iter + 1

    # Classify each bigram in each sentence
    bg[, cls := fifelse(bigram %in% tc, "tc",
                fifelse(bigram %in% farm, "farm", "unc"))]
    cnt <- dcast(bg[, .N, by = .(sid, cls)],
                 sid ~ cls, value.var = "N", fill = 0L)
    for (col in c("tc","farm","unc"))
      if (!col %in% names(cnt)) cnt[[col]] <- 0L

    if (side == "tc") {
      trig <- cnt[tc >= 3 & farm == 0 & unc == n_unc_target, .(sid)]
    } else {
      trig <- cnt[farm >= 3 & tc == 0 & unc == n_unc_target, .(sid)]
    }

    # Words used by ALREADY-classified bigrams in each triggering sentence
    classified_words <- bg[sid %in% trig$sid & cls != "unc",
                           .(words = list(unique(c(w1, w2)))), by = sid]

    # Candidate unclassified bigrams from triggering sentences
    cand <- bg[sid %in% trig$sid & cls == "unc"]

    # Apply Rule (*): drop unclass bigrams whose w1 or w2 appears in
    # any already-classified bigram of the same sentence.
    cand_filt <- merge(cand, classified_words, by = "sid", all.x = TRUE)
    cand_filt[, kept := mapply(function(w1, w2, words) {
      if (is.null(words)) return(TRUE)
      !(w1 %in% words || w2 %in% words)
    }, w1, w2, words)]
    n_blocked <- sum(!cand_filt$kept)
    cand_filt <- cand_filt[kept == TRUE]

    candidate_bg <- unique(cand_filt$bigram)
    candidate_bg <- candidate_bg[!candidate_bg %in% labeled]

    # Frequency / shape filter
    jo_counts_all <- bg[bigram %in% candidate_bg, .(n_jos = .N), by = bigram]
    parts <- data.table(bigram = candidate_bg) |>
      tidyr::separate(bigram, c("w1","w2"), sep = " ", remove = FALSE) |>
      as.data.table()
    keep_bg <- parts[nchar(w1) >= 4 & nchar(w2) >= 4
                    ][bigram %in% jo_counts_all$bigram
                    ][, .(bigram)]
    keep_bg <- merge(keep_bg, jo_counts_all, by = "bigram")
    new_bg <- keep_bg[n_jos >= 5, bigram]

    cat(sprintf("  Iter %d | trig=%d | blocked by Rule(*)=%d | cand=%d | kept=%d\n",
                iter, nrow(trig), n_blocked, length(candidate_bg), length(new_bg)))
    if (length(new_bg) == 0) break

    new_label <- if (side == "tc") 0L else 1L
    jo_counts <- jo_counts_all[bigram %in% new_bg]
    new_rows <- data.frame(bigram = new_bg, label = new_label,
                            n_jos = jo_counts$n_jos[match(new_bg, jo_counts$bigram)],
                            method = "majority", stringsAsFactors = FALSE)
    dict <- rbind(dict, new_rows)
    if (side == "tc") tc <- c(tc, new_bg) else farm <- c(farm, new_bg)
    labeled <- c(labeled, new_bg)
    total_new <- total_new + length(new_bg)
    write.csv(dict, DICT, row.names = FALSE)
  }
  cat(sprintf("  TOTAL new %s bigrams: %d\n", side, total_new))
  cat(sprintf("  Dict now: %d (T&C=%d, farm=%d)\n",
              nrow(dict), sum(dict$label == 0), sum(dict$label == 1)))
}

# ---- 3. Six rounds (same schedule as the v3 expansion) ---------------------
run_round("tc",   1)
run_round("farm", 1)
run_round("tc",   2)
run_round("farm", 2)
run_round("tc",   3)
run_round("farm", 3)

cat("\n=== ALL ROUNDS DONE ===\n")
final <- read.csv(DICT, stringsAsFactors = FALSE)
print(final |> count(label, method))
