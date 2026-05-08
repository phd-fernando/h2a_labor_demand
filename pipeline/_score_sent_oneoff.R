suppressPackageStartupMessages({library(arrow); library(data.table); library(stringr)})

d <- fread("output/text/dictionaries/onet_ability_terms_wn.tsv")
abilities <- split(d$term, d$ability)
all_terms <- unique(unlist(abilities))
N <- length(abilities)
df <- sapply(all_terms, function(t) sum(sapply(abilities, function(a) t %in% a)))
idf <- log(N / df); names(idf) <- all_terms

p <- as.data.table(read_parquet("output/text/core/jo_full.parquet"))
p <- p[!is.na(duties_full) & nchar(duties_full) > 500 & nchar(duties_full) < 4000 &
       grepl("^45-2092", socCode)]
set.seed(987)
jo <- p[sample(.N, 1)]
cat(sprintf("=== JO %s | %s ===\n\n", jo$caseNumber, jo$jobState))

txt <- jo$duties_full
sents <- unlist(strsplit(txt, "(?<=[.;!?])\\s+", perl = TRUE))
sents <- str_squish(sents)
sents <- sents[nchar(sents) > 15]
cat("Sentences:", length(sents), "\n\n")

score_sent <- function(s) {
  s_lower <- tolower(s)
  toks <- unique(unlist(strsplit(gsub("[^a-z]", " ", s_lower), " +")))
  toks <- toks[nchar(toks) > 0]
  out <- list()
  for (a in names(abilities)) {
    terms <- abilities[[a]]
    single <- terms[!grepl(" ", terms) & !grepl("-", terms)]
    multi  <- terms[grepl(" ", terms) | grepl("-", terms)]
    hs <- intersect(single, toks)
    hm <- character(0)
    if (length(multi)) {
      keep <- vapply(multi, function(t) grepl(t, s_lower, fixed = TRUE), logical(1))
      hm <- multi[keep]
    }
    hits <- c(hs, hm)
    if (length(hits) > 0) {
      out[[a]] <- list(score = sum(idf[hits]), hits = hits)
    }
  }
  out
}

for (i in seq_along(sents)) {
  scores <- score_sent(sents[i])
  cat(sprintf("--- Sentence %d ---\n%s\n", i, sents[i]))
  if (length(scores) == 0) {
    cat("  (no ability hits)\n\n")
    next
  }
  ranked <- sort(sapply(scores, function(x) x$score), decreasing = TRUE)
  for (a in names(ranked)[1:min(3, length(ranked))]) {
    cat(sprintf("  [%.2f] %s   hits: %s\n",
                ranked[a], a, paste(scores[[a]]$hits, collapse = ", ")))
  }
  cat("\n")
}
