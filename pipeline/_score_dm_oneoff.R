suppressPackageStartupMessages({library(arrow); library(data.table); library(stringr)})

d <- fread("output/text/dictionaries/onet_ability_terms_dm.tsv")

# Apply stoplist (light verbs + generic adjectives + light verbs missed in expansion script)
STOP <- c(
  # light verbs
  "keep","make","use","do","get","take","give","go","come","look","move","hold",
  "set","put","find","see","tell","say","leave","run","work","stand","sit","lie",
  "lay","bring","send","know","apply","choose","regain","arrange","produce",
  "develop","combine","generate","change","shift","focus","read","identify",
  "detect","judge",
  # generic adjectives / quantifiers
  "general","specific","different","certain","particular","single","simple",
  "short","long","small","large","common","various","other","wrong","likely",
  "exact","maximum","close","far",
  # additions caught in this run
  "maintain","prepare","job","holding"
)
n_before <- nrow(d)
d <- d[!term %in% STOP]
cat(sprintf("Stoplist removed %d rows; dict now %d rows / %d unique terms\n",
            n_before - nrow(d), nrow(d), uniqueN(d$term)))

abilities <- split(d$term, d$ability)
all_terms <- unique(unlist(abilities))
N <- length(abilities)
df <- sapply(all_terms, function(t) sum(sapply(abilities, function(a) t %in% a)))
idf <- log(N / df); names(idf) <- all_terms

# ---- Use farm-only sentences (n_tc==0 & n_farm>=1 already filtered upstream) ----
sents <- as.data.table(read_parquet("output/text/core/sentences.parquet"))
sents <- sents[grepl("^45-2092", socCode)]
cat(sprintf("Farm sentences for SOC 45-2092: %d across %d JOs\n",
            nrow(sents), uniqueN(sents$caseNumber)))

# Pick a JO at random with at least 5 farm sentences for a meaningful sample
jo_counts <- sents[, .N, by = caseNumber][N >= 5]
set.seed(31415)
pick <- jo_counts[sample(.N, 1)]
jo_sents <- sents[caseNumber == pick$caseNumber][order(sid)]
cat(sprintf("\n=== JO %s | %s | %d farm sentences ===\n\n",
            jo_sents$caseNumber[1], jo_sents$jobState[1], nrow(jo_sents)))
for (i in seq_len(nrow(jo_sents))) {
  cat(sprintf("[%d] %s\n", i, jo_sents$sentence[i]))
}
cat("\n")

# Concatenate to one text and tokenize (uniqueness across JO)
txt <- tolower(paste(jo_sents$sentence, collapse = " "))
tokens <- unique(unlist(strsplit(gsub("[^a-z]", " ", txt), " +")))
tokens <- tokens[nchar(tokens) > 0]

MIN_HITS <- 2
res <- data.table(ability = character(), score = numeric(), n_hits = integer(), hits = character())
for (a in names(abilities)) {
  terms <- abilities[[a]]
  single <- terms[!grepl(" ", terms) & !grepl("-", terms)]
  hits <- intersect(single, tokens)
  s <- if (length(hits) >= MIN_HITS) sum(idf[hits]) else 0
  res <- rbind(res, data.table(ability = a, score = round(s, 2), n_hits = length(hits),
                               hits = paste(sprintf("%s(%.2f)", hits, idf[hits]), collapse = ", ")))
}
setorder(res, -score)
cat(sprintf("=== Non-zero ability scores (Datamuse, farm sentences only, >=%d hits) ===\n", MIN_HITS))
print(res[score > 0], nrows = Inf)
cat(sprintf("\nFlagged: %d / 52\n", sum(res$score > 0)))
