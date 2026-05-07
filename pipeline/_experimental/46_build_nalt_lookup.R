# Parse NALT Turtle -> build term -> concept_id lookup.
# Each NALT skos:Concept has a prefLabel + zero or more altLabels (multilingual).
# We keep English labels only, lowercase, and store concept_id (e.g., "nalt:10000")
# along with the preferred English label.

library(data.table)

ROOT <- "h2a_labor_demand/webb_replication"
NALT_TTL <- "data/nalt/nalt-full_dwn_20240716.ttl"
OUT      <- file.path(ROOT, "data/cache/nalt_lookup.rds")
dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)

cat("Reading NALT ttl...\n")
lines <- readLines(NALT_TTL, warn = FALSE)
cat("  lines:", length(lines), "\n")

# Find concept-block starts: lines like 'nalt:10000 a skos:Concept,'
concept_starts <- grep("^nalt:[0-9]+ a skos:Concept", lines)
cat("  concepts found:", length(concept_starts), "\n")

# Walk each block until '.' line. Extract concept_id, prefLabel @en, altLabel @en.
concept_ids <- sub("^(nalt:[0-9]+).*", "\\1", lines[concept_starts])

# Find ends: each concept block terminates with line ending in '.'
ends <- integer(length(concept_starts))
for (i in seq_along(concept_starts)) {
  st <- concept_starts[i]
  ed <- if (i < length(concept_starts)) concept_starts[i + 1] - 1L else length(lines)
  blk <- lines[st:ed]
  end_in_blk <- grep("[.][\t ]*$", blk)[1]
  ends[i] <- st + end_in_blk - 1L
}

# Collect prefLabel and altLabel (English) from each block
out_rows <- vector("list", length(concept_starts))
for (i in seq_along(concept_starts)) {
  blk <- lines[concept_starts[i]:ends[i]]
  txt <- paste(blk, collapse = "\n")
  pref <- regmatches(txt, regexpr('skos:prefLabel\\s+"[^"]+"@en', txt))
  alts <- regmatches(txt, gregexpr('"[^"]+"@en', txt))[[1]]
  pref_lab <- if (length(pref)) sub('skos:prefLabel\\s+"([^"]+)"@en', "\\1", pref) else NA_character_
  all_labs <- if (length(alts)) sub('"([^"]+)"@en', "\\1", alts) else character(0)
  if (length(all_labs) == 0 && !is.na(pref_lab)) all_labs <- pref_lab
  out_rows[[i]] <- data.table(
    concept_id = rep(concept_ids[i], length(all_labs)),
    pref_label = rep(pref_lab, length(all_labs)),
    label      = all_labs)
  if (i %% 20000 == 0) cat("  processed", i, "concepts\n")
}
nalt <- rbindlist(out_rows)
nalt[, label_lc := tolower(label)]
nalt <- unique(nalt, by = c("concept_id","label_lc"))
cat("Total label rows:", nrow(nalt),
    "  unique labels:", uniqueN(nalt$label_lc), "\n")

# Quick sanity: lookup some words
samples <- c("apple","blueberry","corn","strawberry","tomato","tractor",
             "harvesting","picking","spraying","fertilizer","drone","robot",
             "lift","carry","plant","cut","prune","pollinate","irrigation",
             "weed","pest","employer","worker")
cat("\nSample term -> NALT concepts:\n")
for (w in samples) {
  hits <- nalt[label_lc == w]
  if (nrow(hits)) {
    cat(sprintf("  %-15s -> %s (pref: '%s')\n",
                w, hits$concept_id[1], hits$pref_label[1]))
  } else {
    cat(sprintf("  %-15s -> [not in NALT]\n", w))
  }
}

# Save: pick first concept_id per label
lookup <- nalt[!is.na(label_lc), .(concept_id = concept_id[1],
                                     pref_label = pref_label[1]),
                by = label_lc]
saveRDS(lookup, OUT)
cat("\nSaved:", OUT, " (", nrow(lookup), "labels)\n")

# Also build a STEMMED lookup: Porter stem of each label.
# Bridges plural/gerund (NALT) vs lemma (spaCy output) at lookup time.
if (!requireNamespace("SnowballC", quietly = TRUE))
  install.packages("SnowballC", repos = "https://cloud.r-project.org")
library(SnowballC)
lookup[, stem := wordStem(label_lc, language = "en")]
stem_lookup <- lookup[, .(concept_id = concept_id[1],
                           pref_label = pref_label[1],
                           label_lc   = label_lc[1]),
                       by = stem]
OUT_STEM <- file.path(ROOT, "data/cache/nalt_lookup_stemmed.rds")
saveRDS(stem_lookup, OUT_STEM)
cat("Saved stemmed lookup:", OUT_STEM,
    " (", nrow(stem_lookup), "stems)\n")

# Sanity recheck on stemmed lookup
cat("\nStemmed lookup sample:\n")
samples <- c("apple","blueberry","corn","strawberry","tomato","tractor",
             "harvest","pick","spray","fertilizer","drone","robot",
             "lift","carry","plant","cut","prune","pollinate","irrigate",
             "weed","pest","employer","worker")
for (w in samples) {
  s <- wordStem(w, "en")
  hits <- stem_lookup[stem == s]
  if (nrow(hits)) cat(sprintf("  %-12s [%s] -> %s (%s)\n",
                              w, s, hits$concept_id[1], hits$pref_label[1]))
  else            cat(sprintf("  %-12s [%s] -> [not in NALT]\n", w, s))
}
