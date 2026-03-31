###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department,
# University of Florida.
#
# Text analytics on H-2A job order descriptions (section A.8a)
# Emerging unigrams, bigrams, trigrams over time
###############################################################################

cat("\014"); rm(list = ls())

mydir = "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor"
setwd(mydir)

# packages
options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidytext", "tidyverse", "ggplot2", "stringr", "dplyr",
              "tidyr", "forcats", "lubridate", "ggrepel")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ------------------------------------------------------------------------------
# 1. Load data — use df_clean from tasks_jo.rds (Step 2 output).
#    text_clean is already boilerplate-removed, Spanish-filtered, and
#    restricted to sentences from which agricultural verbs were extracted.
# ------------------------------------------------------------------------------
tasks <- readRDS(paste0(mydir, "/h2a_labor_demand/processed/text/tasks_jo.rds"))
df    <- tasks$df_clean   # caseNumber, period, dateSubmitted, year, quarter, text_clean

periods_ord <- sort(unique(df$period))
n_periods   <- length(periods_ord)

cat("Date range:", as.character(min(df$dateSubmitted)), "to",
    as.character(max(df$dateSubmitted)), "\n")
cat("Periods:", paste(periods_ord, collapse = ", "), "\n")
cat("N docs:", nrow(df), "\n\n")

# ------------------------------------------------------------------------------
# 3. Custom stopwords (English + Spanish + agriculture-specific)
# ------------------------------------------------------------------------------
# Spanish stopwords via tidytext
es_stops <- get_stopwords(language = "es") %>% rename(word = word)

ag_stopwords <- tibble(word = c(
  "workers", "worker", "employer", "work", "include", "including",
  "required", "require", "perform", "may", "also", "one", "two", "three",
  "per", "hour", "hours", "day", "days", "week", "weeks", "rate", "pay",
  "able", "duties", "tasks", "job", "field", "fields", "following",
  "equipment", "use", "used", "using", "operate", "operation",
  "maintain", "maintenance", "clean", "cleaning", "repair", "repairs"
))

stop_words_all <- bind_rows(stop_words, es_stops, ag_stopwords) %>%
  distinct(word)

# ------------------------------------------------------------------------------
# 3. Tokenize: unigrams, bigrams, trigrams
# ------------------------------------------------------------------------------

# -- Unigrams ------------------------------------------------------------------
tokens_uni <- df %>%
  select(caseNumber, period, text_clean) %>%
  unnest_tokens(word, text_clean) %>%
  filter(str_detect(word, "^[a-z]+$")) %>%         # letters only
  anti_join(stop_words_all, by = "word")

# -- Bigrams -------------------------------------------------------------------
tokens_bi <- df %>%
  select(caseNumber, period, text_clean) %>%
  unnest_tokens(bigram, text_clean, token = "ngrams", n = 2) %>%
  separate(bigram, into = c("w1", "w2"), sep = " ", remove = FALSE) %>%
  filter(!w1 %in% stop_words_all$word,
         !w2 %in% stop_words_all$word,
         str_detect(w1, "^[a-z]+$"),
         str_detect(w2, "^[a-z]+$")) %>%
  select(caseNumber, period, bigram)

# -- Trigrams ------------------------------------------------------------------
tokens_tri <- df %>%
  select(caseNumber, period, text_clean) %>%
  unnest_tokens(trigram, text_clean, token = "ngrams", n = 3) %>%
  separate(trigram, into = c("w1", "w2", "w3"), sep = " ", remove = FALSE) %>%
  filter(!w1 %in% stop_words_all$word,
         !w3 %in% stop_words_all$word,         # filter at edges only
         str_detect(w1, "^[a-z]+$"),
         str_detect(w2, "^[a-z]+$"),
         str_detect(w3, "^[a-z]+$")) %>%
  select(caseNumber, period, trigram)

# ------------------------------------------------------------------------------
# 4. Overall top terms (full sample)
# ------------------------------------------------------------------------------
top_uni <- tokens_uni %>%
  count(word, sort = TRUE) %>%
  slice_head(n = 30)

top_bi <- tokens_bi %>%
  count(bigram, sort = TRUE) %>%
  slice_head(n = 30)

top_tri <- tokens_tri %>%
  count(trigram, sort = TRUE) %>%
  slice_head(n = 30)

cat("=== Top 30 unigrams ===\n"); print(as.data.frame(top_uni))
cat("\n=== Top 30 bigrams ===\n");  print(as.data.frame(top_bi))
cat("\n=== Top 30 trigrams ===\n"); print(as.data.frame(top_tri))

# ------------------------------------------------------------------------------
# 5. Emerging terms: chronological 50/50 doc split (robust to period imbalance)
# ------------------------------------------------------------------------------
# Split by doc rank to get balanced halves regardless of period distribution
df_sorted   <- df %>% arrange(dateSubmitted) %>% mutate(doc_rank = row_number())
cutoff      <- floor(nrow(df_sorted) / 2)
early_cases <- df_sorted %>% filter(doc_rank <= cutoff) %>% pull(caseNumber)
late_cases  <- df_sorted %>% filter(doc_rank >  cutoff) %>% pull(caseNumber)

cat(sprintf("Emerging terms split: early n=%d (%s to %s)  |  late n=%d (%s to %s)\n",
  length(early_cases),
  as.character(min(df_sorted$dateSubmitted[df_sorted$doc_rank <= cutoff])),
  as.character(max(df_sorted$dateSubmitted[df_sorted$doc_rank <= cutoff])),
  length(late_cases),
  as.character(min(df_sorted$dateSubmitted[df_sorted$doc_rank >  cutoff])),
  as.character(max(df_sorted$dateSubmitted[df_sorted$doc_rank >  cutoff]))
))

# min support: term must appear in >= 0.5% of docs in each half
min_n_early <- max(3, floor(length(early_cases) * 0.005))
min_n_late  <- max(3, floor(length(late_cases)  * 0.005))

emerge_fn <- function(tokens_df, term_col) {
  freq_early <- tokens_df %>%
    filter(caseNumber %in% early_cases) %>%
    count(.data[[term_col]]) %>%
    mutate(share_early = n / length(early_cases)) %>%
    rename(n_early = n)

  freq_late <- tokens_df %>%
    filter(caseNumber %in% late_cases) %>%
    count(.data[[term_col]]) %>%
    mutate(share_late = n / length(late_cases)) %>%
    rename(n_late = n)

  freq_early %>%
    inner_join(freq_late, by = term_col) %>%
    mutate(
      growth   = (share_late - share_early) / (share_early + 1e-9),
      abs_lift = share_late - share_early
    ) %>%
    filter(n_early >= min_n_early, n_late >= min_n_late) %>%
    arrange(desc(growth))
}

emerging_uni <- emerge_fn(tokens_uni, "word")
emerging_bi  <- emerge_fn(tokens_bi,  "bigram")
emerging_tri <- emerge_fn(tokens_tri, "trigram")

cat("\n=== Top 20 EMERGING unigrams ===\n")
print(as.data.frame(slice_head(emerging_uni, n = 20)))
cat("\n=== Top 20 EMERGING bigrams ===\n")
print(as.data.frame(slice_head(emerging_bi, n = 20)))
cat("\n=== Top 20 EMERGING trigrams ===\n")
print(as.data.frame(slice_head(emerging_tri, n = 20)))

cat("\n=== Top 20 DECLINING unigrams ===\n")
print(as.data.frame(slice_tail(emerging_uni, n = 20)))

# ------------------------------------------------------------------------------
# 6. Period-by-period TF-IDF (treat each period as a "document")
# ------------------------------------------------------------------------------
tfidf_uni <- tokens_uni %>%
  count(period, word) %>%
  bind_tf_idf(word, period, n) %>%
  arrange(period, desc(tf_idf))

tfidf_bi <- tokens_bi %>%
  count(period, bigram) %>%
  bind_tf_idf(bigram, period, n) %>%
  arrange(period, desc(tf_idf))

cat("\n=== Top TF-IDF unigram per period ===\n")
tfidf_uni %>%
  group_by(period) %>%
  slice_max(tf_idf, n = 5) %>%
  as.data.frame() %>%
  print()

cat("\n=== Top TF-IDF bigram per period ===\n")
tfidf_bi %>%
  group_by(period) %>%
  slice_max(tf_idf, n = 5) %>%
  as.data.frame() %>%
  print()

# ------------------------------------------------------------------------------
# 7. Plots
# ------------------------------------------------------------------------------
out_dir <- paste0(mydir, "/h2a_labor_demand/processed/text")
img_dir <- paste0(mydir, "/h2a_labor_demand/processed/images")

# 7a. Top 20 overall unigrams
p_top_uni <- top_uni %>%
  slice_head(n = 20) %>%
  mutate(word = fct_reorder(word, n)) %>%
  ggplot(aes(n, word)) +
  geom_col(fill = "#2c7bb6") +
  labs(title = "Top 20 Unigrams — H-2A Job Orders",
       x = "Count", y = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_top_unigrams.png"), p_top_uni, width = 7, height = 5)

# 7b. Top 20 overall bigrams
p_top_bi <- top_bi %>%
  slice_head(n = 20) %>%
  mutate(bigram = fct_reorder(bigram, n)) %>%
  ggplot(aes(n, bigram)) +
  geom_col(fill = "#1a9641") +
  labs(title = "Top 20 Bigrams — H-2A Job Orders",
       x = "Count", y = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_top_bigrams.png"), p_top_bi, width = 8, height = 5)

# 7c. Top 20 overall trigrams
p_top_tri <- top_tri %>%
  slice_head(n = 20) %>%
  mutate(trigram = fct_reorder(trigram, n)) %>%
  ggplot(aes(n, trigram)) +
  geom_col(fill = "#d7191c") +
  labs(title = "Top 20 Trigrams — H-2A Job Orders",
       x = "Count", y = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_top_trigrams.png"), p_top_tri, width = 10, height = 5)

# 7d. Emerging vs declining unigrams (bubble plot)
p_emerge <- emerging_uni %>%
  slice_head(n = 30) %>%
  bind_rows(slice_tail(emerging_uni, n = 15)) %>%
  mutate(direction = ifelse(growth > 0, "Emerging", "Declining")) %>%
  ggplot(aes(share_early, share_late, size = abs_lift, color = direction,
             label = word)) +
  geom_point(alpha = 0.7) +
  geom_abline(linetype = "dashed", color = "grey50") +
  geom_text_repel(size = 3, max.overlaps = 20) +
  scale_color_manual(values = c("Emerging" = "#1a9641", "Declining" = "#d7191c")) +
  scale_size_continuous(range = c(2, 10)) +
  labs(title = "Emerging vs Declining Unigrams",
       subtitle = paste0("Early: first ", length(early_cases), " docs",
                         "  |  Late: last ", length(late_cases), " docs"),
       x = "Share in early periods", y = "Share in late periods",
       size = "Absolute lift", color = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_emerging_unigrams.png"), p_emerge, width = 9, height = 7)

# 7e. Emerging vs declining bigrams (bubble plot)
p_emerge_bi <- emerging_bi %>%
  slice_head(n = 30) %>%
  bind_rows(slice_tail(emerging_bi, n = 15)) %>%
  mutate(direction = ifelse(growth > 0, "Emerging", "Declining")) %>%
  ggplot(aes(share_early, share_late, size = abs_lift, color = direction,
             label = bigram)) +
  geom_point(alpha = 0.7) +
  geom_abline(linetype = "dashed", color = "grey50") +
  geom_text_repel(size = 3, max.overlaps = 20) +
  scale_color_manual(values = c("Emerging" = "#1a9641", "Declining" = "#d7191c")) +
  scale_size_continuous(range = c(2, 10)) +
  labs(title = "Emerging vs Declining Bigrams",
       subtitle = paste0("Early: first ", length(early_cases), " docs",
                         "  |  Late: last ", length(late_cases), " docs"),
       x = "Share in early periods", y = "Share in late periods",
       size = "Absolute lift", color = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_emerging_bigrams.png"), p_emerge_bi, width = 9, height = 7)

# 7f. Emerging vs declining trigrams (bubble plot)
p_emerge_tri <- emerging_tri %>%
  slice_head(n = 30) %>%
  bind_rows(slice_tail(emerging_tri, n = 15)) %>%
  mutate(direction = ifelse(growth > 0, "Emerging", "Declining")) %>%
  ggplot(aes(share_early, share_late, size = abs_lift, color = direction,
             label = trigram)) +
  geom_point(alpha = 0.7) +
  geom_abline(linetype = "dashed", color = "grey50") +
  geom_text_repel(size = 3, max.overlaps = 20) +
  scale_color_manual(values = c("Emerging" = "#1a9641", "Declining" = "#d7191c")) +
  scale_size_continuous(range = c(2, 10)) +
  labs(title = "Emerging vs Declining Trigrams",
       subtitle = paste0("Early: first ", length(early_cases), " docs",
                         "  |  Late: last ", length(late_cases), " docs"),
       x = "Share in early periods", y = "Share in late periods",
       size = "Absolute lift", color = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_emerging_trigrams.png"), p_emerge_tri, width = 9, height = 7)

# 7g. Top TF-IDF unigram per period (faceted bar)
p_tfidf <- tfidf_uni %>%
  group_by(period) %>%
  slice_max(tf_idf, n = 8) %>%
  ungroup() %>%
  mutate(word = reorder_within(word, tf_idf, period)) %>%
  ggplot(aes(tf_idf, word, fill = period)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~period, scales = "free_y", ncol = 2) +
  scale_y_reordered() +
  labs(title = "Highest TF-IDF Unigrams by Quarter",
       x = "TF-IDF", y = NULL) +
  theme_minimal(base_size = 10)
ggsave(paste0(img_dir, "/plot_tfidf_by_period.png"), p_tfidf,
       width = 10, height = 3 * ceiling(n_periods / 2))

cat("\nDone. Plots saved to:", out_dir, "\n")

# ------------------------------------------------------------------------------
# 8. Save results
# ------------------------------------------------------------------------------
saveRDS(
  list(
    top_uni      = top_uni,
    top_bi       = top_bi,
    top_tri      = top_tri,
    emerging_uni = emerging_uni,
    emerging_bi  = emerging_bi,
    emerging_tri = emerging_tri,
    tfidf_uni    = tfidf_uni,
    tfidf_bi     = tfidf_bi
  ),
  file = paste0(out_dir, "/text_analytics_jo.rds")
)
cat("Results saved to text_analytics_jo.rds\n")
