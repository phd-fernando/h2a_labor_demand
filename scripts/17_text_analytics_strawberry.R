###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Emerging / disappearing bigrams of farm practices —
# restricted to strawberry job orders only.
#
# Three-layer cleaning pipeline to isolate FARM PRACTICE language:
#   Layer 1 — Sentence filter    : keep only sentences with ≥1 ag action verb
#   Layer 2 — Stopword filter    : remove T&C vocabulary at the token level
#   Layer 3 — Employer-year dedup: count bigrams once per employer-year,
#                                  not once per JO (kills template inflation)
#
# See notes/17_filtering_decisions.txt for full rationale on each decision.
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "tidytext", "ggrepel", "lubridate", "scales")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# 1. BUILD STRAWBERRY CORPUS
# ==============================================================================
enr      <- readRDS("processed/text/df_jobs_enriched.rds")
txt_full <- readRDS("processed/text/df_text_gap_full.rds")
df_crops <- readRDS("processed/text/df_crops.rds")

addc_prefix <- regex(
  paste0("^3[.]\\s*Details\\s+of\\s+Material\\s+Term\\s+or\\s+Condition",
         "\\s*[(]up\\s+to\\s+3[,.]500\\s+characters?[)]\\s*"),
  ignore_case = TRUE
)

duties_tbl <- txt_full |>
  filter(addmcSectionNumber == "A.8a") |>
  mutate(text_clean  = str_remove(addmcSectionDetails, addc_prefix) |> str_squish(),
         is_see      = str_detect(tolower(text_clean), "^see addendum c")) |>
  group_by(caseNumber) |>
  summarise(
    duties_pdf = if_else(
      any(!is_see & nchar(text_clean) > 5),
      paste(text_clean[!is_see & nchar(text_clean) > 5], collapse = " "),
      paste(text_clean[nchar(text_clean) > 5], collapse = " ")
    ) |> str_squish(),
    .groups = "drop"
  ) |>
  mutate(duties_pdf = na_if(duties_pdf, ""))

farmer_strawberry <- df_crops |>
  filter(!is.na(cropActivity), str_detect(tolower(cropActivity), "strawberr")) |>
  distinct(caseNumber) |>
  mutate(farmer_strawberry = TRUE)

df_raw <- enr |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf)) |>
  left_join(farmer_strawberry, by = "caseNumber") |>
  mutate(is_strawberry = coalesce(farmer_strawberry, FALSE) |
           str_detect(tolower(coalesce(duties, "")), "strawberr")) |>
  filter(is_strawberry,
         !is.na(duties), nchar(trimws(duties)) > 20,
         !is.na(empFein),
         !is.na(dateSubmitted),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30")) |>
  mutate(year = year(dateSubmitted),
         emp_year = paste0(empFein, "_", year)) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year, duties)

cat(sprintf("Raw strawberry corpus: %d JOs from %d employers\n\n",
    nrow(df_raw), n_distinct(df_raw$empFein)))

# ==============================================================================
# LAYER 1 — SENTENCE FILTER
# Keep only sentences that contain at least one agricultural action verb.
#
# RATIONALE (see notes/17_filtering_decisions.txt §1):
#   H-2A duties text mixes two types of content in the same field:
#     (a) Farm practice sentences: "Workers will harvest ripe strawberries
#         by hand, place in flats, and carry to the field cart."
#     (b) T&C sentences: "Workers must submit to drug testing. Housing
#         is provided. The adverse effect wage rate applies."
#   Sentence-level filtering retains (a) and discards (b) before tokenizing,
#   so downstream bigrams reflect physical task language only.
#
# AG VERB WHITELIST — stems (matched with str_detect, so "harvest" matches
#   "harvesting", "harvested", etc.):
#   Field operations  : harvest, pick, plant, transplant, cultivat, till, bed,
#                       weed, hoe, thin, prune, trim, train, trellis, tie, string,
#                       irrigat, fertil, spray, apply, mulch, mow, sow, seed
#   Packinghouse ops  : sort, grade, pack, box, load, unload, wash, cool, wrap,
#                       weigh, label, stack, bin, inspect
#   Equipment ops     : operat, drive, attach, calibrat, adjust
#   General ag tasks  : hand pick, hand harvest, place, carry, cut, pull, dig,
#                       remove, stake, cover, uncover, clip
#
# EXCLUDED VERBS (intentionally NOT in whitelist):
#   report, attend, sign, receive, notify, submit, comply, test, background,
#   provide, ensure, maintain (as in "maintain records"), follow (as in rules)
#   — these appear almost exclusively in T&C sentences.
# ==============================================================================

ag_verb_pattern <- paste0(
  "harvest|pick|plant|transplant|cultivat|till|bed|weed|hoe|thin|",
  "prune|trim|train|trellis|tie|string|irrigat|fertil|spray|apply|",
  "mulch|mow|sow|seed|sort|grade|pack|box|load|unload|wash|cool|",
  "wrap|weigh|label|stack|bin|inspect|operat|driv|attach|calibrat|",
  "adjust|hand.pick|hand.harvest|place.*flat|carry|cut|pull|dig|",
  "remov|stake|cover|clip|propagat|disbud|sucker|debud|thin|floor"
)

# Split duties into sentences, filter to ag-verb sentences, reassemble per JO
df_sentences <- df_raw |>
  mutate(sentence_id = row_number()) |>
  separate_rows(duties, sep = "(?<=[.!?;\\n])\\s+") |>
  mutate(duties = str_squish(duties)) |>
  filter(nchar(duties) > 15,
         str_detect(tolower(duties), ag_verb_pattern))

# Reassemble filtered sentences per JO
df_filtered <- df_sentences |>
  group_by(caseNumber, empFein, emp_year, dateSubmitted, year) |>
  summarise(text_ag = paste(duties, collapse = " "), .groups = "drop") |>
  filter(nchar(text_ag) > 30)

cat(sprintf("After Layer 1 (sentence filter): %d JOs retained (%.1f%%)\n",
    nrow(df_filtered), nrow(df_filtered)/nrow(df_raw)*100))
cat(sprintf("Median ag-sentence text length: %d chars\n\n",
    median(nchar(df_filtered$text_ag))))

# ==============================================================================
# LAYER 2 — STOPWORD FILTER
# Remove T&C vocabulary at the token level after bigram construction.
#
# RATIONALE (see notes/17_filtering_decisions.txt §2):
#   Even after sentence filtering, residual T&C words survive inside
#   otherwise-agricultural sentences (e.g. "workers must harvest and
#   comply with food safety standards"). Token-level stopwords catch these.
#
# Three stopword groups:
#   (A) Standard English function words  — from tidytext::stop_words
#   (B) Employment / legal / admin words — drug, housing, wage, form, etc.
#   (C) Focal crop word                  — "strawberry/strawberries" removed
#       so bigrams describe the task context, not just the crop name.
#       (e.g. "harvest berries" is more informative than "harvest strawberries"
#        which would dominate trivially)
# ==============================================================================

tc_stopwords <- tibble(word = c(
  # --- Employment / legal / administrative (Group B) ---
  # Wages & compensation
  "wage","wages","rate","rates","pay","paid","payment","compensation",
  "adverse","effect","prevailing","hourly","piece","piecework","overtime",
  # Housing & transport
  "housing","house","housing","provided","transportation","transport",
  "vehicle","vehicles","lodging","meals","meal","subsistence",
  # Compliance / legal
  "drug","drugs","alcohol","testing","test","criminal","background",
  "check","policy","policies","zero","tolerance","illegal","substances",
  "prohibited","termination","terminated","disciplinary","discipline",
  # Forms / paperwork
  "form","eta","cfr","omb","burden","statement","public","paperwork",
  "reduction","act","section","paragraph","addendum","exhibit",
  # Supervisory / HR boilerplate
  "supervisor","supervision","supervisory","instructions","instruction",
  "directed","direction","employer","employee","workers","worker",
  "foreman","crew","crews","management","manager","personnel",
  # Scheduling / time boilerplate
  "hours","hour","daily","weekly","schedule","scheduled","period",
  "employment","duration","contract","season","seasonal",
  # Safety boilerplate
  "safety","safe","ppe","protective","equipment","gloves","boots",
  "personal","required","requirements","requirement","mandatory",
  # General admin filler
  "including","related","various","general","duties","tasks","activities",
  "job","assigned","assignment","performed","performance","following",
  "listed","list","describe","description","continued","continuation",
  "able","ability","physical","physically","must","shall","will","may",
  "also","including","well","however","therefore","thus","upon",
  # Spanish filler (appears in bilingual boilerplate)
  "de","la","el","en","los","las","del","con","para","por","que","se",
  "trabajadores","trabajador","los","las",
  # --- Focal crop removal (Group C) ---
  "strawberry","strawberries","berry","berries"
))

stop_words_all <- bind_rows(stop_words, tc_stopwords) |> distinct(word)

# ==============================================================================
# LAYER 3 — EMPLOYER-YEAR DEDUPLICATION
# Count each bigram at most once per employer-year, not once per JO.
#
# RATIONALE (see notes/17_filtering_decisions.txt §3):
#   Problem: a farm labor association distributes a standard duties template
#   to 200 member farms. All 200 file JOs using identical language. Without
#   deduplication, that template's bigrams appear 200× and dominate the
#   frequency table — and when the association updates its template, those
#   bigrams "emerge" or "decline" en masse, masking genuine task changes.
#
#   Fix: for each (empFein × year) pair, count a bigram as present (1) if
#   it appears in ANY JO from that employer that year. Then aggregate across
#   all employer-years. This makes the unit of observation the employer-year,
#   not the job order.
#
#   Consequence: an employer filing 10 JOs with the same text contributes
#   the same weight as an employer filing 1 JO. Small employers are not
#   disadvantaged relative to large ones in terms of vocabulary contribution.
# ==============================================================================

# ==============================================================================
# LAYER 4 — VERB-ANCHOR FILTER
# Keep only bigrams where at least one word is an agricultural action verb stem.
#
# RATIONALE (see notes/17_filtering_decisions.txt §4):
#   Layer 1 filters at the sentence level: it keeps sentences that CONTAIN an
#   ag verb, but then ALL bigrams in that sentence are extracted — including
#   noun-noun pairs like "eggplant peppers" or "ragweed goldenrod" that happen
#   to sit next to a verb elsewhere in the sentence.
#
#   Layer 4 filters at the bigram level: at least one of the two words must
#   be (or begin with) an ag action verb stem. This ensures every retained
#   bigram describes a TASK (verb + object, verb + modifier, modifier + verb)
#   rather than a list of nouns.
#
# VERB STEM WHITELIST (same roots as Layer 1, matched with str_detect):
#   harvest, pick, plant, transplant, cultivat, till, bed, weed, hoe, thin,
#   prune, trim, train, trellis, tie, irrig, fertil, spray, apply, mulch,
#   mow, sow, seed, sort, grade, pack, box, load, unload, wash, cool, wrap,
#   weigh, label, stack, bin, inspect, operat, driv, attach, calibrat, adjust,
#   carry, cut, pull, dig, remov, stake, cover, clip, propagat, place, repair,
#   clean, discard, monitor, scout, sample, record, measure, count, flag
#
# NOTE: "clean" and "repair" are included here (not in Layer 1 exclusion list)
#   because they can anchor legitimate task bigrams ("clean drip", "repair
#   irrigation") even though "cleaning" in isolation often appears in T&C
#   sentences. The verb-anchor requirement prevents "cleaning bathrooms" from
#   surviving only if "bathrooms" is also filtered by Layer 2 stopwords.
#
# BIGRAM TYPES RETAINED:
#   verb + noun     : "harvest ripe", "spray fungicide", "thin runners"
#   verb + adverb   : "pick carefully", "sort quickly"
#   noun + verb     : "hand pick", "hand harvest", "machine harvest"
#   verb + verb     : "harvest pack", "plant cultivate"  (task sequences)
#
# BIGRAM TYPES REJECTED:
#   noun + noun     : "eggplant peppers", "ragweed goldenrod", "sweet corn"
#   adj  + noun     : "plastic flat" — retained only if one word is a verb stem
# ==============================================================================

verb_anchor <- paste0(
  "^harvest|^pick|^plant|^transplant|^cultivat|^till|^bed|^weed|^hoe|",
  "^thin|^prune|^trim|^train|^trellis|^tie|^irrig|^fertil|^spray|^apply|",
  "^mulch|^mow|^sow|^seed|^sort|^grade|^pack|^box|^load|^unload|^wash|",
  "^cool|^wrap|^weigh|^label|^stack|^bin|^inspect|^operat|^driv|^attach|",
  "^calibrat|^adjust|^carry|^cut|^pull|^dig|^remov|^stake|^cover|^clip|",
  "^propagat|^place|^repair|^clean|^discard|^monitor|^scout|^sample|",
  "^record|^measur|^count|^flag|^sucker|^debud|^disbud|^floor|^hoe|^set"
)

# Tokenize to bigrams from ag-sentence text
tokens_bi_raw <- df_filtered |>
  select(caseNumber, empFein, emp_year, dateSubmitted, text_ag) |>
  unnest_tokens(bigram, text_ag, token = "ngrams", n = 2) |>
  separate(bigram, into = c("w1", "w2"), sep = " ", remove = FALSE) |>
  filter(!w1 %in% stop_words_all$word,
         !w2 %in% stop_words_all$word,
         str_detect(w1, "^[a-z]+$"),
         str_detect(w2, "^[a-z]+$"),
         # Layer 4: at least one word must be an ag verb stem
         str_detect(w1, verb_anchor) | str_detect(w2, verb_anchor)) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, bigram, w1, w2)

cat(sprintf("Bigram tokens after Layers 1-4 (before dedup): %d\n", nrow(tokens_bi_raw)))

tokens_bi_raw <- tokens_bi_raw |> select(-w1, -w2)

# Layer 3: deduplicate to employer-year level
tokens_bi <- tokens_bi_raw |>
  distinct(emp_year, bigram) |>          # one occurrence per employer-year
  left_join(df_filtered |> distinct(emp_year, dateSubmitted) |>
              group_by(emp_year) |> slice_min(dateSubmitted, n=1) |> ungroup(),
            by = "emp_year")             # reattach a representative date

# Also do unigrams with same pipeline
tokens_uni <- df_filtered |>
  select(caseNumber, empFein, emp_year, dateSubmitted, text_ag) |>
  unnest_tokens(word, text_ag) |>
  filter(str_detect(word, "^[a-z]+$")) |>
  anti_join(stop_words_all, by = "word") |>
  distinct(emp_year, word) |>
  left_join(df_filtered |> distinct(emp_year, dateSubmitted) |>
              group_by(emp_year) |> slice_min(dateSubmitted, n=1) |> ungroup(),
            by = "emp_year")

n_empyear <- n_distinct(df_filtered$emp_year)
cat(sprintf("Employer-years in corpus: %d\n", n_empyear))
cat(sprintf("Bigram tokens after all 4 layers: %d unique employer-year × bigram pairs\n\n",
    nrow(tokens_bi)))

cat("=== Top 30 bigrams (employer-year deduplicated) ===\n")
tokens_bi |> count(bigram, sort=TRUE) |>
  mutate(pct = round(n/n_empyear*100, 1)) |>
  slice_head(n=30) |> print()

# ==============================================================================
# 4. EARLY / LATE SPLIT  (chronological 50/50 by employer-year)
# ==============================================================================
empyear_dates <- df_filtered |>
  group_by(emp_year) |>
  summarise(first_date = min(dateSubmitted), .groups = "drop") |>
  arrange(first_date) |>
  mutate(ey_rank = row_number())

cutoff     <- floor(nrow(empyear_dates) / 2)
early_ey   <- empyear_dates |> filter(ey_rank <= cutoff) |> pull(emp_year)
late_ey    <- empyear_dates |> filter(ey_rank >  cutoff) |> pull(emp_year)

early_dates <- empyear_dates |> filter(ey_rank <= cutoff) |> pull(first_date)
late_dates  <- empyear_dates |> filter(ey_rank >  cutoff) |> pull(first_date)

cat(sprintf(
  "Early half: %d employer-years  (%s – %s)\nLate  half: %d employer-years  (%s – %s)\n\n",
  length(early_ey), min(early_dates), max(early_dates),
  length(late_ey),  min(late_dates),  max(late_dates)
))

min_n <- max(3, floor(length(early_ey) * 0.02))  # ≥2% of employer-years in each half

emerge_fn <- function(tokens_df, term_col) {
  fe <- tokens_df |> filter(emp_year %in% early_ey) |>
    count(.data[[term_col]]) |>
    mutate(share_early = n / length(early_ey)) |> rename(n_early = n)
  fl <- tokens_df |> filter(emp_year %in% late_ey) |>
    count(.data[[term_col]]) |>
    mutate(share_late  = n / length(late_ey))  |> rename(n_late  = n)
  fe |> inner_join(fl, by = term_col) |>
    mutate(growth   = (share_late - share_early) / (share_early + 1e-9),
           abs_lift = share_late - share_early) |>
    filter(n_early >= min_n, n_late >= min_n) |>
    arrange(desc(growth))
}

emerging_bi  <- emerge_fn(tokens_bi,  "bigram")
emerging_uni <- emerge_fn(tokens_uni, "word")

cat("=== Top 20 EMERGING bigrams (farm practices) ===\n")
print(as.data.frame(slice_head(emerging_bi,  n = 20)))
cat("\n=== Top 20 DECLINING bigrams (farm practices) ===\n")
print(as.data.frame(slice_tail(emerging_bi,  n = 20)))

# ==============================================================================
# 5. PLOTS
# ==============================================================================
subtitle_txt <- sprintf(
  "Strawberry H-2A job orders  |  Early: %s – %s  |  Late: %s – %s  |  Unit: employer-year",
  format(min(early_dates),"%b %Y"), format(max(early_dates),"%b %Y"),
  format(min(late_dates), "%b %Y"), format(max(late_dates), "%b %Y")
)

# 5a. Bubble plot — bigrams
plot_df_bi <- bind_rows(
  slice_head(emerging_bi, n = 30),
  slice_tail(emerging_bi, n = 20)
) |>
  distinct(bigram, .keep_all = TRUE) |>
  mutate(direction = if_else(growth > 0, "Emerging", "Declining"))

p_bi <- ggplot(plot_df_bi,
               aes(share_early, share_late, size = abs(abs_lift),
                   color = direction, label = bigram)) +
  geom_abline(linetype = "dashed", color = "grey60") +
  geom_point(alpha = 0.75) +
  geom_text_repel(size = 3, max.overlaps = 30,
                  segment.color = "grey70", segment.size = 0.3) +
  scale_color_manual(values = c("Emerging" = "#1a9641", "Declining" = "#d7191c"),
                     name = NULL) +
  scale_size_continuous(range = c(2, 10), guide = "none") +
  scale_x_continuous(labels = percent_format(accuracy = 1)) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title    = "Emerging vs Declining Farm Practice Bigrams — Strawberry H-2A Orders",
    subtitle = subtitle_txt,
    x = "Share of employer-years in early period",
    y = "Share of employer-years in late period",
    caption  = paste0(
      "Source: DOL H-2A job orders (JSON + gap PDFs + OFLC disclosure).\n",
      "Cleaning: ag-verb sentence filter + T&C stopwords + employer-year deduplication.\n",
      "Points above dashed line = more common recently; below = declining."
    )
  ) +
  theme_minimal(base_size = 11) +
  theme(plot.title      = element_text(face = "bold", size = 13),
        plot.subtitle   = element_text(colour = "grey40", size = 8),
        plot.caption    = element_text(colour = "grey50", size = 7.5),
        legend.position = "top",
        plot.background = element_rect(fill = "white", colour = NA))

ggsave("processed/images/plot_strawberry_emerging_bigrams_clean.png",
       p_bi, width = 11, height = 8, dpi = 180, bg = "white")
cat("Saved: plot_strawberry_emerging_bigrams_clean.png\n")

# 5b. Lollipop chart — top 20 emerging + top 20 declining
lollipop_df <- bind_rows(
  slice_head(emerging_bi, n = 20) |> mutate(direction = "Emerging"),
  slice_tail(emerging_bi, n = 20) |> mutate(direction = "Declining")
) |>
  mutate(bigram = fct_reorder(bigram, growth))

p_lollipop <- ggplot(lollipop_df,
                     aes(x = growth, y = bigram, color = direction)) +
  geom_segment(aes(x = 0, xend = growth, y = bigram, yend = bigram),
               linewidth = 0.6, alpha = 0.7) +
  geom_point(size = 3) +
  geom_vline(xintercept = 0, color = "grey40", linewidth = 0.4) +
  scale_color_manual(values = c("Emerging" = "#1a9641", "Declining" = "#d7191c"),
                     name = NULL) +
  scale_x_continuous(labels = function(x) paste0(round(x*100), "%")) +
  facet_wrap(~direction, scales = "free_y") +
  labs(
    title    = "Top Emerging and Declining Farm Practice Bigrams — Strawberry H-2A Orders",
    subtitle = subtitle_txt,
    x = "Growth rate (late share − early share) / early share", y = NULL,
    caption  = "Source: DOL H-2A job orders. Min support: ≥2% of employer-years in each half."
  ) +
  theme_minimal(base_size = 10) +
  theme(plot.title      = element_text(face = "bold", size = 12),
        plot.subtitle   = element_text(colour = "grey40", size = 8),
        plot.caption    = element_text(colour = "grey50", size = 7.5),
        legend.position = "none",
        strip.text      = element_text(face = "bold"),
        plot.background = element_rect(fill = "white", colour = NA))

ggsave("processed/images/plot_strawberry_bigrams_lollipop_clean.png",
       p_lollipop, width = 13, height = 9, dpi = 180, bg = "white")
cat("Saved: plot_strawberry_bigrams_lollipop_clean.png\n")

# ==============================================================================
# 6. SAVE
# ==============================================================================
saveRDS(
  list(emerging_bi  = emerging_bi,
       emerging_uni = emerging_uni,
       early_dates  = range(early_dates),
       late_dates   = range(late_dates),
       n_empyear    = n_empyear),
  "processed/text/text_analytics_strawberry_clean.rds"
)
cat("Saved: text_analytics_strawberry_clean.rds\nAll done.\n")
