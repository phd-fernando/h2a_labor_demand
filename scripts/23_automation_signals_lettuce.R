###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Automation adoption signals in LETTUCE H-2A job orders.
# Identical three-step methodology as script 18 (strawberry).
# Only the crop filter and focal-crop stopwords differ.
#
# Lettuce filter:
#   farmer-entered cropActivity contains "lettuce" or "romaine"
#   OR duties text contains "lettuce" or "romaine"
# Note: leafy greens (romaine, head, iceberg, butter, red/green leaf) are a
# primary target of mechanized harvesting in CA/AZ (Salinas Valley, Yuma).
# Over-the-row harvesters and robotic thinning systems are actively deployed.
#
# See notes/18_automation_filtering.txt for full methodology rationale.
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "tidytext", "ggrepel", "lubridate", "scales", "patchwork")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# 0. CROP PARAMETERS  — only this block differs from script 18
# ==============================================================================
CROP_NAME    <- "Lettuce"
CROP_PATTERN <- "lettuce|romaine"
CROP_WORDS   <- c("lettuce","romaine","iceberg","leafy","greens","arugula",
                  "spinach","endive","escarole","radicchio","mesclun")
SLUG         <- "lettuce"   # used in output filenames

# ==============================================================================
# 0b. LOAD DATA & BUILD CORPUS
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
  mutate(text_clean = str_remove(addmcSectionDetails, addc_prefix) |> str_squish(),
         is_see     = str_detect(tolower(text_clean), "^see addendum c")) |>
  group_by(caseNumber) |>
  summarise(
    duties_pdf = if_else(
      any(!is_see & nchar(text_clean) > 5),
      paste(text_clean[!is_see & nchar(text_clean) > 5], collapse = " "),
      paste(text_clean[nchar(text_clean) > 5], collapse = " ")
    ) |> str_squish(), .groups = "drop"
  ) |> mutate(duties_pdf = na_if(duties_pdf, ""))

farmer_crop <- df_crops |>
  filter(!is.na(cropActivity),
         str_detect(tolower(cropActivity), CROP_PATTERN)) |>
  distinct(caseNumber) |>
  mutate(farmer_crop = TRUE)

df_raw <- enr |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf)) |>
  left_join(farmer_crop, by = "caseNumber") |>
  mutate(is_crop = coalesce(farmer_crop, FALSE) |
           str_detect(tolower(coalesce(duties, "")), CROP_PATTERN)) |>
  filter(is_crop,
         !is.na(duties), nchar(trimws(duties)) > 20,
         !is.na(empFein), !is.na(dateSubmitted),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30")) |>
  mutate(year     = year(dateSubmitted),
         emp_year = paste0(empFein, "_", year)) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year, duties,
         jobState, jobWrksNeeded)

cat(sprintf("%s corpus: %d JOs from %d employers\n\n",
    CROP_NAME, nrow(df_raw), n_distinct(df_raw$empFein)))

# Split to sentences
df_sentences <- df_raw |>
  separate_rows(duties, sep = "(?<=[.!?;\\n])\\s+") |>
  mutate(sentence = str_squish(duties)) |>
  filter(nchar(sentence) > 15) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year,
         jobState, jobWrksNeeded, sentence)

# ==============================================================================
# STEP 1 — DIRECT MENTION SEARCH
# (vocabulary same as script 18 — crop-agnostic mechanization language)
# ==============================================================================
auto_vocab <- tribble(
  ~pattern,                                      ~tier, ~label,
  "\\brobot\\b|\\brobotic",                      "A",   "robotic",
  "\\bautonomous\\b",                            "A",   "autonomous",
  "mechanical.*harvest|harvest.*mechanical",     "B",   "mechanical harvest",
  "machine.*harvest|harvest.*machine",           "B",   "machine harvest",
  "harvest(ing)? assist",                        "B",   "harvest assist",
  "harvest(ing)? platform",                      "B",   "harvest platform",
  "\\bconveyor\\b",                              "B",   "conveyor",
  "self.propelled",                              "B",   "self-propelled",
  "\\bautomated\\b",                             "B",   "automated",
  "\\bGPS\\b|\\bgps.guid",                       "C",   "GPS",
  "\\bdrone\\b|\\bUAV\\b",                       "C",   "drone/UAV",
  "\\bsensor\\b",                                "C",   "sensor",
  "precision.agricult|precision.farm",           "C",   "precision ag",
  "electronic.record|digital.record",            "C",   "electronic records"
)

auto_cols_A <- c("auto_robotic", "auto_autonomous")
auto_cols_B <- c("auto_mechanical_harvest","auto_machine_harvest",
                 "auto_harvest_assist","auto_harvest_platform",
                 "auto_conveyor","auto_self_propelled","auto_automated")
auto_cols_C <- c("auto_GPS","auto_drone_UAV","auto_sensor",
                 "auto_precision_ag","auto_electronic_records")

mentions <- df_raw |> select(caseNumber, empFein, dateSubmitted, year, jobWrksNeeded, duties)

for (i in seq_len(nrow(auto_vocab))) {
  col <- paste0("auto_", gsub("[^a-z0-9]", "_", auto_vocab$label[i]))
  mentions[[col]] <- str_detect(tolower(mentions$duties), auto_vocab$pattern[i])
}

all_auto_cols <- names(mentions)[str_detect(names(mentions), "^auto_")]
mentions <- mentions |>
  mutate(
    any_auto  = rowSums(across(all_of(all_auto_cols))) > 0,
    any_tierA = rowSums(across(any_of(auto_cols_A))) > 0,
    any_tierB = rowSums(across(any_of(auto_cols_B))) > 0,
    any_tierC = rowSums(across(any_of(auto_cols_C))) > 0
  )

cat(sprintf("=== STEP 1: Direct automation mentions — %s ===\n", CROP_NAME))
cat(sprintf("JOs with any automation term:  %d / %d  (%.1f%%)\n",
    sum(mentions$any_auto), nrow(mentions), mean(mentions$any_auto)*100))
cat(sprintf("  Tier A (robotic/autonomous): %d (%.1f%%)\n",
    sum(mentions$any_tierA), mean(mentions$any_tierA)*100))
cat(sprintf("  Tier B (mechanization):      %d (%.1f%%)\n",
    sum(mentions$any_tierB), mean(mentions$any_tierB)*100))
cat(sprintf("  Tier C (precision ag):       %d (%.1f%%)\n\n",
    sum(mentions$any_tierC), mean(mentions$any_tierC)*100))

cat("Term-by-term counts:\n")
mentions |>
  summarise(across(all_of(all_auto_cols), sum)) |>
  tidyr::pivot_longer(everything(), names_to="term", values_to="n") |>
  mutate(term = str_remove(term, "^auto_"),
         pct  = round(n / nrow(mentions) * 100, 2)) |>
  filter(n > 0) |> arrange(desc(n)) |> print()

cat(sprintf("\n=== Automation mentions by year — %s ===\n", CROP_NAME))
mentions |>
  group_by(year) |>
  summarise(n_jos   = n(),
            n_any   = sum(any_auto),
            n_tierA = sum(any_tierA),
            n_tierB = sum(any_tierB),
            n_tierC = sum(any_tierC),
            pct_any = round(mean(any_auto)*100, 1),
            workers = sum(jobWrksNeeded, na.rm=TRUE),
            .groups = "drop") |>
  print()

auto_cases <- mentions |> filter(any_auto) |> pull(caseNumber)
cat(sprintf("\n%d JOs with direct automation mention — used in Step 3\n\n",
    length(auto_cases)))

p1 <- mentions |>
  group_by(year) |>
  summarise(pct_tierA = mean(any_tierA)*100,
            pct_tierB = mean(any_tierB)*100,
            pct_tierC = mean(any_tierC)*100,
            n = n(), .groups = "drop") |>
  filter(year >= 2020, year <= 2025) |>
  tidyr::pivot_longer(starts_with("pct_"), names_to="tier", values_to="pct") |>
  mutate(tier = recode(tier,
    pct_tierA = "Tier A: Robotic / autonomous",
    pct_tierB = "Tier B: Mechanized harvest",
    pct_tierC = "Tier C: Precision agriculture")) |>
  ggplot(aes(x=year, y=pct, color=tier, group=tier)) +
  geom_line(linewidth=1) + geom_point(size=2.5) +
  scale_color_manual(values=c("Tier A: Robotic / autonomous"="#d73027",
                               "Tier B: Mechanized harvest"  ="#f46d43",
                               "Tier C: Precision agriculture"="#4575b4"),
                     name=NULL) +
  scale_y_continuous(labels=function(x) paste0(x,"%")) +
  labs(title    = sprintf("Step 1: Direct Automation Mentions — %s H-2A Orders", CROP_NAME),
       subtitle = "% of job orders containing each tier of automation vocabulary",
       x=NULL, y="% of JOs",
       caption  = "Source: DOL H-2A job orders. See notes/18_automation_filtering.txt.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="top",
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# STEP 2 — TASK DISPLACEMENT SIGNAL
# ==============================================================================
ag_verb_pattern <- paste0(
  "harvest|pick|plant|transplant|cultivat|till|bed|weed|hoe|thin|",
  "prune|trim|train|trellis|tie|irrigat|fertil|spray|apply|",
  "mulch|mow|sow|seed|sort|grade|pack|box|load|unload|wash|cool|",
  "wrap|weigh|label|stack|bin|inspect|operat|driv|attach|calibrat|",
  "adjust|hand.pick|hand.harvest|place|carry|cut|pull|dig|",
  "remov|stake|cover|clip|propagat|discard|monitor|repair|clean"
)

tc_stopwords <- tibble(word = c(
  "wage","wages","rate","rates","pay","paid","payment","compensation",
  "adverse","effect","prevailing","hourly","piece","piecework","overtime",
  "housing","house","provided","transportation","transport","vehicle",
  "vehicles","lodging","meals","meal","subsistence",
  "drug","drugs","alcohol","testing","test","criminal","background",
  "check","policy","policies","zero","tolerance","illegal","substances",
  "prohibited","termination","terminated","disciplinary","discipline",
  "form","eta","cfr","omb","burden","statement","public","paperwork",
  "reduction","act","section","paragraph","addendum","exhibit",
  "supervisor","supervision","supervisory","instructions","instruction",
  "directed","direction","employer","employee","workers","worker",
  "foreman","crew","crews","management","manager","personnel",
  "hours","hour","daily","weekly","schedule","scheduled","period",
  "employment","duration","contract","season","seasonal",
  "safety","safe","ppe","protective","gloves","boots","personal",
  "required","requirements","requirement","mandatory",
  "including","related","various","general","duties","tasks","activities",
  "job","assigned","assignment","performed","performance","following",
  "listed","list","describe","description","continued","continuation",
  "able","ability","physical","physically","must","shall","will","may",
  "also","well","however","therefore","thus","upon",
  "de","la","el","en","los","las","del","con","para","por","que","se",
  "trabajadores","trabajador",
  CROP_WORDS   # focal crop words removed (Group C)
))
stop_words_all <- bind_rows(stop_words, tc_stopwords) |> distinct(word)

verb_anchor <- paste0(
  "^harvest|^pick|^plant|^transplant|^cultivat|^till|^bed|^weed|^hoe|",
  "^thin|^prune|^trim|^train|^trellis|^tie|^irrig|^fertil|^spray|^apply|",
  "^mulch|^mow|^sow|^seed|^sort|^grade|^pack|^box|^load|^unload|^wash|",
  "^cool|^wrap|^weigh|^label|^stack|^bin|^inspect|^operat|^driv|^attach|",
  "^calibrat|^adjust|^carry|^cut|^pull|^dig|^remov|^stake|^cover|^clip|",
  "^propagat|^place|^repair|^clean|^discard|^monitor|^scout|^sample|",
  "^record|^measur|^count|^flag|^sucker|^debud|^set"
)

hand_pattern    <- "^hand$|^manually$|^manual$|^handpick|^handharv"
machine_pattern <- paste0("^operat|^driv|^attach|^calibrat|^adjust|^monitor|",
                           "^load|^unload|machine|mechanic|conveyor|platform|",
                           "^sensor|autonomous|robotic|automated|self.propel")
quality_pattern <- "^inspect|^grade|^sort|^discard|^cull|^reject|^weigh|^pack"

df_ag_sentences <- df_sentences |>
  filter(str_detect(tolower(sentence), ag_verb_pattern))

tokens_bi_all <- df_ag_sentences |>
  unnest_tokens(bigram, sentence, token="ngrams", n=2) |>
  separate(bigram, into=c("w1","w2"), sep=" ", remove=FALSE) |>
  filter(!w1 %in% stop_words_all$word,
         !w2 %in% stop_words_all$word,
         str_detect(w1, "^[a-z]+$"),
         str_detect(w2, "^[a-z]+$"),
         str_detect(w1, verb_anchor) | str_detect(w2, verb_anchor)) |>
  mutate(mode = case_when(
    str_detect(w1, hand_pattern) | str_detect(w2, hand_pattern)       ~ "hand-task",
    str_detect(w1, machine_pattern) | str_detect(w2, machine_pattern) ~ "machine-op",
    str_detect(w1, quality_pattern) | str_detect(w2, quality_pattern) ~ "quality-ctrl",
    TRUE                                                                ~ "other-ag"
  ))

cat(sprintf("=== STEP 2: Task mode distribution — %s ===\n", CROP_NAME))
tokens_bi_all |>
  distinct(caseNumber, bigram, mode) |>
  count(mode, sort=TRUE) |>
  mutate(pct=round(n/sum(n)*100,1)) |> print()

annual_mode <- tokens_bi_all |>
  distinct(emp_year, bigram, mode) |>
  left_join(df_raw |> distinct(emp_year, year), by="emp_year") |>
  group_by(year, mode) |> summarise(n=n(), .groups="drop") |>
  group_by(year) |> mutate(share=n/sum(n)*100) |> ungroup()

cat(sprintf("\n=== Annual mode shares (%%) — %s ===\n", CROP_NAME))
annual_mode |>
  select(year, mode, share) |>
  tidyr::pivot_wider(names_from=mode, values_from=share, values_fill=0) |>
  mutate(across(where(is.numeric), ~round(.,1))) |>
  print()

ratio_df <- annual_mode |>
  filter(mode %in% c("hand-task","machine-op")) |>
  select(year, mode, share) |>
  tidyr::pivot_wider(names_from=mode, values_from=share, values_fill=0) |>
  mutate(hand_machine_ratio = `hand-task` / (`machine-op` + 0.01)) |>
  filter(year >= 2020, year <= 2025)

cat(sprintf("\n=== Hand-task / machine-op ratio — %s ===\n", CROP_NAME))
print(ratio_df)

cat(sprintf("\nTop 20 HAND-TASK bigrams — %s:\n", CROP_NAME))
tokens_bi_all |> filter(mode=="hand-task") |>
  distinct(emp_year, bigram) |> count(bigram, sort=TRUE) |> slice_head(n=20) |> print()

cat(sprintf("\nTop 20 MACHINE-OP bigrams — %s:\n", CROP_NAME))
tokens_bi_all |> filter(mode=="machine-op") |>
  distinct(emp_year, bigram) |> count(bigram, sort=TRUE) |> slice_head(n=20) |> print()

p2a <- annual_mode |>
  filter(year >= 2020, year <= 2025) |>
  mutate(mode = factor(mode, levels=c("hand-task","quality-ctrl","machine-op","other-ag"))) |>
  ggplot(aes(x=year, y=share, color=mode, group=mode)) +
  geom_line(linewidth=1) + geom_point(size=2.5) +
  scale_color_manual(values=c("hand-task"="#d73027","machine-op"="#1a9641",
                               "quality-ctrl"="#4575b4","other-ag"="#bababa"), name=NULL) +
  scale_y_continuous(labels=function(x) paste0(round(x),"%")) +
  labs(title=sprintf("Step 2a: Task Mode Shares — %s H-2A", CROP_NAME),
       subtitle="Employer-year deduplicated verb-anchored bigrams",
       x=NULL, y="Share of bigrams", caption="") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=11),
        plot.subtitle=element_text(colour="grey40",size=8),
        legend.position="top",
        plot.background=element_rect(fill="white",colour=NA))

p2b <- ratio_df |>
  ggplot(aes(x=year, y=hand_machine_ratio)) +
  geom_line(linewidth=1, color="#d73027") + geom_point(size=3, color="#d73027") +
  geom_hline(yintercept=1, linetype="dashed", color="grey50") +
  labs(title="Step 2b: Hand-task / Machine-op Ratio",
       subtitle="Ratio > 1 = hand-dominant; falling ratio = mechanization signal",
       x=NULL, y="Ratio", caption="") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=11),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# STEP 3 — SEMANTIC NEIGHBORHOOD EXPANSION
# ==============================================================================
auto_keyword_pattern <- paste0(auto_vocab$pattern, collapse="|")

auto_sentences <- df_sentences |>
  filter(caseNumber %in% auto_cases,
         str_detect(tolower(sentence), auto_keyword_pattern),
         str_detect(tolower(sentence), ag_verb_pattern))

control_sentences <- df_sentences |>
  filter(!caseNumber %in% auto_cases,
         str_detect(tolower(sentence), ag_verb_pattern))

cat(sprintf("\n=== STEP 3: Semantic neighborhood — %s ===\n", CROP_NAME))
cat(sprintf("Automation-keyword sentences: %d (from %d JOs)\n",
    nrow(auto_sentences), n_distinct(auto_sentences$caseNumber)))
cat(sprintf("Control ag sentences:         %d (from %d JOs)\n\n",
    nrow(control_sentences), n_distinct(control_sentences$caseNumber)))

extract_verb_bigrams <- function(df_sent) {
  df_sent |>
    unnest_tokens(bigram, sentence, token="ngrams", n=2) |>
    separate(bigram, into=c("w1","w2"), sep=" ", remove=FALSE) |>
    filter(!w1 %in% stop_words_all$word,
           !w2 %in% stop_words_all$word,
           str_detect(w1, "^[a-z]+$"),
           str_detect(w2, "^[a-z]+$"),
           str_detect(w1, verb_anchor) | str_detect(w2, verb_anchor),
           !str_detect(w1, "robot|autonom|mechanic|machine|conveyor|sensor|gps|drone"),
           !str_detect(w2, "robot|autonom|mechanic|machine|conveyor|sensor|gps|drone")) |>
    count(bigram, sort=TRUE)
}

n_auto_sent    <- nrow(auto_sentences)
n_control_sent <- nrow(control_sentences)

auto_bi    <- extract_verb_bigrams(auto_sentences)    |> rename(n_auto=n)
control_bi <- extract_verb_bigrams(control_sentences) |> rename(n_ctrl=n)

neighbor_df <- auto_bi |>
  full_join(control_bi, by="bigram") |>
  mutate(n_auto     = coalesce(n_auto, 0L),
         n_ctrl     = coalesce(n_ctrl, 0L),
         share_auto = n_auto / n_auto_sent,
         share_ctrl = n_ctrl / n_control_sent,
         lift       = (share_auto + 1e-6) / (share_ctrl + 1e-6),
         log2_lift  = log2(lift)) |>
  filter(n_auto >= 3) |>
  arrange(desc(lift))

cat(sprintf("=== Top 30 bigrams enriched in automation context — %s ===\n", CROP_NAME))
neighbor_df |>
  select(bigram, n_auto, n_ctrl, share_auto, share_ctrl, lift) |>
  mutate(share_auto=round(share_auto,3), share_ctrl=round(share_ctrl,3),
         lift=round(lift,2)) |>
  slice_head(n=30) |> as.data.frame() |> print()

p3 <- neighbor_df |>
  slice_head(n=25) |>
  mutate(bigram=fct_reorder(bigram, log2_lift)) |>
  ggplot(aes(x=log2_lift, y=bigram)) +
  geom_segment(aes(x=0, xend=log2_lift, y=bigram, yend=bigram),
               color="#f4a520", linewidth=0.7) +
  geom_point(color="#f4a520", size=3) +
  geom_vline(xintercept=0, color="grey40", linewidth=0.4) +
  labs(title=sprintf("Step 3: Bigrams Enriched in Automation-Sentence Context — %s", CROP_NAME),
       subtitle=sprintf("Top 25 verb-anchored bigrams more common when automation keywords appear  |  n=%d automation sentences",
                        n_auto_sent),
       x="Log₂ lift (auto context vs non-auto JOs)", y=NULL,
       caption=paste0("Source: DOL H-2A job orders.\n",
                      "Lift = share in auto sentences / share in control sentences. ",
                      "Min 3 occurrences in auto context.")) +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.caption=element_text(colour="grey50",size=7.5),
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# COMBINED FIGURE & SAVE
# ==============================================================================
p_combined <- (p1 / (p2a | p2b) / p3) +
  plot_annotation(
    title    = sprintf("Automation Signals in %s H-2A Job Orders (2020–2025)", CROP_NAME),
    subtitle = "Three-step detection: direct mentions → task displacement → semantic neighbors",
    caption  = "Source: DOL H-2A job orders (JSON + gap PDFs + OFLC disclosure).",
    theme    = theme(plot.title=element_text(face="bold",size=14),
                     plot.subtitle=element_text(colour="grey40",size=9),
                     plot.caption=element_text(colour="grey50",size=7.5),
                     plot.background=element_rect(fill="white",colour=NA))
  )

ggsave(sprintf("processed/images/plot_%s_automation_combined.png", SLUG),
       p_combined, width=14, height=18, dpi=180, bg="white")
ggsave(sprintf("processed/images/plot_%s_automation_step1.png", SLUG),
       p1, width=10, height=5, dpi=180, bg="white")
ggsave(sprintf("processed/images/plot_%s_automation_step2.png", SLUG),
       p2a / p2b, width=10, height=8, dpi=180, bg="white")
ggsave(sprintf("processed/images/plot_%s_automation_step3.png", SLUG),
       p3, width=10, height=7, dpi=180, bg="white")
cat(sprintf("Saved: plot_%s_automation_*.png\n", SLUG))

saveRDS(
  list(crop=CROP_NAME, auto_cases=auto_cases, mentions=mentions,
       annual_mode=annual_mode, ratio_df=ratio_df, neighbor_df=neighbor_df),
  sprintf("processed/text/automation_signals_%s.rds", SLUG)
)
cat(sprintf("Saved: automation_signals_%s.rds\nAll done.\n", SLUG))
