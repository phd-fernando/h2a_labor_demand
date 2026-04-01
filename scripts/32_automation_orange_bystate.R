###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Script 32: Automation signals in ORANGE/CITRUS H-2A job orders, broken down
# by producing State. Same Steps 1-4 methodology as script 31 (corn).
#
# Corpus: uses 00_crop_classifier.R (two-tier primary-crop assignment).
#   Tier 1: employer-declared cropActivity field.
#   Tier 2: within-document term frequency on duties text (word-boundary regex).
#   Only JOs where citrus/orange is the PRIMARY crop are included.
#
# Citrus: 1,327 primary-crop JOs. Key producing states: FL, CA, TX, AZ.
#   H-2A citrus work is primarily hand-picking fruit, grove maintenance,
#   and post-harvest handling. Mechanization targets canopy shakers,
#   continuous harvesting platforms, and robotic picking arms.
#   FL citrus has faced severe labor and HLB disease pressure — making
#   automation adoption signals especially policy-relevant.
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse","tidytext","ggrepel","lubridate","scales","patchwork","maps")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

source("scripts/00_crop_classifier.R")

# State abbreviation → full name (lowercase, for map join)
state_abb2name <- tibble(
  jobState   = state.abb,
  state_name = tolower(state.name)
)

# ==============================================================================
# 0. CROP PARAMETERS
# ==============================================================================
CROP_KEY     <- "citrus"        # key in crop_patterns (00_crop_classifier.R)
CROP_NAME    <- "Orange/Citrus"
CROP_WORDS   <- c("orange","oranges","citrus","grapefruit","tangerine",
                   "clementine","lemon","lemons","lime","limes","grove","groves")
SLUG         <- "orange"

MIN_JOS_STATE <- 50

# ==============================================================================
# 0b. LOAD DATA & BUILD CORPUS USING PRIMARY-CROP CLASSIFIER
# ==============================================================================
enr      <- readRDS("processed/text/df_jobs_enriched.rds")
txt_full <- readRDS("processed/text/df_text_gap_full.rds")

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

# Build primary-crop map (Tier 1: cropActivity; Tier 2: duties term frequency)
duties_for_classifier <- enr |>
  left_join(duties_tbl, by = "caseNumber") |>
  transmute(caseNumber, duties = coalesce(jobDuties, duties_pdf))

crop_map <- build_crop_map("processed/text/df_crops.rds", duties_for_classifier)

# Filter to primary-crop == CROP_KEY
crop_cases <- crop_map |> filter(primary_crop == CROP_KEY) |> pull(caseNumber)
cat(sprintf("\n%s primary-crop cases: %d\n\n", CROP_NAME, length(crop_cases)))

df_raw <- enr |>
  filter(caseNumber %in% crop_cases) |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf)) |>
  filter(!is.na(duties), nchar(trimws(duties)) > 20,
         !is.na(empFein), !is.na(dateSubmitted),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30")) |>
  mutate(year     = year(dateSubmitted),
         emp_year = paste0(empFein, "_", year)) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year, duties,
         jobState, jobWrksNeeded)

cat(sprintf("%s corpus: %d JOs from %d employers across %d states\n\n",
    CROP_NAME, nrow(df_raw), n_distinct(df_raw$empFein),
    n_distinct(df_raw$jobState)))

cat("JOs by state (top 20):\n")
df_raw |> count(jobState, sort=TRUE) |> head(20) |> print()

# Split to sentences
df_sentences <- df_raw |>
  separate_rows(duties, sep = "(?<=[.!?;\\n])\\s+") |>
  mutate(sentence = str_squish(duties)) |>
  filter(nchar(sentence) > 15) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year,
         jobState, jobWrksNeeded, sentence)

# ==============================================================================
# STEP 1 — DIRECT MENTION SEARCH (national + by state)
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
  "canopy.*shaker|trunk.*shaker",                "B",   "canopy shaker",
  "harvest.*platform|picking.*platform",         "B",   "picking platform",
  "\\bGPS\\b|\\bgps.guid",                       "C",   "GPS",
  "\\bdrone\\b|\\bUAV\\b",                       "C",   "drone/UAV",
  "\\bsensor\\b",                                "C",   "sensor",
  "precision.agricult|precision.farm",           "C",   "precision ag",
  "electronic.record|digital.record",            "C",   "electronic records"
)

auto_cols_A <- c("auto_robotic", "auto_autonomous")
auto_cols_B <- c("auto_mechanical_harvest","auto_machine_harvest",
                 "auto_harvest_assist","auto_harvest_platform",
                 "auto_conveyor","auto_self_propelled","auto_automated",
                 "auto_canopy_shaker","auto_picking_platform")
auto_cols_C <- c("auto_GPS","auto_drone_UAV","auto_sensor",
                 "auto_precision_ag","auto_electronic_records")

mentions <- df_raw |> select(caseNumber, empFein, dateSubmitted, year,
                               jobState, jobWrksNeeded, duties)
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

cat(sprintf("=== STEP 1: Direct automation mentions — %s (National) ===\n", CROP_NAME))
cat(sprintf("JOs with any automation term:  %d / %d  (%.1f%%)\n",
    sum(mentions$any_auto), nrow(mentions), mean(mentions$any_auto)*100))
cat(sprintf("  Tier A (robotic/autonomous): %d (%.1f%%)\n",
    sum(mentions$any_tierA), mean(mentions$any_tierA)*100))
cat(sprintf("  Tier B (mechanization):      %d (%.1f%%)\n",
    sum(mentions$any_tierB), mean(mentions$any_tierB)*100))
cat(sprintf("  Tier C (precision ag):       %d (%.1f%%)\n\n",
    sum(mentions$any_tierC), mean(mentions$any_tierC)*100))

# --- Step 1 by state ---
step1_state <- mentions |>
  group_by(jobState) |>
  summarise(
    n_jos      = n(),
    pct_tierB  = mean(any_tierB)*100,
    pct_any    = mean(any_auto)*100,
    pct_tierC  = mean(any_tierC)*100,
    workers    = sum(jobWrksNeeded, na.rm=TRUE),
    .groups    = "drop"
  ) |>
  filter(n_jos >= MIN_JOS_STATE) |>
  arrange(desc(pct_tierB))

cat("=== Step 1 by state (Tier B mechanization rate, min 50 JOs) ===\n")
print(step1_state, n=30)

# --- Step 1 annual trend by state (top 10 states) ---
top10_states <- step1_state |> slice_head(n=10) |> pull(jobState)

step1_state_yr <- mentions |>
  filter(jobState %in% top10_states, year >= 2020, year <= 2025) |>
  group_by(jobState, year) |>
  summarise(n_jos=n(), pct_tierB=mean(any_tierB)*100, .groups="drop")

# National trend
step1_national_yr <- mentions |>
  filter(year >= 2020, year <= 2025) |>
  group_by(year) |>
  summarise(pct_tierA=mean(any_tierA)*100, pct_tierB=mean(any_tierB)*100,
            pct_tierC=mean(any_tierC)*100, n=n(), .groups="drop")

auto_cases <- mentions |> filter(any_auto) |> pull(caseNumber)
cat(sprintf("\n%d JOs with direct automation mention — used in Step 3\n\n",
    length(auto_cases)))

# ==============================================================================
# STEP 2 — TASK DISPLACEMENT SIGNAL (national + by state)
# ==============================================================================
ag_verb_pattern <- paste0(
  "harvest|pick|plant|transplant|cultivat|till|bed|weed|hoe|thin|",
  "prune|trim|train|trellis|tie|irrigat|fertil|spray|apply|",
  "mulch|mow|sow|seed|sort|grade|pack|box|load|unload|wash|cool|",
  "wrap|weigh|label|stack|bin|inspect|operat|driv|attach|calibrat|",
  "adjust|hand.pick|hand.harvest|place|carry|cut|pull|dig|",
  "remov|stake|cover|clip|propagat|discard|monitor|repair|clean|",
  "clip|snap|twist|pull|drop|bag|bucket|bin|degreen|wax|juice|press"
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
  "also","well","however","therefore","thus","upon","field","fields",
  "de","la","el","en","los","las","del","con","para","por","que","se",
  "trabajadores","trabajador",
  CROP_WORDS
))
stop_words_all <- bind_rows(stop_words, tc_stopwords) |> distinct(word)

verb_anchor <- paste0(
  "^harvest|^pick|^plant|^transplant|^cultivat|^till|^bed|^weed|^hoe|",
  "^thin|^prune|^trim|^train|^trellis|^tie|^irrig|^fertil|^spray|^apply|",
  "^mulch|^mow|^sow|^seed|^sort|^grade|^pack|^box|^load|^unload|^wash|",
  "^cool|^wrap|^weigh|^label|^stack|^bin|^inspect|^operat|^driv|^attach|",
  "^calibrat|^adjust|^carry|^cut|^pull|^dig|^remov|^stake|^cover|^clip|",
  "^propagat|^place|^repair|^clean|^discard|^monitor|^scout|^sample|",
  "^record|^measur|^count|^flag|^sucker|^debud|^set|^clip|^snap|^twist|^degreen"
)

hand_pattern    <- "^hand$|^manually$|^manual$|^handpick|^handharv"
machine_pattern <- paste0("^operat|^driv|^attach|^calibrat|^adjust|^monitor|",
                           "^load|^unload|machine|mechanic|conveyor|platform|",
                           "^sensor|autonomous|robotic|automated|self.propel|",
                           "precision.plant|gps|drone")
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

cat(sprintf("=== STEP 2: Task mode distribution — %s (National) ===\n", CROP_NAME))
tokens_bi_all |>
  distinct(caseNumber, bigram, mode) |>
  count(mode, sort=TRUE) |>
  mutate(pct=round(n/sum(n)*100,1)) |> print()

annual_mode <- tokens_bi_all |>
  distinct(emp_year, bigram, mode) |>
  left_join(df_raw |> distinct(emp_year, year), by="emp_year") |>
  group_by(year, mode) |> summarise(n=n(), .groups="drop") |>
  group_by(year) |> mutate(share=n/sum(n)*100) |> ungroup()

ratio_df <- annual_mode |>
  filter(mode %in% c("hand-task","machine-op")) |>
  select(year, mode, share) |>
  tidyr::pivot_wider(names_from=mode, values_from=share, values_fill=0) |>
  mutate(hand_machine_ratio = `hand-task` / (`machine-op` + 0.01)) |>
  filter(year >= 2020, year <= 2025)

cat(sprintf("\n=== Hand/machine ratio — %s (National) ===\n", CROP_NAME))
print(ratio_df)

# --- Step 2 by state: hand/machine ratio for 2020-2022 vs 2023-2025 ---
tokens_bi_state <- df_ag_sentences |>
  filter(jobState %in% step1_state$jobState) |>
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
  ),
  period = if_else(year <= 2022, "early_2020_22", "late_2023_25"))

step2_state <- tokens_bi_state |>
  filter(mode %in% c("hand-task","machine-op")) |>
  group_by(jobState, period, mode) |>
  summarise(n=n(), .groups="drop") |>
  group_by(jobState, period) |>
  mutate(share=n/sum(n)*100) |>
  ungroup() |>
  filter(mode=="hand-task") |>
  select(jobState, period, hand_share=share) |>
  tidyr::pivot_wider(names_from=period, values_from=hand_share, values_fill=NA) |>
  mutate(hand_share_change = late_2023_25 - early_2020_22) |>   # negative = shift toward machine
  arrange(hand_share_change)

cat("\n=== Step 2 by state: hand-task share change (2020-22 → 2023-25) ===\n")
cat("  Negative = shift toward machine-op (mechanization signal)\n")
print(step2_state, n=30)

# ==============================================================================
# STEP 3 — SEMANTIC NEIGHBORHOOD (national only; state is too sparse)
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
           !str_detect(w1, "robot|autonom|mechanic|machine|conveyor|sensor|gps|drone|shaker"),
           !str_detect(w2, "robot|autonom|mechanic|machine|conveyor|sensor|gps|drone|shaker")) |>
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
  select(bigram, n_auto, n_ctrl, lift) |>
  mutate(lift=round(lift,2)) |>
  slice_head(n=30) |> as.data.frame() |> print()

# ==============================================================================
# STEP 4 — EMPLOYER ADOPTION HETEROGENEITY
# ==============================================================================
cat(sprintf("\n=== STEP 4: Employer adoption heterogeneity — %s ===\n", CROP_NAME))

# For each employer (FEIN), classify adoption timing
employer_adoption <- mentions |>
  filter(!is.na(empFein), year >= 2019) |>
  group_by(empFein) |>
  summarise(
    n_jos         = n(),
    first_year    = min(year),
    last_year     = max(year),
    first_auto_yr = if_else(any(any_tierB), min(year[any_tierB], na.rm=TRUE), NA_real_),
    n_auto_jos    = sum(any_tierB),
    pct_auto      = mean(any_tierB)*100,
    state_mode    = names(which.max(table(jobState))),
    avg_workers   = mean(jobWrksNeeded, na.rm=TRUE),
    .groups       = "drop"
  ) |>
  mutate(
    adopter_class = case_when(
      is.na(first_auto_yr)         ~ "Non-adopter",
      first_auto_yr <= 2021        ~ "Early (≤2021)",
      first_auto_yr <= 2023        ~ "Mid (2022-23)",
      TRUE                         ~ "Late (2024-25)"
    ),
    adopter_class = factor(adopter_class,
      levels = c("Early (≤2021)","Mid (2022-23)","Late (2024-25)","Non-adopter"))
  )

cat("Employer adoption class counts:\n")
employer_adoption |> count(adopter_class) |> print()

cat("\nMedian farm size (avg workers requested) by adoption class:\n")
employer_adoption |>
  group_by(adopter_class) |>
  summarise(n=n(), median_workers=median(avg_workers, na.rm=TRUE),
            mean_workers=round(mean(avg_workers, na.rm=TRUE),1), .groups="drop") |>
  print()

# Adoption timing by state
step4_state <- employer_adoption |>
  filter(adopter_class != "Non-adopter") |>
  group_by(state_mode) |>
  summarise(
    n_adopters     = n(),
    median_adopt_yr= median(first_auto_yr, na.rm=TRUE),
    pct_early      = mean(adopter_class=="Early (≤2021)")*100,
    .groups="drop"
  ) |>
  filter(n_adopters >= 5) |>
  arrange(median_adopt_yr)

cat("\nAdoption timing by state (min 5 adopting employers):\n")
print(step4_state, n=25)

# Non-adopter share by state
step4_nonadopt_state <- employer_adoption |>
  group_by(state_mode) |>
  summarise(
    n_employers    = n(),
    pct_nonadopter = mean(adopter_class=="Non-adopter")*100,
    .groups="drop"
  ) |>
  filter(n_employers >= 10) |>
  arrange(pct_nonadopter)

# ==============================================================================
# FIGURES — NATIONAL
# ==============================================================================

# Step 1 national trend
p1 <- step1_national_yr |>
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
  labs(title    = "Step 1: Direct Automation Mentions — Corn H-2A Orders (National)",
       subtitle = "% of job orders containing each tier of automation vocabulary",
       x=NULL, y="% of JOs",
       caption  = "Source: DOL H-2A job orders.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        legend.position="top",
        plot.background=element_rect(fill="white",colour=NA))

# Step 2 national ratio
p2b <- ratio_df |>
  ggplot(aes(x=year, y=hand_machine_ratio)) +
  geom_line(linewidth=1, color="#d73027") + geom_point(size=3, color="#d73027") +
  geom_hline(yintercept=1, linetype="dashed", color="grey50") +
  labs(title="Step 2: Hand-task / Machine-op Ratio — Corn (National)",
       subtitle="Falling ratio = shift toward machine-operation language",
       x=NULL, y="Ratio",
       caption="Source: DOL H-2A job orders.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=11),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.background=element_rect(fill="white",colour=NA))

# Step 3 neighbor lollipop
p3 <- neighbor_df |>
  slice_head(n=25) |>
  mutate(bigram=fct_reorder(bigram, log2_lift)) |>
  ggplot(aes(x=log2_lift, y=bigram)) +
  geom_segment(aes(x=0, xend=log2_lift, y=bigram, yend=bigram),
               color="#f4a520", linewidth=0.7) +
  geom_point(color="#f4a520", size=3) +
  geom_vline(xintercept=0, color="grey40", linewidth=0.4) +
  labs(title="Step 3: Bigrams Enriched in Automation-Sentence Context — Corn",
       subtitle=sprintf("Top 25 verb-anchored bigrams more common when automation keywords appear  |  n=%d automation sentences",
                        n_auto_sent),
       x="Log₂ lift (auto context vs non-auto JOs)", y=NULL,
       caption="Lift = share in auto sentences / share in control sentences. Min 3 occurrences.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.caption=element_text(colour="grey50",size=7.5),
        plot.background=element_rect(fill="white",colour=NA))

# Step 4 — employer adoption size distribution
p4a <- employer_adoption |>
  filter(!is.na(avg_workers), avg_workers < 300) |>
  ggplot(aes(x=adopter_class, y=avg_workers, fill=adopter_class)) +
  geom_boxplot(outlier.alpha=0.3, linewidth=0.5) +
  scale_fill_manual(values=c("Early (≤2021)"="#1a9641","Mid (2022-23)"="#a6d96a",
                               "Late (2024-25)"="#fdae61","Non-adopter"="#d7d7d7"),
                    guide="none") +
  labs(title="Step 4: Farm Size by Adoption Class — Corn",
       subtitle="Workers requested (avg per employer). Earlier adopters tend to be larger operations.",
       x=NULL, y="Avg workers requested",
       caption="Source: DOL H-2A job orders. Employers with ≥1 JO 2019-2025.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=11),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.background=element_rect(fill="white",colour=NA))

p4b <- step1_state_yr |>
  ggplot(aes(x=year, y=pct_tierB, color=jobState, group=jobState)) +
  geom_line(linewidth=0.8, alpha=0.85) +
  geom_point(size=2) +
  ggrepel::geom_text_repel(
    data = step1_state_yr |> filter(year==2025),
    aes(label=jobState), size=2.8, nudge_x=0.2, direction="y", segment.size=0.3
  ) +
  scale_y_continuous(labels=function(x) paste0(round(x),"%")) +
  labs(title="Step 1 by State: Tier B Mechanization Rate (Top 10 States)",
       subtitle="% of corn JOs mentioning mechanization vocabulary",
       x=NULL, y="% of JOs", color=NULL,
       caption="Source: DOL H-2A job orders. Top 10 states by overall Tier B rate.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=11),
        plot.subtitle=element_text(colour="grey40",size=8),
        legend.position="none",
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# MAPS
# ==============================================================================
us_states <- map_data("state") |> as_tibble()

# Helper: merge state data with map data
make_map_df <- function(state_df, value_col, abb_col="jobState") {
  state_df |>
    rename(jobState_tmp = all_of(abb_col)) |>
    left_join(state_abb2name, by=c("jobState_tmp"="jobState")) |>
    right_join(us_states, by=c("state_name"="region")) |>
    rename(!!abb_col := jobState_tmp)
}

# Map 1: Step 1 — Tier B mechanization rate by state (most recent 2 years 2024-25)
step1_map_data <- mentions |>
  filter(year %in% 2024:2025) |>
  group_by(jobState) |>
  summarise(n_jos=n(), pct_tierB=mean(any_tierB)*100, .groups="drop") |>
  filter(n_jos >= 10)

map1_df <- step1_map_data |>
  left_join(state_abb2name, by="jobState") |>
  right_join(us_states, by=c("state_name"="region"))

p_map1 <- ggplot(map1_df, aes(x=long, y=lat, group=group, fill=pct_tierB)) +
  geom_polygon(color="white", linewidth=0.3) +
  scale_fill_gradient(low="#fff7bc", high="#d73027", na.value="grey90",
                      name="% JOs with\nTier B mention",
                      labels=function(x) paste0(round(x),"%")) +
  coord_fixed(1.3) +
  labs(title="Map 1: Corn H-2A Mechanization Language Rate by State (2024-25)",
       subtitle="% of corn job orders containing Tier B automation vocabulary",
       caption="Source: DOL H-2A job orders. Grey = fewer than 10 JOs in 2024-25.") +
  theme_void(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="right",
        plot.background=element_rect(fill="white",colour=NA))

# Map 2: Step 2 — change in hand-task share 2020-22 → 2023-25 by state
map2_df <- step2_state |>
  left_join(state_abb2name, by="jobState") |>
  right_join(us_states, by=c("state_name"="region"))

p_map2 <- ggplot(map2_df, aes(x=long, y=lat, group=group, fill=hand_share_change)) +
  geom_polygon(color="white", linewidth=0.3) +
  scale_fill_gradient2(low="#1a9641", mid="#ffffbf", high="#d73027", midpoint=0,
                        na.value="grey90",
                        name="Δ Hand-task share\n(pp, + = more manual)",
                        labels=function(x) paste0(round(x,1),"pp")) +
  coord_fixed(1.3) +
  labs(title="Map 2: Shift in Hand-task Language — Corn H-2A (2020-22 vs 2023-25)",
       subtitle="Negative (green) = relative shift toward machine-operation language",
       caption="Source: DOL H-2A job orders. Grey = insufficient data.") +
  theme_void(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="right",
        plot.background=element_rect(fill="white",colour=NA))

# Map 3: Step 4 — median employer adoption year by state
map3_df <- step4_state |>
  rename(jobState=state_mode) |>
  left_join(state_abb2name, by="jobState") |>
  right_join(us_states, by=c("state_name"="region"))

p_map3 <- ggplot(map3_df, aes(x=long, y=lat, group=group, fill=median_adopt_yr)) +
  geom_polygon(color="white", linewidth=0.3) +
  scale_fill_gradient(low="#1a9641", high="#d73027", na.value="grey90",
                       name="Median first\nadoption year",
                       breaks=c(2020,2021,2022,2023,2024,2025)) +
  coord_fixed(1.3) +
  labs(title="Map 3: Employer Adoption Timing — Corn H-2A (Step 4)",
       subtitle="Median year of first Tier B mechanization mention per employer, by state",
       caption="Source: DOL H-2A job orders. Grey = fewer than 5 adopting employers.") +
  theme_void(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="right",
        plot.background=element_rect(fill="white",colour=NA))

# Map 4: JO density (total JOs 2019-2025) — reference map
map4_df <- df_raw |>
  filter(year >= 2019) |>
  count(jobState, name="n_jos") |>
  left_join(state_abb2name, by="jobState") |>
  right_join(us_states, by=c("state_name"="region"))

p_map4 <- ggplot(map4_df, aes(x=long, y=lat, group=group, fill=n_jos)) +
  geom_polygon(color="white", linewidth=0.3) +
  scale_fill_gradient(low="#deebf7", high="#08519c", na.value="grey90",
                       name="Corn H-2A JOs\n(2019-2025)",
                       labels=scales::comma) +
  coord_fixed(1.3) +
  labs(title="Map 4 (Reference): Corn H-2A Job Orders by State (2019-2025)",
       subtitle="Total number of corn H-2A job orders filed",
       caption="Source: DOL H-2A job orders.") +
  theme_void(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="right",
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# SAVE ALL FIGURES
# ==============================================================================
# National combined panel
p_combined_national <- (p1 / p2b / p3) +
  plot_annotation(
    title    = "Corn H-2A Job Orders: Automation Signals (National, 2020-2025)",
    subtitle = "Steps 1-3: direct mentions → task displacement → semantic neighbors",
    caption  = "Source: DOL H-2A job orders (JSON + gap PDFs + OFLC disclosure).",
    theme    = theme(plot.title=element_text(face="bold",size=14),
                     plot.subtitle=element_text(colour="grey40",size=9),
                     plot.background=element_rect(fill="white",colour=NA))
  )

ggsave("processed/images/plot_orange_automation_national.png",
       p_combined_national, width=14, height=18, dpi=180, bg="white")

# Step 4 panel
p4_combined <- (p4b | p4a) +
  plot_annotation(
    title   = "Corn H-2A Step 4: State Trends & Employer Adoption Heterogeneity",
    caption = "Source: DOL H-2A job orders.",
    theme   = theme(plot.title=element_text(face="bold",size=13),
                    plot.background=element_rect(fill="white",colour=NA))
  )
ggsave("processed/images/plot_orange_step4_adoption.png",
       p4_combined, width=16, height=7, dpi=180, bg="white")

# Maps
ggsave("processed/images/plot_orange_map1_mechanization_rate.png",
       p_map1, width=12, height=7, dpi=180, bg="white")
ggsave("processed/images/plot_orange_map2_hand_shift.png",
       p_map2, width=12, height=7, dpi=180, bg="white")
ggsave("processed/images/plot_orange_map3_adoption_timing.png",
       p_map3, width=12, height=7, dpi=180, bg="white")
ggsave("processed/images/plot_orange_map4_jo_density.png",
       p_map4, width=12, height=7, dpi=180, bg="white")

# 4-map composite
p_maps <- (p_map4 | p_map1) / (p_map2 | p_map3) +
  plot_annotation(
    title   = "Corn H-2A Automation Analysis — State-Level Maps",
    caption = "Source: DOL H-2A job orders. Steps 1-3 methodology; Step 4 employer adoption.",
    theme   = theme(plot.title=element_text(face="bold",size=14),
                    plot.background=element_rect(fill="white",colour=NA))
  )
ggsave("processed/images/plot_orange_maps_composite.png",
       p_maps, width=20, height=12, dpi=180, bg="white")

cat("Saved: all corn automation figures\n")

# ==============================================================================
# SAVE RDS
# ==============================================================================
saveRDS(
  list(
    crop         = CROP_NAME,
    auto_cases   = auto_cases,
    mentions     = mentions,
    annual_mode  = annual_mode,
    ratio_df     = ratio_df,
    neighbor_df  = neighbor_df,
    step1_state  = step1_state,
    step2_state  = step2_state,
    step4_state  = step4_state,
    employer_adoption = employer_adoption
  ),
  "processed/text/automation_signals_orange.rds"
)
cat("Saved: automation_signals_orange.rds\nAll done.\n")
