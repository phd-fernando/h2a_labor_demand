###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Automation adoption signals in strawberry H-2A job orders — three-step
# strategy to detect robot/mechanization language:
#
#   Step 1 — Direct mention search
#             Explicit automation vocabulary (robot, mechanical harvest, GPS,
#             sensor, conveyor…). High precision, expected low frequency.
#
#   Step 2 — Task displacement signal
#             Track hand-task bigrams vs machine-operation bigrams over time.
#             A falling hand/machine ratio = labor displacement signal even
#             without "robot" ever appearing.
#
#   Step 3 — Semantic neighborhood expansion
#             From Step 1 direct-mention JOs, extract bigrams that co-occur
#             in the same sentences as automation keywords. These describe
#             complementary/adjacent tasks that employers discuss alongside
#             automation — the indirect vocabulary of mechanization.
#
# See notes/18_automation_filtering.txt for full rationale.
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
# 0. BUILD STRAWBERRY CORPUS  (same pipeline as script 17)
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

farmer_strawberry <- df_crops |>
  filter(!is.na(cropActivity), str_detect(tolower(cropActivity), "strawberr")) |>
  distinct(caseNumber) |> mutate(farmer_strawberry = TRUE)

df_raw <- enr |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf)) |>
  left_join(farmer_strawberry, by = "caseNumber") |>
  mutate(is_strawberry = coalesce(farmer_strawberry, FALSE) |
           str_detect(tolower(coalesce(duties, "")), "strawberr")) |>
  filter(is_strawberry,
         !is.na(duties), nchar(trimws(duties)) > 20,
         !is.na(empFein), !is.na(dateSubmitted),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30")) |>
  mutate(year     = year(dateSubmitted),
         emp_year = paste0(empFein, "_", year)) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year, duties,
         jobState, jobWrksNeeded)

cat(sprintf("Strawberry corpus: %d JOs from %d employers\n\n",
    nrow(df_raw), n_distinct(df_raw$empFein)))

# Split to sentences (used in Steps 1 and 3)
df_sentences <- df_raw |>
  separate_rows(duties, sep = "(?<=[.!?;\\n])\\s+") |>
  mutate(sentence = str_squish(duties)) |>
  filter(nchar(sentence) > 15) |>
  select(caseNumber, empFein, emp_year, dateSubmitted, year,
         jobState, jobWrksNeeded, sentence)

# ==============================================================================
# STEP 1 — DIRECT MENTION SEARCH
#
# STRATEGY: scan raw duties text (not sentence-filtered) for explicit
# automation/mechanization vocabulary. We use the full text here — not the
# ag-verb-filtered text — because automation mentions sometimes appear in
# context sentences that lack ag verbs (e.g. "The farm uses robotic platforms
# to assist with harvest operations.").
#
# VOCABULARY RATIONALE (see notes/18_automation_filtering.txt §1):
#   Three tiers of explicitness:
#   Tier A — Unambiguous automation: robot, robotic, autonomous, self-propelled
#             harvester (when paired with "machine" or "automated")
#   Tier B — Strong mechanization signals: mechanical harvest, machine harvest,
#             conveyor, harvesting machine, harvest assist(ant)
#   Tier C — Precision agriculture (often precursor to automation): GPS, sensor,
#             electronic record, precision, variable rate, drone, UAV
#
# EXCLUDED FROM DIRECT MENTION:
#   "tractor", "equipment", "machinery" — too generic; present in virtually
#   all JOs and not specific to automation.
#   "electronic" alone — often refers to payroll/timekeeping, not field tech.
# ==============================================================================

# Tier labels for interpretability in output
auto_vocab <- tribble(
  ~pattern,                         ~tier,  ~label,
  "\\brobot\\b|\\brobotic",         "A",    "robotic",
  "\\bautonomous\\b",               "A",    "autonomous",
  "mechanical.*harvest|harvest.*mechanical", "B", "mechanical harvest",
  "machine.*harvest|harvest.*machine",       "B", "machine harvest",
  "harvest(ing)? assist",           "B",    "harvest assist",
  "harvest(ing)? platform",         "B",    "harvest platform",
  "\\bconveyor\\b",                 "B",    "conveyor",
  "self.propelled",                 "B",    "self-propelled",
  "\\bautomated\\b",                "B",    "automated",
  "\\bGPS\\b|\\bgps.guid",         "C",    "GPS",
  "\\bdrone\\b|\\bUAV\\b",          "C",    "drone/UAV",
  "\\bsensor\\b",                   "C",    "sensor",
  "precision.agricult|precision.farm", "C", "precision ag",
  "electronic.record|digital.record",  "C", "electronic records"
)

# Flag each JO for each vocab term
flag_mentions <- function(text_vec, pattern) str_detect(tolower(text_vec), pattern)

mentions <- df_raw |>
  select(caseNumber, empFein, dateSubmitted, year, jobWrksNeeded, duties)

for (i in seq_len(nrow(auto_vocab))) {
  col <- paste0("auto_", gsub("[^a-z0-9]", "_", auto_vocab$label[i]))
  mentions[[col]] <- flag_mentions(mentions$duties, auto_vocab$pattern[i])
}

# Any automation mention (any tier)
auto_cols <- names(mentions)[str_detect(names(mentions), "^auto_")]
mentions <- mentions |>
  mutate(any_auto  = rowSums(across(all_of(auto_cols))) > 0,
         any_tierA = rowSums(across(starts_with("auto_robot") |
                                    starts_with("auto_autonomous"))) > 0,
         any_tierB = rowSums(across(matches("auto_(mechanical|machine|harvest_assist|harvest_platform|conveyor|self|automated)"))) > 0,
         any_tierC = rowSums(across(matches("auto_(GPS|drone|sensor|precision|electronic)"))) > 0)

cat("=== STEP 1: Direct automation mentions ===\n")
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
  summarise(across(all_of(auto_cols), sum)) |>
  tidyr::pivot_longer(everything(), names_to="term", values_to="n") |>
  mutate(term = str_remove(term, "^auto_"),
         pct  = round(n / nrow(mentions) * 100, 2)) |>
  filter(n > 0) |> arrange(desc(n)) |> print()

# Trend by year
cat("\n=== Automation mentions by year ===\n")
mentions |>
  group_by(year) |>
  summarise(n_jos     = n(),
            n_any     = sum(any_auto),
            n_tierA   = sum(any_tierA),
            n_tierB   = sum(any_tierB),
            n_tierC   = sum(any_tierC),
            pct_any   = round(mean(any_auto)*100, 1),
            workers   = sum(jobWrksNeeded, na.rm=TRUE),
            .groups="drop") |>
  print()

# Save direct-mention case numbers for Step 3
auto_cases <- mentions |> filter(any_auto) |> pull(caseNumber)
cat(sprintf("\n%d JOs with direct automation mention — used in Step 3\n\n",
    length(auto_cases)))

# Plot: share of JOs mentioning automation by year
p1 <- mentions |>
  group_by(year) |>
  summarise(pct_tierA = mean(any_tierA)*100,
            pct_tierB = mean(any_tierB)*100,
            pct_tierC = mean(any_tierC)*100,
            n = n(), .groups="drop") |>
  filter(year >= 2020, year <= 2025) |>
  tidyr::pivot_longer(starts_with("pct_"), names_to="tier", values_to="pct") |>
  mutate(tier = recode(tier,
    pct_tierA = "Tier A: Robotic / autonomous",
    pct_tierB = "Tier B: Mechanized harvest",
    pct_tierC = "Tier C: Precision agriculture")) |>
  ggplot(aes(x=year, y=pct, color=tier, group=tier)) +
  geom_line(linewidth=1) +
  geom_point(size=2.5) +
  scale_color_manual(values=c("Tier A: Robotic / autonomous"="#d73027",
                               "Tier B: Mechanized harvest"  ="#f46d43",
                               "Tier C: Precision agriculture"="#4575b4"),
                     name=NULL) +
  scale_y_continuous(labels=function(x) paste0(x,"%")) +
  labs(title    = "Step 1: Direct Automation Mentions — Strawberry H-2A Orders",
       subtitle = "% of job orders containing each tier of automation vocabulary",
       x=NULL, y="% of JOs",
       caption  = "Source: DOL H-2A job orders. See notes/18_automation_filtering.txt for vocabulary.") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="top",
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# STEP 2 — TASK DISPLACEMENT SIGNAL
#
# STRATEGY: classify every verb-anchored bigram as one of four task modes:
#
#   HAND-TASK    : bigram contains "hand" as a modifier of an ag verb, OR
#                  the ag verb alone implies manual work without equipment
#                  (hand pick, hand harvest, hand place, hand sort, hand thin,
#                   hand prune, hand weed, hand plant, manually)
#
#   MACHINE-OP   : bigram pairs an ag verb with equipment/operation language
#                  (operate harvester, drive tractor, load conveyor, attach
#                   implement, calibrate equipment, monitor sensor)
#
#   QUALITY-CTRL : grading, inspection, sorting tasks — often shift from
#                  field-level to machine-output-level as automation grows
#                  (inspect, grade, sort, discard, cull)
#
#   OTHER-AG     : all remaining verb-anchored bigrams
#
# The ratio hand-task / machine-op over time is the displacement signal.
# A rising machine-op share alongside falling hand-task share = mechanization.
#
# NOTE: a bigram can only be in one mode (precedence order: hand > machine > quality > other).
# ==============================================================================

# Ag-verb sentence filter (same as script 17 Layer 1)
ag_verb_pattern <- paste0(
  "harvest|pick|plant|transplant|cultivat|till|bed|weed|hoe|thin|",
  "prune|trim|train|trellis|tie|irrigat|fertil|spray|apply|",
  "mulch|mow|sow|seed|sort|grade|pack|box|load|unload|wash|cool|",
  "wrap|weigh|label|stack|bin|inspect|operat|driv|attach|calibrat|",
  "adjust|hand.pick|hand.harvest|place|carry|cut|pull|dig|",
  "remov|stake|cover|clip|propagat|discard|monitor|repair|clean"
)

# Layer 2 stopwords (same as script 17)
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
  "strawberry","strawberries","berry","berries"
))
stop_words_all <- bind_rows(stop_words, tc_stopwords) |> distinct(word)

# Verb-anchor pattern (Layer 4 from script 17)
verb_anchor <- paste0(
  "^harvest|^pick|^plant|^transplant|^cultivat|^till|^bed|^weed|^hoe|",
  "^thin|^prune|^trim|^train|^trellis|^tie|^irrig|^fertil|^spray|^apply|",
  "^mulch|^mow|^sow|^seed|^sort|^grade|^pack|^box|^load|^unload|^wash|",
  "^cool|^wrap|^weigh|^label|^stack|^bin|^inspect|^operat|^driv|^attach|",
  "^calibrat|^adjust|^carry|^cut|^pull|^dig|^remov|^stake|^cover|^clip|",
  "^propagat|^place|^repair|^clean|^discard|^monitor|^scout|^sample|",
  "^record|^measur|^count|^flag|^sucker|^debud|^set"
)

# Sentence filter → bigrams → verb anchor
df_ag_sentences <- df_sentences |>
  filter(str_detect(tolower(sentence), ag_verb_pattern))

tokens_bi_all <- df_ag_sentences |>
  unnest_tokens(bigram, sentence, token="ngrams", n=2) |>
  separate(bigram, into=c("w1","w2"), sep=" ", remove=FALSE) |>
  filter(!w1 %in% stop_words_all$word,
         !w2 %in% stop_words_all$word,
         str_detect(w1, "^[a-z]+$"),
         str_detect(w2, "^[a-z]+$"),
         str_detect(w1, verb_anchor) | str_detect(w2, verb_anchor))

# Task mode classification
hand_pattern    <- "^hand$|^manually$|^manual$|^handpick|^handharv"
machine_pattern <- paste0("^operat|^driv|^attach|^calibrat|^adjust|^monitor|",
                           "^load|^unload|machine|mechanic|conveyor|platform|",
                           "^sensor|autonomous|robotic|automated|self.propel")
quality_pattern <- "^inspect|^grade|^sort|^discard|^cull|^reject|^weigh|^pack"

tokens_bi_mode <- tokens_bi_all |>
  mutate(mode = case_when(
    str_detect(w1, hand_pattern) | str_detect(w2, hand_pattern)       ~ "hand-task",
    str_detect(w1, machine_pattern) | str_detect(w2, machine_pattern) ~ "machine-op",
    str_detect(w1, quality_pattern) | str_detect(w2, quality_pattern) ~ "quality-ctrl",
    TRUE                                                                ~ "other-ag"
  ))

cat("=== STEP 2: Task mode distribution (all years, JO level) ===\n")
tokens_bi_mode |>
  distinct(caseNumber, bigram, mode) |>
  count(mode, sort=TRUE) |>
  mutate(pct=round(n/sum(n)*100,1)) |> print()

# Employer-year dedup then annual shares
annual_mode <- tokens_bi_mode |>
  distinct(emp_year, bigram, mode) |>
  left_join(df_raw |> distinct(emp_year, year), by="emp_year") |>
  group_by(year, mode) |>
  summarise(n=n(), .groups="drop") |>
  group_by(year) |>
  mutate(share=n/sum(n)*100) |> ungroup()

cat("\n=== Annual mode shares (%) ===\n")
annual_mode |>
  select(year, mode, share) |>
  tidyr::pivot_wider(names_from=mode, values_from=share, values_fill=0) |>
  mutate(across(where(is.numeric), ~round(.,1))) |>
  print()

# Hand vs machine ratio
ratio_df <- annual_mode |>
  filter(mode %in% c("hand-task","machine-op")) |>
  select(year, mode, share) |>
  tidyr::pivot_wider(names_from=mode, values_from=share, values_fill=0) |>
  mutate(hand_machine_ratio = `hand-task` / (`machine-op` + 0.01)) |>
  filter(year >= 2020, year <= 2025)

cat("\n=== Hand-task / machine-op ratio by year ===\n")
print(ratio_df)

p2a <- annual_mode |>
  filter(year >= 2020, year <= 2025) |>
  mutate(mode = factor(mode, levels=c("hand-task","quality-ctrl","machine-op","other-ag"))) |>
  ggplot(aes(x=year, y=share, color=mode, group=mode)) +
  geom_line(linewidth=1) + geom_point(size=2.5) +
  scale_color_manual(values=c("hand-task"="#d73027","machine-op"="#1a9641",
                               "quality-ctrl"="#4575b4","other-ag"="#bababa"),
                     name=NULL) +
  scale_y_continuous(labels=function(x) paste0(round(x),"%")) +
  labs(title="Step 2a: Task Mode Shares Over Time — Strawberry H-2A",
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
       x=NULL, y="Hand-task / machine-op bigram ratio", caption="") +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=11),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.background=element_rect(fill="white",colour=NA))

cat("\nTop 20 HAND-TASK bigrams:\n")
tokens_bi_mode |> filter(mode=="hand-task") |>
  distinct(emp_year, bigram) |> count(bigram, sort=TRUE) |>
  slice_head(n=20) |> print()

cat("\nTop 20 MACHINE-OP bigrams:\n")
tokens_bi_mode |> filter(mode=="machine-op") |>
  distinct(emp_year, bigram) |> count(bigram, sort=TRUE) |>
  slice_head(n=20) |> print()

# ==============================================================================
# STEP 3 — SEMANTIC NEIGHBORHOOD EXPANSION
#
# STRATEGY: from the direct-mention JOs (Step 1), extract all ag-verb sentences
# that contain an automation keyword. Then collect every OTHER verb-anchored
# bigram in those same sentences — these are the task-context neighbors of
# automation language.
#
# Compare frequency of each neighbor bigram between:
#   - automation-sentence context  (same sentence as auto keyword)
#   - non-automation strawberry JOs (control group)
# A bigram significantly more common in automation context than in control =
# a complement/adjacent task to automation.
#
# This answers: "When employers describe robots, what else do they say workers
# will do in the same breath?"
# ==============================================================================

cat("\n=== STEP 3: Semantic neighborhood expansion ===\n")

# Sentences from direct-mention JOs that themselves contain an auto keyword
auto_keyword_pattern <- paste0(
  auto_vocab$pattern, collapse="|"
)

auto_sentences <- df_sentences |>
  filter(caseNumber %in% auto_cases,
         str_detect(tolower(sentence), auto_keyword_pattern),
         str_detect(tolower(sentence), ag_verb_pattern))

cat(sprintf("Automation-keyword sentences: %d (from %d JOs)\n",
    nrow(auto_sentences), n_distinct(auto_sentences$caseNumber)))

# Control: ag-verb sentences from non-auto JOs
control_sentences <- df_sentences |>
  filter(!caseNumber %in% auto_cases,
         str_detect(tolower(sentence), ag_verb_pattern))

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
           # exclude the automation keyword itself from the neighbor list
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
  mutate(n_auto = coalesce(n_auto, 0L),
         n_ctrl = coalesce(n_ctrl, 0L),
         share_auto = n_auto / n_auto_sent,
         share_ctrl = n_ctrl / n_control_sent,
         # lift: how much more common in auto context vs control?
         lift       = (share_auto + 1e-6) / (share_ctrl + 1e-6),
         log2_lift  = log2(lift)) |>
  filter(n_auto >= 3) |>   # min 3 occurrences in automation sentences
  arrange(desc(lift))

cat("=== Top 30 bigrams enriched in automation-sentence context ===\n")
neighbor_df |>
  select(bigram, n_auto, n_ctrl, share_auto, share_ctrl, lift) |>
  mutate(share_auto=round(share_auto,3), share_ctrl=round(share_ctrl,3),
         lift=round(lift,2)) |>
  slice_head(n=30) |> as.data.frame() |> print()

# Plot: lollipop of top neighbors
p3 <- neighbor_df |>
  slice_head(n=25) |>
  mutate(bigram=fct_reorder(bigram, log2_lift)) |>
  ggplot(aes(x=log2_lift, y=bigram)) +
  geom_segment(aes(x=0, xend=log2_lift, y=bigram, yend=bigram),
               color="#4575b4", linewidth=0.7) +
  geom_point(color="#4575b4", size=3) +
  geom_vline(xintercept=0, color="grey40", linewidth=0.4) +
  labs(title="Step 3: Bigrams Enriched in Automation-Sentence Context",
       subtitle=sprintf("Top 25 verb-anchored bigrams more common when automation keywords appear  |  n=%d automation sentences",
                        n_auto_sent),
       x="Log₂ lift (auto context vs non-auto JOs)", y=NULL,
       caption=paste0("Source: DOL H-2A strawberry job orders.\n",
                      "Lift = share in auto sentences / share in control sentences. ",
                      "Min 3 occurrences in auto context.")) +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=12),
        plot.subtitle=element_text(colour="grey40",size=8),
        plot.caption=element_text(colour="grey50",size=7.5),
        plot.background=element_rect(fill="white",colour=NA))

# ==============================================================================
# COMBINED FIGURE
# ==============================================================================
p_combined <- (p1 / (p2a | p2b) / p3) +
  plot_annotation(
    title    = "Automation Signals in Strawberry H-2A Job Orders (2020–2025)",
    subtitle = "Three-step detection: direct mentions → task displacement → semantic neighbors",
    caption  = "Source: DOL H-2A job orders (JSON + gap PDFs + OFLC disclosure).",
    theme    = theme(
      plot.title    = element_text(face="bold", size=14),
      plot.subtitle = element_text(colour="grey40", size=9),
      plot.caption  = element_text(colour="grey50", size=7.5),
      plot.background = element_rect(fill="white", colour=NA)
    )
  )

ggsave("processed/images/plot_strawberry_automation_combined.png",
       p_combined, width=14, height=18, dpi=180, bg="white")
cat("\nSaved: plot_strawberry_automation_combined.png\n")

ggsave("processed/images/plot_strawberry_automation_step1.png",
       p1, width=10, height=5, dpi=180, bg="white")
ggsave("processed/images/plot_strawberry_automation_step2.png",
       p2a / p2b, width=10, height=8, dpi=180, bg="white")
ggsave("processed/images/plot_strawberry_automation_step3.png",
       p3, width=10, height=7, dpi=180, bg="white")
cat("Saved individual step plots.\n")

# ==============================================================================
# SAVE
# ==============================================================================
saveRDS(
  list(auto_cases   = auto_cases,
       mentions     = mentions,
       annual_mode  = annual_mode,
       ratio_df     = ratio_df,
       neighbor_df  = neighbor_df),
  "processed/text/automation_signals_strawberry.rds"
)
cat("Saved: automation_signals_strawberry.rds\nAll done.\n")
