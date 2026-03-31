###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Extract top gerunds (-ing words) from H-2A task descriptions.
# Purpose: data-driven review of candidate task verbs before finalizing
#          the ag_verbs filter used in extract_tasks_jo.R
#
# Output: gerunds_top300.csv  — review and mark "keep" column manually,
#         then paste confirmed verbs into ag_verbs in extract_tasks_jo.R
###############################################################################

cat("\014"); rm(list = ls())

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor"
out_dir <- paste0(mydir, "/h2a_labor_demand/processed/text")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ------------------------------------------------------------------------------
# 1. Load cleaned task text (output of extract_tasks_jo.R)
# ------------------------------------------------------------------------------
tasks <- readRDS(paste0(out_dir, "/tasks_jo.rds"))
cat("Docs loaded:", nrow(tasks$df_clean), "\n")

# ------------------------------------------------------------------------------
# 2. Non-task -ing words to exclude upfront
#    (legal/boilerplate, modal, physical-condition, or generic English gerunds)
# ------------------------------------------------------------------------------
exclude_ing <- c(
  # generic English
  "including","following","during","according","something","anything",
  "everything","nothing","morning","evening","building","housing","clothing",
  "being","having","doing","going","coming","getting","making","taking",
  "saying","knowing","looking","working","living","giving","using","walking",
  "talking","thinking","moving","sitting","standing","starting","ending",
  # legal / HR / T&C verbs
  "providing","requiring","agreeing","signing","understanding","indicating",
  "ensuring","allowing","governing","receiving","reporting","submitting",
  "determining","constituting","accepting","notifying","complying",
  "authorizing","violating","terminating","assigning","prevailing",
  "remaining","concerning","regarding","representing","existing",
  "resulting","occurring","involving","containing","affecting",
  "completing","continuing","meeting","holding","keeping","letting",
  "bringing","finding","telling","calling","trying","asking","changing",
  "showing","becoming","putting","setting","turning","writing",
  "reading","opening","closing","leaving","playing","leading","helping",
  "processing","scheduling","training","hiring","paying","covering",
  "protecting","preventing","conducting","managing","supervising",
  "coordinating","planning","testing","verifying","attending","raising",
  # physical/health conditions (appear in T&C accommodation language)
  "bending","stooping","lifting","kneeling","reaching","crawling",
  "grasping","pushing","pulling","throwing","climbing","carrying",
  "wearing","touching","coughing","sneezing","drinking","smoking",
  "sleeping","fighting","chewing","blowing",
  # misconduct / disciplinary language
  "abusing","bullying","loitering","threatening","arguing","bickering",
  "scuffling","wasting","abandoning","neglecting","refusing","damaging",
  "disabling","restraining","possessing",
  # other noise
  "depending","willing","missing","warning","offspring","earring",
  "wedding","corresponding","surrounding","belonging","dwelling",
  "commuting","varying","arising","functioning","revolving","rotating",
  "earning","visiting","avoiding","causing","permitting","learning",
  "participating","relating","challenging","deducting","selling",
  "accounting","posting","obtaining","securing","returning","freezing",
  "rolling","ranging","seeking","entering","beginning","corresponding"
)

# ------------------------------------------------------------------------------
# 3. Extract all -ing tokens (vectorized — fast on 99K docs)
# ------------------------------------------------------------------------------
cat("Extracting -ing tokens...\n")
t0 <- Sys.time()

all_ing <- unlist(str_extract_all(str_to_lower(tasks$df_clean$text_clean),
                                  "[a-z]{4,}ing"))
cat(sprintf("  Total -ing tokens: %s  (%.1f sec)\n",
            format(length(all_ing), big.mark = ","),
            as.numeric(Sys.time() - t0)))

# ------------------------------------------------------------------------------
# 4. Frequency table, exclude noise, top 300
# ------------------------------------------------------------------------------
gerunds <- tibble(word = all_ing) %>%
  filter(!word %in% exclude_ing) %>%
  count(word, sort = TRUE) %>%
  mutate(
    rank      = row_number(),
    base_form = str_remove(word, "ing$"),          # rough base: cultivating → cultivat
    # heuristic base cleanup
    base_form = case_when(
      str_sub(base_form, -1) == str_sub(base_form, -2, -2) ~
        str_sub(base_form, 1, -2),                 # doubling: running → run
      TRUE ~ base_form
    )
  ) %>%
  slice_head(n = 300)

cat("\n=== Top 300 candidate task gerunds ===\n")
print(as.data.frame(gerunds[, c("rank","word","n","base_form")]),
      row.names = FALSE)

# ------------------------------------------------------------------------------
# 5. Save CSV for manual review
#    Add a "keep" column — fill in 1=yes, 0=no, then paste confirmed verbs
#    into ag_verbs in extract_tasks_jo.R
# ------------------------------------------------------------------------------
gerunds$keep <- NA   # fill manually: 1 = agricultural task verb, 0 = noise

write.csv(gerunds[, c("rank","word","n","base_form","keep")],
          paste0(out_dir, "/gerunds_top300.csv"),
          row.names = FALSE)

cat(sprintf("\nSaved: %s/gerunds_top300.csv\n", out_dir))
cat("Next step: open CSV, fill the 'keep' column (1/0), then update ag_verbs in extract_tasks_jo.R\n")
