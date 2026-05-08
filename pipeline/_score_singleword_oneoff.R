suppressPackageStartupMessages({library(arrow); library(data.table); library(stringr)})

abilities <- list(
  "Trunk Strength"                  = c("abdominal","back","muscle","support","body","repeatedly","continuously","fatigue"),
  "Arm-Hand Steadiness"             = c("hand","arm","steady","moving","holding","position"),
  "Control Precision"               = c("quickly","repeatedly","adjust","control","machine","vehicle","exact","position"),
  "Extent Flexibility"              = c("bend","stretch","twist","reach","body","arm","leg"),
  "Manual Dexterity"                = c("move","hand","arm","grasp","manipulate","assemble","object"),
  "Multilimb Coordination"          = c("coordinate","limb","arm","leg","sitting","standing","lying","motion"),
  "Finger Dexterity"                = c("coordinated","movement","finger","hand","grasp","manipulate","assemble","small","object"),
  "Static Strength"                 = c("exert","maximum","muscle","force","lift","push","pull","carry","object"),
  "Dynamic Strength"                = c("exert","muscle","force","repeatedly","continuously","endurance","resistance","fatigue"),
  "Near Vision"                     = c("see","detail","close","range","foot","observer"),
  "Rate Control"                    = c("movement","equipment","anticipation","change","speed","direction","moving","object","scene"),
  "Stamina"                         = c("exert","physically","long","period","winded","breath"),
  "Explosive Strength"              = c("short","burst","muscle","force","propel","jumping","sprinting","throw","object"),
  "Oral Expression"                 = c("communicate","information","idea","speaking","understand"),
  "Reaction Time"                   = c("respond","hand","finger","foot","signal","sound","light","picture"),
  "Far Vision"                      = c("see","detail","distance"),
  "Gross Body Coordination"         = c("coordinate","movement","arm","leg","torso","body","motion"),
  "Oral Comprehension"              = c("listen","understand","information","idea","spoken","word","sentence"),
  "Deductive Reasoning"             = c("apply","general","rule","specific","problem","answer","sense"),
  "Gross Body Equilibrium"          = c("keep","regain","body","balance","upright","unstable","position"),
  "Problem Sensitivity"             = c("wrong","likely","solving","problem","recognizing"),
  "Speech Clarity"                  = c("speak","clearly","understand"),
  "Speech Recognition"              = c("identify","understand","speech","person"),
  "Category Flexibility"            = c("generate","different","set","rule","combining","grouping"),
  "Inductive Reasoning"             = c("combine","piece","information","general","rule","conclusion","relationship","event"),
  "Written Expression"              = c("communicate","information","idea","writing","understand"),
  "Information Ordering"            = c("arrange","action","order","pattern","rule","number","letter","word","picture","mathematical","operation"),
  "Response Orientation"            = c("choose","movement","response","signal","light","sound","picture","speed","hand","foot","body"),
  "Selective Attention"             = c("concentrate","task","period","distracted"),
  "Visualization"                   = c("imagine","look","moved","part","rearranged"),
  "Dynamic Flexibility"             = c("bend","stretch","twist","reach","body","arm","leg"),
  "Flexibility of Closure"          = c("identify","detect","pattern","figure","object","word","sound","distracting","material"),
  "Fluency of Ideas"                = c("number","idea","topic","quality","correctness","creativity"),
  "Originality"                     = c("unusual","clever","idea","topic","situation","develop","creative","solve","problem"),
  "Spatial Orientation"             = c("know","location","relation","environment","object"),
  "Written Comprehension"           = c("read","understand","information","idea","writing"),
  "Depth Perception"                = c("judge","object","closer","farther","distance"),
  "Time Sharing"                    = c("shift","activity","source","information","speech","sound","touch"),
  "Wrist-Finger Speed"              = c("fast","simple","repeated","movement","finger","hand","wrist"),
  "Perceptual Speed"                = c("compare","similarity","difference","letter","number","object","picture","pattern","presented","remembered"),
  "Speed of Closure"                = c("make","sense","combine","organize","information","meaningful","pattern"),
  "Speed of Limb Movement"          = c("move","arm","leg"),
  "Visual Color Discrimination"     = c("match","detect","difference","color","shade","brightness"),
  "Auditory Attention"              = c("focus","single","source","sound","distracting"),
  "Hearing Sensitivity"             = c("detect","difference","sound","pitch","loudness"),
  "Mathematical Reasoning"          = c("choose","mathematical","method","formula","solve","problem"),
  "Memorization"                    = c("remember","information","word","number","picture","procedure"),
  "Number Facility"                 = c("add","subtract","multiply","divide"),
  "Peripheral Vision"               = c("object","movement","side","eye","looking","ahead"),
  "Glare Sensitivity"               = c("object","glare","bright","lighting"),
  "Sound Localization"              = c("direction","sound","originated"),
  "Night Vision"                    = c("see","low","light","condition")
)

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
  "exact","maximum","close","far"
)
abilities <- lapply(abilities, function(v) setdiff(v, STOP))

all_terms <- unique(unlist(abilities))
N <- length(abilities)
df <- sapply(all_terms, function(t) sum(sapply(abilities, function(a) t %in% a)))
idf <- log(N / df); names(idf) <- all_terms

p <- as.data.table(read_parquet("output/text/core/jo_full.parquet"))
p <- p[!is.na(duties_full) & nchar(duties_full) > 500 & nchar(duties_full) < 4000 &
       grepl("^45-2092", socCode)]
set.seed(31415)   # different random pick
jo <- p[sample(.N, 1)]
cat(sprintf("=== JO %s | %s ===\n\n", jo$caseNumber, jo$jobState))
cat(jo$duties_full, "\n\n")

txt <- tolower(jo$duties_full)
tokens <- unique(unlist(strsplit(gsub("[^a-z]", " ", txt), " +")))
tokens <- tokens[nchar(tokens) > 0]

res <- data.table(ability = character(), score = numeric(), hits = character())
for (a in names(abilities)) {
  terms <- abilities[[a]]
  hits <- intersect(terms, tokens)
  s <- if (length(hits)) sum(idf[hits]) else 0
  res <- rbind(res, data.table(ability = a, score = round(s, 2),
                               hits = paste(sprintf("%s(%.2f)", hits, idf[hits]), collapse = ", ")))
}
setorder(res, -score)
cat("=== Non-zero ability scores (single-word + TF-IDF, no stoplist) ===\n")
print(res[score > 0], nrows = Inf)
cat(sprintf("\nFlagged: %d / 52\n", sum(res$score > 0)))
