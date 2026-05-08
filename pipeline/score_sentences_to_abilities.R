###############################################################################
# Score every farm sentence (n_tc==0 & n_farm>=1, already filtered upstream)
# against the 41 O*NET GWAs via MPNet sentence embeddings, then derive 52
# per-ability scores via the published 41x52 GWA->Abilities linkage matrix
# weighted by SOC-level GWA and ability importance scraped from O*NET.
#
# Filter: socCode starts with 45-2091, 45-2092, or 45-2093 only.
#
# Per-ability score formula (per sentence s, per ability a), option-2 normalized:
#   ability[s, a] = imp_abil[soc(s), a] / k[a] *
#                   sum_g ( cosine[s, g] * link[g, a] * imp_gwa[soc(s), g] )
# where link[g, a] is the GRADED matrix (digit/8, X=1, blank/#=0) and
#       k[a] = sum_g link[g, a] is the (graded) ability in-degree.
# Columns with k[a] = 0 stay at raw 0 (no GWA links to that ability).
#
# Output:
#   output/text/core/sentences_with_onet_scores.parquet
#   columns: caseNumber, sid, sentence, socCode, year, month, n_tc, n_farm,
#            crops, n_crops, jobState,
#            gwa_<41 short codes>   (cosine similarity, raw),
#            abil_<52 short codes>  (final weighted score).
###############################################################################
suppressPackageStartupMessages({
  library(reticulate); library(arrow); library(data.table); library(stringi)
})
use_python("C:/Users/Fer/AppData/Local/Programs/Python/Python310/python.exe", required = TRUE)
st <- import("sentence_transformers")
np <- import("numpy")

CORE      <- "output/text/core"
DICT      <- "output/text/dictionaries"
CACHE_DIR <- "output/cache"
OUT_PATH  <- file.path(CORE, "sentences_with_onet_scores.parquet")
BATCH     <- 256L

# ---- 41 GWA names + definitions ----
GWAs <- list(
  c("Getting Information","Observing, receiving, and otherwise obtaining information from all relevant sources."),
  c("Identifying Objects, Actions, and Events","Identifying information by categorizing, estimating, recognizing differences or similarities, and detecting changes in circumstances or events."),
  c("Monitoring Processes, Materials, or Surroundings","Monitoring and reviewing information from materials, events, or the environment, to detect or assess problems."),
  c("Inspecting Equipment, Structures, or Materials","Inspecting equipment, structures, or materials to identify the cause of errors or other problems or defects."),
  c("Estimating the Quantifiable Characteristics of Products, Events, or Information","Estimating sizes, distances, and quantities; or determining time, costs, resources, or materials needed to perform a work activity."),
  c("Judging the Qualities of Objects, Services, or People","Assessing the value, importance, or quality of things or people."),
  c("Evaluating Information to Determine Compliance with Standards","Using relevant information and individual judgment to determine whether events or processes comply with laws, regulations, or standards."),
  c("Processing Information","Compiling, coding, categorizing, calculating, tabulating, auditing, or verifying information or data."),
  c("Analyzing Data or Information","Identifying the underlying principles, reasons, or facts of information by breaking down information or data into separate parts."),
  c("Making Decisions and Solving Problems","Analyzing information and evaluating results to choose the best solution and solve problems."),
  c("Thinking Creatively","Developing, designing, or creating new applications, ideas, relationships, systems, or products, including artistic contributions."),
  c("Updating and Using Relevant Knowledge","Keeping up-to-date technically and applying new knowledge to your job."),
  c("Developing Objectives and Strategies","Establishing long-range objectives and specifying the strategies and actions to achieve them."),
  c("Scheduling Work and Activities","Scheduling events, programs, and activities, as well as the work of others."),
  c("Organizing, Planning, and Prioritizing Work","Developing specific goals and plans to prioritize, organize, and accomplish your work."),
  c("Performing General Physical Activities","Performing physical activities that require considerable use of your arms and legs and moving your whole body, such as climbing, lifting, balancing, walking, stooping, and handling materials."),
  c("Handling and Moving Objects","Using hands and arms in handling, installing, positioning, and moving materials, and manipulating things."),
  c("Controlling Machines and Processes","Using either control mechanisms or direct physical activity to operate machines or processes (not including computers or vehicles)."),
  c("Working with Computers","Using computers and computer systems (including hardware and software) to program, write software, set up functions, enter data, or process information."),
  c("Operating Vehicles, Mechanized Devices, or Equipment","Running, maneuvering, navigating, or driving vehicles or mechanized equipment, such as forklifts, passenger vehicles, aircraft, or water craft."),
  c("Drafting, Laying Out, and Specifying Technical Devices, Parts, and Equipment","Providing documentation, detailed instructions, drawings, or specifications to tell others about how devices, parts, equipment, or structures are to be fabricated, constructed, assembled, modified, maintained, or used."),
  c("Repairing and Maintaining Mechanical Equipment","Servicing, repairing, adjusting, and testing machines, devices, moving parts, and equipment that operate primarily on the basis of mechanical (not electronic) principles."),
  c("Repairing and Maintaining Electronic Equipment","Servicing, repairing, calibrating, regulating, fine-tuning, or testing machines, devices, and equipment that operate primarily on the basis of electrical or electronic (not mechanical) principles."),
  c("Documenting/Recording Information","Entering, transcribing, recording, storing, or maintaining information in written or electronic/magnetic form."),
  c("Interpreting the Meaning of Information for Others","Translating or explaining what information means and how it can be used."),
  c("Communicating with Supervisors, Peers, or Subordinates","Providing information to supervisors, co-workers, and subordinates by telephone, in written form, e-mail, or in person."),
  c("Communicating with People Outside the Organization","Communicating with people outside the organization, representing the organization to customers, the public, government, and other external sources."),
  c("Establishing and Maintaining Interpersonal Relationships","Developing constructive and cooperative working relationships with others, and maintaining them over time."),
  c("Assisting and Caring for Others","Providing personal assistance, medical attention, emotional support, or other personal care to others such as coworkers, customers, or patients."),
  c("Selling or Influencing Others","Convincing others to buy merchandise/goods or to otherwise change their minds or actions."),
  c("Resolving Conflicts and Negotiating with Others","Handling complaints, settling disputes, and resolving grievances and conflicts, or otherwise negotiating with others."),
  c("Performing for or Working Directly with the Public","Performing for people or dealing directly with the public. This includes serving customers in restaurants and stores, and receiving clients or guests."),
  c("Coordinating the Work and Activities of Others","Getting members of a group to work together to accomplish tasks."),
  c("Developing and Building Teams","Encouraging and building mutual trust, respect, and cooperation among team members."),
  c("Training and Teaching Others","Identifying the educational needs of others, developing formal educational or training programs or classes, and teaching or instructing others."),
  c("Guiding, Directing, and Motivating Subordinates","Providing guidance and direction to subordinates, including setting performance standards and monitoring performance."),
  c("Coaching and Developing Others","Identifying the developmental needs of others and coaching, mentoring, or otherwise helping others to improve their knowledge or skills."),
  c("Providing Consultation and Advice to Others","Providing guidance and expert advice to management or other groups on technical, systems-, or process-related topics."),
  c("Performing Administrative Activities","Performing day-to-day administrative tasks such as maintaining information files and processing paperwork."),
  c("Staffing Organizational Units","Recruiting, interviewing, selecting, hiring, and promoting employees in an organization."),
  c("Monitoring and Controlling Resources","Monitoring and controlling resources and overseeing the spending of money.")
)
gwa_names <- vapply(GWAs, `[`, character(1), 1)
gwa_text  <- vapply(GWAs, function(x) paste0(x[1], ": ", x[2]), character(1))
stopifnot(length(gwa_text) == 41)

# ---- short codes for column names (so parquet headers stay manageable) ----
slug <- function(x) {
  x <- stri_trans_general(x, "Latin-ASCII")
  x <- tolower(stri_replace_all_regex(x, "[^a-zA-Z0-9]+", "_"))
  x <- stri_replace_all_regex(x, "^_+|_+$", "")
  substr(x, 1, 32)
}
gwa_codes <- paste0("gwa_", slug(gwa_names))
stopifnot(!anyDuplicated(gwa_codes))

# ---- Load matrix and importance ----
M <- as.data.table(fread(file.path(DICT, "onet_gwa_x_abilities.tsv")))
ability_names <- setdiff(colnames(M), "GWA")
stopifnot(length(ability_names) == 52)
abil_codes <- paste0("abil_", slug(ability_names))
stopifnot(!anyDuplicated(abil_codes))

# Make matrix in correct order (rows = gwa_names, cols = ability_names)
setkey(M, GWA)
link <- as.matrix(M[gwa_names, ability_names, with = FALSE])
storage.mode(link) <- "double"

# ---- Importance scores per SOC ----
imp <- fread("output/cache/onet_relevance.tsv")
soc_list <- c("45-2091.00","45-2092.00","45-2093.00")
imp_gwa  <- matrix(0, length(soc_list), length(gwa_names),
                   dimnames = list(soc_list, gwa_names))
imp_abil <- matrix(0, length(soc_list), length(ability_names),
                   dimnames = list(soc_list, ability_names))
for (s in soc_list) {
  g <- imp[socCode == s & kind == "gwa"]
  a <- imp[socCode == s & kind == "abil"]
  ig <- setNames(g$importance, g$name)
  ia <- setNames(a$importance, a$name)
  miss_g <- setdiff(gwa_names, names(ig))
  miss_a <- setdiff(ability_names, names(ia))
  if (length(miss_g)) cat("WARN", s, "missing GWA importance for:", miss_g, "\n")
  if (length(miss_a)) cat("WARN", s, "missing ability importance for:", miss_a, "\n")
  imp_gwa[s,  intersect(gwa_names, names(ig))]      <- ig[intersect(gwa_names, names(ig))]
  imp_abil[s, intersect(ability_names, names(ia))]  <- ia[intersect(ability_names, names(ia))]
}

# ---- Load sentences and filter to the 3 SOCs ----
sents <- as.data.table(read_parquet(file.path(CORE, "sentences.parquet")))
sents <- sents[grepl("^45-209[123]", socCode)]
# Normalize socCode to "XX-XXXX.00" form for joining with importance
sents[, socCode_full := substr(socCode, 1, 7)]
sents[, socCode_full := paste0(socCode_full, ".00")]
sents <- sents[socCode_full %in% soc_list]
cat(sprintf("Sentences to score: %d across %d JOs across %d SOCs\n",
            nrow(sents), uniqueN(sents$caseNumber), uniqueN(sents$socCode_full)))
print(sents[, .N, by = socCode_full])

# ---- Encode GWAs (one-time) ----
cat("\nLoading MPNet model...\n")
model <- st$SentenceTransformer("sentence-transformers/all-mpnet-base-v2")
cat("Encoding 41 GWAs...\n")
gwa_emb <- model$encode(gwa_text, normalize_embeddings = TRUE,
                        show_progress_bar = FALSE)
stopifnot(all(dim(gwa_emb) == c(41, 768)))

# ---- Encode sentences in batches and compute scores ----
n <- nrow(sents)
cat(sprintf("\nEncoding %d sentences in batches of %d...\n", n, BATCH))

# Pre-allocate output blocks
gwa_scores  <- matrix(0, nrow = n, ncol = length(gwa_names),
                      dimnames = list(NULL, gwa_codes))
abil_scores <- matrix(0, nrow = n, ncol = length(ability_names),
                      dimnames = list(NULL, abil_codes))

t0 <- Sys.time()
for (start in seq.int(1, n, by = BATCH)) {
  end <- min(start + BATCH - 1, n)
  batch_text <- sents$sentence[start:end]
  emb <- model$encode(batch_text, normalize_embeddings = TRUE,
                      show_progress_bar = FALSE, batch_size = 64L)
  # cosine = dot product (both L2-normalized)
  cos_b <- emb %*% t(gwa_emb)         # rows = batch sentences, cols = 41 GWAs
  gwa_scores[start:end, ] <- cos_b

  # Per-row weighting by SOC importance
  socs_b <- sents$socCode_full[start:end]
  # Vectorize: build weighted GWA scores then propagate via link matrix
  # weighted_g[i, g] = cos_b[i, g] * imp_gwa[soc(i), g]
  imp_g_b <- imp_gwa[socs_b, , drop = FALSE]   # (batch, 41)
  weighted_g <- cos_b * imp_g_b                # (batch, 41)
  # ability raw = weighted_g %*% link  -> (batch, 52)
  abil_raw <- weighted_g %*% link
  imp_a_b  <- imp_abil[socs_b, , drop = FALSE] # (batch, 52)
  abil_scores[start:end, ] <- abil_raw * imp_a_b

  if (start %% (BATCH * 20) == 1 || end == n) {
    el <- as.numeric(Sys.time() - t0, units = "secs")
    rate <- end / el
    eta <- (n - end) / rate
    cat(sprintf("  %d / %d  (%.1fs elapsed, ~%.1f sent/s, ETA %.0fs)\n",
                end, n, el, rate, eta))
  }
}
cat(sprintf("Total encode time: %.1f min\n", (Sys.time() - t0) / 60))

# ---- Option 2 normalization: divide each ability column by its in-degree
# k_a = sum_g link[g, a]. Removes structural advantage of broadly-linked
# (cognitive) abilities. Skip columns with k_a = 0 (no GWA links to them).
k <- colSums(link)
for (i in seq_along(ability_names)) {
  if (k[i] > 0) abil_scores[, i] <- abil_scores[, i] / k[i]
}
cat(sprintf("Normalized by ability in-degree; %d columns left at raw 0 (k=0).\n",
            sum(k == 0)))

# ---- Assemble output ----
out <- cbind(
  sents[, .(caseNumber, sid, sentence, socCode = socCode_full,
            year, month, jobState, n_tc, n_farm, crops, n_crops)],
  as.data.table(gwa_scores),
  as.data.table(abil_scores)
)
write_parquet(out, OUT_PATH)
cat(sprintf("\nSaved %d rows x %d cols -> %s\n", nrow(out), ncol(out), OUT_PATH))

# ---- Tiny sanity print ----
cat("\nGWA-score summary (top 5 by mean cosine):\n")
gw_mean <- colMeans(gwa_scores)
print(head(sort(gw_mean, decreasing = TRUE), 5))
cat("\nAbility-score summary (top 5 by mean weighted score):\n")
ab_mean <- colMeans(abil_scores)
print(head(sort(ab_mean, decreasing = TRUE), 5))
