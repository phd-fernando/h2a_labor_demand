###############################################################################
# Build a 0/1 matrix linking O*NET items (Tasks, DWAs, Generalized Work
# Activities) for SOC 45-2092.00 to the canonical 52 O*NET abilities.
#
# Source page: https://www.onetonline.org/link/details/45-2092.00
# (LLM-curated mapping; O*NET does NOT publish a per-task ability link.
#  Each cell = my judgment from reading the item text against ability
#  definitions. See _make_onet_matrix_log.md for the rationale.)
###############################################################################
suppressPackageStartupMessages({library(data.table)})

ABIL <- c(
  "Oral Comprehension","Written Comprehension","Oral Expression","Written Expression",
  "Fluency of Ideas","Originality","Problem Sensitivity","Deductive Reasoning",
  "Inductive Reasoning","Information Ordering","Category Flexibility",
  "Mathematical Reasoning","Number Facility","Memorization","Speed of Closure",
  "Flexibility of Closure","Perceptual Speed","Spatial Orientation","Visualization",
  "Selective Attention","Time Sharing","Arm-Hand Steadiness","Manual Dexterity",
  "Finger Dexterity","Control Precision","Multilimb Coordination",
  "Response Orientation","Rate Control","Reaction Time","Wrist-Finger Speed",
  "Speed of Limb Movement","Static Strength","Explosive Strength","Dynamic Strength",
  "Trunk Strength","Stamina","Extent Flexibility","Dynamic Flexibility",
  "Gross Body Coordination","Gross Body Equilibrium","Near Vision","Far Vision",
  "Visual Color Discrimination","Night Vision","Peripheral Vision","Depth Perception",
  "Glare Sensitivity","Hearing Sensitivity","Auditory Attention","Sound Localization",
  "Speech Recognition","Speech Clarity"
)
stopifnot(length(ABIL) == 52)

# ---- Items (verbatim from O*NET 45-2092.00) ----
TASKS <- c(
  "Record information about crops, such as pesticide use, yields, or costs.",
  "Direct and monitor the work of casual and seasonal help during planting and harvesting.",
  "Participate in the inspection, grading, sorting, storage, and post-harvest treatment of crops.",
  "Harvest plants, and transplant or pot and label them.",
  "Repair and maintain farm vehicles, implements, and mechanical equipment.",
  "Harvest fruits and vegetables by hand.",
  "Set up and operate irrigation equipment.",
  "Inform farmers or farm managers of crop progress.",
  "Identify plants, pests, and weeds to determine the selection and application of pesticides and fertilizers.",
  "Operate tractors, tractor-drawn machinery, and self-propelled machinery to plow, harrow and fertilize soil, or to plant, cultivate, spray and harvest crops.",
  "Load agricultural products into trucks, and drive trucks to market or storage facilities.",
  "Clean work areas, and maintain grounds and landscaping.",
  "Sell and deliver plants and flowers to customers.",
  "Regulate greenhouse conditions, and indoor and outdoor irrigation systems.",
  "Feel plants' leaves and note their coloring to detect the presence of insects or disease.",
  "Provide information and advice to the public regarding the selection, purchase, and care of products.",
  "Maintain and repair irrigation and climate control systems.",
  "Dig, cut, and transplant seedlings, cuttings, trees, and shrubs.",
  "Record information about plants and plant growth.",
  "Maintain inventory, ordering materials as required.",
  "Dig, rake, and screen soil, filling cold frames and hot beds in preparation for planting.",
  "Inspect plants and bud ties to assess quality.",
  "Move containerized shrubs, plants, and trees, using wheelbarrows or tractors.",
  "Tie and bunch flowers, plants, shrubs, and trees, wrap their roots, and pack them into boxes to fill orders.",
  "Haul and spread topsoil, fertilizer, peat moss, and other materials to condition soil, using wheelbarrows or carts and shovels.",
  "Repair farm buildings, fences, and other structures.",
  "Plant, spray, weed, fertilize, water, and prune plants, shrubs, and trees, using gardening tools."
)
DWAS <- c(
  "Transport animals, crops, or equipment.",
  "Sell agricultural products.",
  "Maintain operational records.",
  "Direct activities of agricultural, forestry, or fishery employees.",
  "Harvest agricultural products.",
  "Mark agricultural or forestry products for identification.",
  "Sort forestry or agricultural materials.",
  "Operate irrigation systems.",
  "Evaluate quality of plants or crops.",
  "Maintain forestry, hunting, or agricultural equipment.",
  "Advise others on farming or forestry operations, regulations, or equipment.",
  "Build agricultural structures.",
  "Confer with managers to make operational decisions.",
  "Cut trees or logs.",
  "Plant crops, trees, or other plants.",
  "Examine characteristics or behavior of living organisms.",
  "Operate farming equipment.",
  "Maintain inventories of materials, equipment, or products.",
  "Prepare land for agricultural use.",
  "Load agricultural or forestry products for shipment.",
  "Package agricultural products for shipment or further processing.",
  "Clean equipment or facilities.",
  "Perform manual agricultural, aquacultural, or horticultural tasks."
)
GWAS <- c(
  "Performing General Physical Activities",
  "Handling and Moving Objects",
  "Getting Information",
  "Identifying Objects, Actions, and Events",
  "Communicating with Supervisors, Peers, or Subordinates",
  "Controlling Machines and Processes",
  "Operating Vehicles, Mechanized Devices, or Equipment",
  "Updating and Using Relevant Knowledge",
  "Inspecting Equipment, Structures, or Materials",
  "Making Decisions and Solving Problems",
  "Repairing and Maintaining Mechanical Equipment",
  "Establishing and Maintaining Interpersonal Relationships",
  "Evaluating Information to Determine Compliance with Standards",
  "Organizing, Planning, and Prioritizing Work",
  "Training and Teaching Others",
  "Analyzing Data or Information",
  "Estimating the Quantifiable Characteristics of Products, Events, or Information",
  "Communicating with People Outside the Organization",
  "Judging the Qualities of Objects, Services, or People",
  "Monitoring Processes, Materials, or Surroundings",
  "Developing Objectives and Strategies",
  "Processing Information",
  "Thinking Creatively",
  "Documenting/Recording Information",
  "Monitoring and Controlling Resources",
  "Coordinating the Work and Activities of Others",
  "Coaching and Developing Others",
  "Scheduling Work and Activities",
  "Developing and Building Teams",
  "Interpreting the Meaning of Information for Others",
  "Performing for or Working Directly with the Public",
  "Providing Consultation and Advice to Others",
  "Assisting and Caring for Others",
  "Resolving Conflicts and Negotiating with Others",
  "Guiding, Directing, and Motivating Subordinates",
  "Selling or Influencing Others",
  "Working with Computers",
  "Performing Administrative Activities",
  "Repairing and Maintaining Electronic Equipment",
  "Drafting, Laying Out, and Specifying Technical Devices, Parts, and Equipment",
  "Staffing Organizational Units"
)

# ---- Per-item ability sets (LLM judgment based on item text) ----
T_MAP <- list(
  c("Written Expression","Information Ordering","Number Facility","Memorization"),
  c("Oral Expression","Speech Clarity","Selective Attention","Problem Sensitivity","Deductive Reasoning"),
  c("Near Vision","Visual Color Discrimination","Selective Attention","Perceptual Speed","Manual Dexterity","Finger Dexterity"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Trunk Strength","Extent Flexibility","Stamina"),
  c("Manual Dexterity","Arm-Hand Steadiness","Finger Dexterity","Problem Sensitivity","Deductive Reasoning","Visualization"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Trunk Strength","Extent Flexibility","Static Strength","Stamina","Wrist-Finger Speed"),
  c("Manual Dexterity","Arm-Hand Steadiness","Static Strength","Control Precision"),
  c("Oral Expression","Speech Clarity","Written Expression"),
  c("Near Vision","Visual Color Discrimination","Category Flexibility","Inductive Reasoning","Memorization","Flexibility of Closure"),
  c("Control Precision","Multilimb Coordination","Rate Control","Response Orientation","Far Vision","Reaction Time","Spatial Orientation","Depth Perception"),
  c("Static Strength","Stamina","Trunk Strength","Control Precision","Multilimb Coordination","Far Vision","Spatial Orientation","Depth Perception"),
  c("Stamina","Static Strength","Manual Dexterity","Trunk Strength","Extent Flexibility"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension","Static Strength"),
  c("Control Precision","Selective Attention","Number Facility","Problem Sensitivity"),
  c("Near Vision","Visual Color Discrimination","Finger Dexterity","Arm-Hand Steadiness","Perceptual Speed","Flexibility of Closure"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension","Memorization"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Problem Sensitivity","Deductive Reasoning"),
  c("Manual Dexterity","Arm-Hand Steadiness","Finger Dexterity","Trunk Strength","Extent Flexibility","Static Strength","Stamina","Dynamic Strength"),
  c("Written Expression","Information Ordering","Memorization","Near Vision"),
  c("Information Ordering","Number Facility","Memorization","Written Expression","Mathematical Reasoning"),
  c("Static Strength","Stamina","Trunk Strength","Manual Dexterity","Extent Flexibility","Dynamic Strength"),
  c("Near Vision","Visual Color Discrimination","Selective Attention","Perceptual Speed","Flexibility of Closure"),
  c("Static Strength","Stamina","Trunk Strength","Multilimb Coordination","Control Precision","Gross Body Coordination"),
  c("Finger Dexterity","Manual Dexterity","Arm-Hand Steadiness","Wrist-Finger Speed","Near Vision"),
  c("Static Strength","Stamina","Dynamic Strength","Trunk Strength","Manual Dexterity","Multilimb Coordination","Extent Flexibility"),
  c("Manual Dexterity","Arm-Hand Steadiness","Static Strength","Trunk Strength","Stamina","Visualization","Problem Sensitivity","Deductive Reasoning"),
  c("Manual Dexterity","Arm-Hand Steadiness","Finger Dexterity","Trunk Strength","Extent Flexibility","Stamina","Static Strength")
)
stopifnot(length(T_MAP) == length(TASKS))

D_MAP <- list(
  c("Static Strength","Stamina","Trunk Strength","Multilimb Coordination","Gross Body Coordination"),
  c("Oral Expression","Speech Clarity","Number Facility"),
  c("Written Expression","Information Ordering","Memorization"),
  c("Oral Expression","Speech Clarity","Selective Attention","Problem Sensitivity","Deductive Reasoning"),
  c("Manual Dexterity","Finger Dexterity","Trunk Strength","Extent Flexibility","Static Strength","Stamina"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Near Vision"),
  c("Near Vision","Visual Color Discrimination","Manual Dexterity","Finger Dexterity","Selective Attention","Perceptual Speed"),
  c("Manual Dexterity","Arm-Hand Steadiness","Static Strength","Control Precision"),
  c("Near Vision","Visual Color Discrimination","Selective Attention","Perceptual Speed","Flexibility of Closure"),
  c("Manual Dexterity","Arm-Hand Steadiness","Finger Dexterity","Problem Sensitivity","Deductive Reasoning"),
  c("Oral Expression","Speech Clarity","Memorization","Written Expression"),
  c("Manual Dexterity","Arm-Hand Steadiness","Static Strength","Trunk Strength","Stamina","Visualization"),
  c("Oral Expression","Speech Recognition","Oral Comprehension","Speech Clarity"),
  c("Manual Dexterity","Arm-Hand Steadiness","Static Strength","Dynamic Strength","Trunk Strength","Multilimb Coordination"),
  c("Manual Dexterity","Finger Dexterity","Trunk Strength","Extent Flexibility","Stamina"),
  c("Near Vision","Visual Color Discrimination","Perceptual Speed","Selective Attention","Flexibility of Closure"),
  c("Control Precision","Multilimb Coordination","Rate Control","Response Orientation","Far Vision","Reaction Time","Depth Perception","Spatial Orientation"),
  c("Information Ordering","Number Facility","Memorization","Written Expression"),
  c("Static Strength","Stamina","Trunk Strength","Multilimb Coordination","Manual Dexterity"),
  c("Static Strength","Stamina","Trunk Strength","Multilimb Coordination","Gross Body Coordination","Manual Dexterity"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Wrist-Finger Speed"),
  c("Stamina","Static Strength","Manual Dexterity","Extent Flexibility"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Trunk Strength","Extent Flexibility","Stamina","Static Strength")
)
stopifnot(length(D_MAP) == length(DWAS))

G_MAP <- list(
  c("Static Strength","Stamina","Trunk Strength","Extent Flexibility","Gross Body Coordination","Dynamic Strength"),
  c("Manual Dexterity","Static Strength","Trunk Strength","Multilimb Coordination","Arm-Hand Steadiness"),
  c("Oral Comprehension","Written Comprehension","Near Vision","Information Ordering"),
  c("Near Vision","Visual Color Discrimination","Perceptual Speed","Flexibility of Closure"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension"),
  c("Control Precision","Multilimb Coordination","Rate Control","Reaction Time","Selective Attention","Response Orientation"),
  c("Control Precision","Multilimb Coordination","Rate Control","Far Vision","Depth Perception","Spatial Orientation","Reaction Time","Response Orientation"),
  c("Memorization","Information Ordering","Written Comprehension"),
  c("Near Vision","Selective Attention","Perceptual Speed","Visual Color Discrimination","Problem Sensitivity","Flexibility of Closure"),
  c("Deductive Reasoning","Inductive Reasoning","Problem Sensitivity","Information Ordering"),
  c("Manual Dexterity","Arm-Hand Steadiness","Finger Dexterity","Problem Sensitivity","Visualization","Deductive Reasoning"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension"),
  c("Deductive Reasoning","Information Ordering","Selective Attention","Problem Sensitivity","Flexibility of Closure"),
  c("Information Ordering","Time Sharing","Deductive Reasoning","Category Flexibility"),
  c("Oral Expression","Speech Clarity","Information Ordering","Memorization"),
  c("Inductive Reasoning","Deductive Reasoning","Mathematical Reasoning","Information Ordering","Number Facility"),
  c("Number Facility","Mathematical Reasoning","Visualization","Deductive Reasoning"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension"),
  c("Visual Color Discrimination","Near Vision","Perceptual Speed","Deductive Reasoning","Flexibility of Closure"),
  c("Selective Attention","Time Sharing","Near Vision","Far Vision","Auditory Attention","Problem Sensitivity"),
  c("Inductive Reasoning","Deductive Reasoning","Information Ordering","Originality","Fluency of Ideas"),
  c("Information Ordering","Number Facility","Mathematical Reasoning","Memorization"),
  c("Originality","Fluency of Ideas","Visualization","Category Flexibility"),
  c("Written Expression","Information Ordering","Memorization"),
  c("Information Ordering","Number Facility","Selective Attention","Problem Sensitivity"),
  c("Oral Expression","Speech Clarity","Time Sharing","Selective Attention"),
  c("Oral Expression","Speech Clarity","Memorization"),
  c("Information Ordering","Time Sharing","Number Facility"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension"),
  c("Oral Expression","Speech Clarity","Written Expression","Oral Comprehension","Written Comprehension"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension"),
  c("Oral Expression","Speech Clarity","Memorization","Deductive Reasoning"),
  c("Oral Comprehension","Oral Expression","Speech Recognition"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension","Problem Sensitivity"),
  c("Oral Expression","Speech Clarity","Selective Attention"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension"),
  c("Finger Dexterity","Near Vision","Wrist-Finger Speed","Information Ordering"),
  c("Information Ordering","Memorization","Written Expression","Number Facility"),
  c("Manual Dexterity","Finger Dexterity","Arm-Hand Steadiness","Visualization","Problem Sensitivity"),
  c("Visualization","Information Ordering","Spatial Orientation"),
  c("Oral Expression","Speech Clarity","Speech Recognition","Oral Comprehension")
)
stopifnot(length(G_MAP) == length(GWAS))

# ---- Validation: every mapped ability must exist in canonical list ----
all_mapped <- unique(c(unlist(T_MAP), unlist(D_MAP), unlist(G_MAP)))
bad <- setdiff(all_mapped, ABIL)
if (length(bad)) stop("Unknown abilities in mapping: ", paste(bad, collapse=", "))

# ---- Build matrix ----
build_block <- function(items, maps, source_label) {
  M <- matrix(0L, nrow = length(items), ncol = length(ABIL),
              dimnames = list(NULL, ABIL))
  for (i in seq_along(items)) M[i, maps[[i]]] <- 1L
  data.table(source = source_label, id = seq_along(items),
             text = items, as.data.table(M))
}
T_dt <- build_block(TASKS, T_MAP, "task")
D_dt <- build_block(DWAS,  D_MAP, "dwa")
G_dt <- build_block(GWAS,  G_MAP, "gwa")
mat  <- rbindlist(list(T_dt, D_dt, G_dt))

# ---- Output: wide TSV ----
out_dir <- "output/text/dictionaries"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
fwrite(mat, file.path(out_dir, "onet_45_2092_item_ability_matrix.tsv"), sep = "\t")

# ---- Output: long TSV (item, ability, link) ----
long <- melt(mat, id.vars = c("source","id","text"),
             variable.name = "ability", value.name = "link")
fwrite(long, file.path(out_dir, "onet_45_2092_item_ability_long.tsv"), sep = "\t")

# ---- Summary ----
cat(sprintf("Items: %d (tasks=%d, dwas=%d, gwas=%d)\n",
            nrow(mat), nrow(T_dt), nrow(D_dt), nrow(G_dt)))
cat(sprintf("Abilities: %d\n", length(ABIL)))
cat(sprintf("Cells set to 1: %d / %d (%.1f%%)\n",
            sum(long$link), nrow(long), 100*mean(long$link)))
cat("\n--- Coverage per ability ---\n")
ab_cov <- long[, .(n_items = sum(link)), by = ability][order(-n_items)]
print(ab_cov, nrows = Inf)
cat("\n--- Items with most abilities ---\n")
mat[, n_ab := rowSums(.SD), .SDcols = ABIL]
print(mat[order(-n_ab), .(source, id, n_ab, text)][1:15], nrows = Inf)
cat("\n--- Items with fewest abilities ---\n")
print(mat[order(n_ab), .(source, id, n_ab, text)][1:5], nrows = Inf)
