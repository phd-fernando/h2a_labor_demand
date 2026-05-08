###############################################################################
# Demo: match a JO sentence to the 41 O*NET GWAs via MPNet embeddings.
###############################################################################
suppressPackageStartupMessages({library(reticulate); library(data.table)})
# Use the system Python where sentence-transformers is installed
use_python("C:/Users/Fer/AppData/Local/Programs/Python/Python310/python.exe", required = TRUE)

st <- import("sentence_transformers")
np <- import("numpy")
model <- st$SentenceTransformer("sentence-transformers/all-mpnet-base-v2")

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
gwa_names <- sapply(GWAs, `[`, 1)
gwa_text  <- sapply(GWAs, function(x) paste0(x[1], ": ", x[2]))
stopifnot(length(gwa_text) == 41)

# Encode the 41 GWAs (one-time). Returns R matrix (41 x 768) when reticulate auto-converts.
cat("Encoding 41 GWAs...\n")
gwa_emb <- model$encode(gwa_text, normalize_embeddings = TRUE)
cat("  shape:", dim(gwa_emb), "\n")

# Pick a random JO from SOC 45-2092 with at least 5 farm sentences
suppressPackageStartupMessages(library(arrow))
sents <- as.data.table(read_parquet("output/text/core/sentences.parquet"))
sents <- sents[grepl("^45-2092", socCode)]
jo_counts <- sents[, .N, by = caseNumber][N >= 5]
set.seed(as.integer(Sys.time()) %% 100000L)
pick <- jo_counts[sample(.N, 1)]
jo_sents <- sents[caseNumber == pick$caseNumber][order(sid)]
cat(sprintf("\n=== JO %s | %s | %d farm sentences ===\n",
            jo_sents$caseNumber[1], jo_sents$jobState[1], nrow(jo_sents)))

# Encode all sentences in batch
sent_emb <- model$encode(jo_sents$sentence, normalize_embeddings = TRUE)
sim <- sent_emb %*% t(gwa_emb)   # n_sent x 41

for (i in seq_len(nrow(jo_sents))) {
  s <- jo_sents$sentence[i]
  ord <- order(sim[i, ], decreasing = TRUE)
  k <- ord[1]
  cat(sprintf("\n[%d] %s\n", i, s))
  cat(sprintf("    --> [%.3f] %s\n", sim[i, k], gwa_names[k]))
}
