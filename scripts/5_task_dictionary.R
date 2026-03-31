###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# H-2A Agricultural Task Dictionary
# Builds a structured dictionary of unique agricultural tasks (verb + object)
# from the verb-object pairs extracted in Step 2.
#
# Outputs:
#   task_dictionary.csv         — full dictionary with frequency
#   plot_task_treemap.png       — treemap: verbs → objects, sized by freq
#   plot_task_network.png       — bipartite network: verbs ↔ top objects
#   plot_task_wordcloud.png     — wordcloud of task phrases
#   plot_task_heatmap.png       — heatmap: top verbs × top objects
###############################################################################

cat("\014"); rm(list = ls())

mydir  <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
out_dir <- paste0(mydir, "/processed/text")
img_dir <- paste0(mydir, "/processed/images")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "ggrepel", "wordcloud", "RColorBrewer",
              "igraph", "ggraph", "treemapify")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ------------------------------------------------------------------------------
# 1. Load verb-object pairs from Step 2
# ------------------------------------------------------------------------------
tasks <- readRDS(paste0(out_dir, "/tasks_jo.rds"))
vo    <- tasks$verb_obj   # caseNumber, verb_base, obj_lemma, period, dateSubmitted, year, quarter

# ------------------------------------------------------------------------------
# 2. Build dictionary: unique (verb, object) pairs with corpus frequency
#    n_docs  = number of distinct job orders containing this task
#    n_total = total occurrences across corpus
# ------------------------------------------------------------------------------
dict <- vo %>%
  group_by(verb_base, obj_lemma) %>%
  summarise(
    n_docs  = n_distinct(caseNumber),
    n_total = n(),
    .groups = "drop"
  ) %>%
  mutate(task = paste(verb_base, obj_lemma)) %>%
  arrange(desc(n_docs))

cat("=== Task Dictionary Summary ===\n")
cat("Unique (verb, object) pairs:", nrow(dict), "\n")
cat("Unique verbs:               ", n_distinct(dict$verb_base), "\n")
cat("Unique objects:             ", n_distinct(dict$obj_lemma), "\n\n")

# frequency-filtered tiers
cat("Pairs appearing in >= 1 job order:  ", nrow(dict), "\n")
cat("Pairs appearing in >= 2 job orders: ", sum(dict$n_docs >= 2), "\n")
cat("Pairs appearing in >= 5 job orders: ", sum(dict$n_docs >= 5), "\n")
cat("Pairs appearing in >= 10 job orders:", sum(dict$n_docs >= 10), "\n\n")

# save full dictionary
write.csv(dict, paste0(out_dir, "/task_dictionary.csv"), row.names = FALSE)
cat("Full dictionary saved to task_dictionary.csv\n\n")

# working subset: tasks appearing in >= 2 job orders
dict2 <- filter(dict, n_docs >= 2)

# ------------------------------------------------------------------------------
# 3. Treemap — verbs as groups, objects sized by n_docs
#    Top 12 verbs for readability
# ------------------------------------------------------------------------------
top_verbs <- dict2 %>%
  group_by(verb_base) %>%
  summarise(verb_total = sum(n_docs), .groups = "drop") %>%
  slice_max(verb_total, n = 12) %>%
  pull(verb_base)

treemap_data <- dict2 %>%
  filter(verb_base %in% top_verbs) %>%
  group_by(verb_base) %>%
  slice_max(n_docs, n = 20) %>%   # top 20 objects per verb
  ungroup()

p_tree <- ggplot(treemap_data,
                 aes(area = n_docs, fill = verb_base,
                     label = obj_lemma, subgroup = verb_base)) +
  geom_treemap() +
  geom_treemap_subgroup_border(colour = "white", size = 2) +
  geom_treemap_subgroup_text(
    place = "topleft", colour = "white", fontface = "bold",
    size = 13, alpha = 0.9, grow = FALSE
  ) +
  geom_treemap_text(
    colour = "white", place = "centre",
    size = 9, grow = FALSE, reflow = TRUE
  ) +
  scale_fill_brewer(palette = "Paired") +
  labs(title = "H-2A Agricultural Task Dictionary",
       subtitle = "Top 12 verbs · top 20 objects each · sized by job-order frequency") +
  theme_minimal(base_size = 12) +
  theme(legend.position = "none")

ggsave(paste0(img_dir, "/plot_task_treemap.png"), p_tree, width = 14, height = 10)
cat("Saved: plot_task_treemap.png\n")

# ------------------------------------------------------------------------------
# 4. Bipartite network graph — top 10 verbs, top 8 objects per verb
# ------------------------------------------------------------------------------
net_data <- dict2 %>%
  filter(verb_base %in% top_verbs[1:10]) %>%
  group_by(verb_base) %>%
  slice_max(n_docs, n = 8) %>%
  ungroup()

# build igraph bipartite graph
edges <- net_data %>% select(from = verb_base, to = obj_lemma, weight = n_docs)
nodes_v <- tibble(name = unique(edges$from), type = "verb")
nodes_o <- tibble(name = unique(edges$to),   type = "object")
nodes   <- bind_rows(nodes_v, nodes_o)

g <- graph_from_data_frame(edges, directed = FALSE, vertices = nodes)

set.seed(42)
p_net <- ggraph(g, layout = "fr") +
  geom_edge_link(aes(width = weight), alpha = 0.25, colour = "grey50") +
  geom_node_point(aes(colour = type, size = type)) +
  geom_node_text(aes(label = name, colour = type),
                 repel = TRUE, size = 3.2, fontface = "bold",
                 max.overlaps = 40) +
  scale_colour_manual(values = c("verb" = "#d7191c", "object" = "#2c7bb6")) +
  scale_size_manual(values  = c("verb" = 6,          "object" = 3)) +
  scale_edge_width(range = c(0.3, 3)) +
  labs(title    = "H-2A Task Network",
       subtitle = "Red = agricultural verb  ·  Blue = object  ·  Edge width = frequency") +
  theme_graph(base_size = 12) +
  theme(legend.position = "none")

ggsave(paste0(img_dir, "/plot_task_network.png"), p_net, width = 14, height = 10)
cat("Saved: plot_task_network.png\n")

# ------------------------------------------------------------------------------
# 5. Wordcloud — task phrases (verb + object), sized by n_docs
# ------------------------------------------------------------------------------
wc_data <- dict2 %>% slice_max(n_docs, n = 200)

pal <- brewer.pal(8, "Dark2")
png(paste0(img_dir, "/plot_task_wordcloud.png"), width = 1400, height = 1000, res = 150)
wordcloud(
  words  = wc_data$task,
  freq   = wc_data$n_docs,
  min.freq = 1,
  max.words = 200,
  random.order = FALSE,
  rot.per = 0.15,
  colors = pal,
  scale  = c(3.5, 0.5)
)
title(main = "H-2A Agricultural Task Dictionary — Top 200 Tasks",
      cex.main = 1.2)
dev.off()
cat("Saved: plot_task_wordcloud.png\n")

# ------------------------------------------------------------------------------
# 6. Heatmap — top 15 verbs × top 20 objects (by overall freq)
# ------------------------------------------------------------------------------
top15_verbs <- dict2 %>%
  group_by(verb_base) %>% summarise(s = sum(n_docs), .groups = "drop") %>%
  slice_max(s, n = 15) %>% pull(verb_base)

top20_objs <- dict2 %>%
  filter(verb_base %in% top15_verbs) %>%
  group_by(obj_lemma) %>% summarise(s = sum(n_docs), .groups = "drop") %>%
  slice_max(s, n = 20) %>% pull(obj_lemma)

heat_data <- dict2 %>%
  filter(verb_base %in% top15_verbs, obj_lemma %in% top20_objs) %>%
  complete(verb_base, obj_lemma, fill = list(n_docs = 0))

p_heat <- heat_data %>%
  mutate(
    verb_base = fct_reorder(verb_base, n_docs, sum),
    obj_lemma = fct_reorder(obj_lemma, n_docs, sum)
  ) %>%
  ggplot(aes(obj_lemma, verb_base, fill = n_docs)) +
  geom_tile(colour = "white", linewidth = 0.4) +
  geom_text(aes(label = ifelse(n_docs > 0, n_docs, "")),
            size = 2.5, colour = "white") +
  scale_fill_gradient(low = "#f7fbff", high = "#08306b", name = "Job orders") +
  labs(title    = "H-2A Task Frequency Heatmap",
       subtitle = "Top 15 verbs × top 20 objects · cell = number of job orders",
       x = NULL, y = NULL) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(paste0(img_dir, "/plot_task_heatmap.png"), p_heat, width = 13, height = 8)
cat("Saved: plot_task_heatmap.png\n")

# ------------------------------------------------------------------------------
# 7. Summary table: top 30 tasks
# ------------------------------------------------------------------------------
cat("\n=== Top 30 tasks by job-order coverage ===\n")
print(as.data.frame(head(dict, 30)))
