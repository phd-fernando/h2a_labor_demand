# Compare two patent-filter scatter views with arrows for highlighted crops.

suppressPackageStartupMessages({
  library(data.table); library(arrow); library(ggplot2); library(ggrepel)
  library(ggforce); library(scales); library(cluster)
})

ml_ids <- readRDS("output/cache/ai_ml_ids.rds")
source("pipeline/make_scatter.R")

dt_a <- filter_and_plot(
  filter_expr_text = "patent_id %in% ml_ids & year >= 2018 & year <= 2020",
  out_name = "_tmp_compare_a.png", single_crop = TRUE,
  jo_year_min = 2020L, jo_year_max = 2022L, weight_cites = TRUE)

dt_b <- filter_and_plot(
  filter_expr_text = "patent_id %in% ml_ids & year >= 2018 & year <= 2023",
  out_name = "_tmp_compare_b.png", single_crop = TRUE,
  jo_year_min = 2023L, jo_year_max = 2025L, weight_cites = TRUE)

# Merge on crop_canonical: A = (patents 2018-2020, FTE 2020-2022), B = (patents 2018-2023, FTE 2023-2025)
m <- merge(
  dt_a[, .(crop_canonical, exp_a = exp_pct, fte_a = fte_total)],
  dt_b[, .(crop_canonical, exp_b = exp_pct, fte_b = fte_total)],
  by = "crop_canonical")
m[, target := crop_canonical %in% c("lettuce","corn","apple","strawberry")]
cat("Crops in both:", nrow(m), "\n")

# k-means cluster on the 'after' state (B)
X <- as.matrix(m[, .(exp_b, log_fte_b = log10(fte_b))])
set.seed(7)
km <- kmeans(X, centers = min(5, nrow(m) - 1), nstart = 25)
m[, cluster := factor(km$cluster)]

fig <- ggplot(m, aes(x = exp_b, y = fte_b)) +
  geom_mark_hull(aes(group = cluster, fill = cluster),
                 expand = unit(3, "mm"),
                 alpha = 0.08, colour = NA, concavity = 5) +
  # arrows for highlighted crops: A -> B (both x and y move)
  geom_segment(data = m[target == TRUE],
               aes(x = exp_a, xend = exp_b, y = fte_a, yend = fte_b),
               arrow = arrow(length = unit(2.5, "mm"), type = "closed"),
               colour = "firebrick", alpha = 0.8, linewidth = 0.7) +
  # A ghost points (start) for highlighted crops
  geom_point(data = m[target == TRUE],
             aes(x = exp_a, y = fte_a),
             colour = "firebrick", size = 1.8, alpha = 0.5, shape = 21) +
  # B points for everyone (end state)
  geom_point(aes(colour = target), size = 2.2, alpha = .85) +
  geom_text_repel(aes(label = crop_canonical, colour = target),
                  size = 2.7, max.overlaps = Inf,
                  segment.size = 0.2, segment.alpha = 0.4,
                  min.segment.length = 0.1, force = 1.2) +
  scale_color_manual(values = c("FALSE" = "grey45", "TRUE" = "firebrick"),
                     guide = "none") +
  scale_fill_brewer(palette = "Set2", guide = "none") +
  scale_y_log10(labels = label_comma()) +
  labs(title    = "AI-ML patents x H-2A workforce: shift over time",
       subtitle = "Start: patents 2018-2020, FTE 2020-2022. End: patents 2018-2023, FTE 2023-2025. Single-crop, citation-weighted.",
       x = "Exposure percentile",
       y = "H-2A FTE workers (log scale)") +
  theme_minimal(base_size = 11)

OUT <- "output/results/scatter_ai_ml_compare_diag.png"
ggsave(OUT, fig, width = 10, height = 6.5, dpi = 150)
cat("Saved:", OUT, "\n")
