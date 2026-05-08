###############################################################################
# Temporary: re-render the crop-level scatter using hexbin + LOESS, just to see.
# Sources make_scatter.R for its data prep, then overrides the plot.
###############################################################################
MAKE_SCATTER_NO_DISPATCH <- TRUE
suppressPackageStartupMessages({
  library(data.table); library(ggplot2); library(scales); library(hexbin)
})
source("pipeline/make_scatter.R")

dt <- filter_and_plot(filter_expr_text = "TRUE",
                      out_name = "_tmp_crop_for_hex.png",
                      single_crop = FALSE,
                      jo_year_min = 2020L, jo_year_max = 2025L,
                      weight_cites = FALSE,
                      q1q2_only = FALSE,
                      group_by = "crop")

fig <- ggplot(dt, aes(x = exp_pct, y = fte_total)) +
  geom_hex(bins = 30, colour = NA) +
  scale_fill_viridis_c(option = "B", trans = "log10",
                       name = "Crops / hex", labels = label_comma()) +
  geom_smooth(method = "loess", span = 0.6, se = TRUE,
              colour = "white", fill = "white", alpha = 0.25,
              linewidth = 0.7) +
  scale_y_log10(labels = label_comma()) +
  labs(title    = "Crop H-2A FTE vs mechanization-patent exposure (hexbin + LOESS)",
       subtitle = sprintf("%d crops", nrow(dt)),
       x = "Exposure-to-mechanization-patents percentile",
       y = "H-2A FTE workers (log scale)") +
  theme_minimal(base_size = 11)

ggsave("output/results/scatter_crop_hex_test.png", fig,
       width = 10, height = 6.5, dpi = 150)
cat("Saved output/results/scatter_crop_hex_test.png\n")
