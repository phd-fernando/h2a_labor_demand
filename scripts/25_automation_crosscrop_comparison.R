###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Cross-crop comparison of automation signals across all 7 specialty crops:
#   strawberry, citrus, blueberry, tobacco, tomato, lettuce, watermelon
#
# Reads automation_signals_{crop}.rds produced by scripts 18–24.
# Produces:
#   (A) Summary table: corpus size, Step 1 mechanization rate, Step 2
#       hand/machine ratio start/end + trend, Step 3 top neighbors
#   (B) Figure panel:
#       - Lollipop: Step 1 Tier B % by crop (ordered)
#       - Line: hand/machine ratio 2020–2025, one line per crop
#       - Faceted lollipop: Step 3 top-10 semantic neighbors per crop
#
# Output: processed/images/plot_crosscrop_automation_comparison.png
#         processed/text/automation_crosscrop_summary.csv
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "tidytext", "scales", "patchwork", "ggrepel")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ── Crop order (by corpus size, descending) ───────────────────────────────────
slugs <- c("citrus","watermelon","tomato","lettuce","strawberry","blueberry","tobacco",
           "apple","grape","cherry","brassica","cucumber")

# ── Load all RDS files ─────────────────────────────────────────────────────────
rds_list <- lapply(slugs, function(s) {
  readRDS(sprintf("processed/text/automation_signals_%s.rds", s))
})
names(rds_list) <- slugs

# ── Step 1: Mechanization rates ───────────────────────────────────────────────
# mentions is the JO-level detection tibble with any_tierA, any_tierB, any_tierC cols
step1 <- map_dfr(slugs, function(s) {
  d <- rds_list[[s]]
  m <- d$mentions  # one row per JO
  crop_name <- if (length(d$crop) == 0 || nchar(d$crop) == 0) str_to_title(s) else d$crop
  tibble(
    crop     = crop_name,
    slug     = s,
    n_jos    = nrow(m),
    n_any    = sum(m$any_auto,  na.rm = TRUE),
    n_tierA  = sum(m$any_tierA, na.rm = TRUE),
    n_tierB  = sum(m$any_tierB, na.rm = TRUE),
    n_tierC  = sum(m$any_tierC, na.rm = TRUE),
    pct_mech = round(sum(m$any_tierB, na.rm = TRUE) / nrow(m) * 100, 1)
  )
}) |>
  arrange(desc(pct_mech))

cat("=== STEP 1: Mechanization rates by crop ===\n")
print(step1)

# ── Step 2: Hand/machine ratios ───────────────────────────────────────────────
step2_ratios <- map_dfr(slugs, function(s) {
  d <- rds_list[[s]]
  crop_name <- if (length(d$crop) == 0 || nchar(d$crop) == 0) str_to_title(s) else d$crop
  d$ratio_df |>
    mutate(crop = crop_name, slug = s) |>
    select(crop, slug, year, `hand-task`, `machine-op`, hand_machine_ratio)
})

# Summary: first and last year ratio + direction
step2_summary <- step2_ratios |>
  filter(year >= 2020, year <= 2025) |>
  group_by(crop, slug) |>
  summarise(
    ratio_2020 = first(hand_machine_ratio[year == min(year)]),
    ratio_2025 = last(hand_machine_ratio[year == max(year)]),
    .groups = "drop"
  ) |>
  mutate(
    ratio_change = round(ratio_2025 - ratio_2020, 3),
    direction    = if_else(ratio_change < 0, "declining", "rising")
  )

cat("\n=== STEP 2: Hand/machine ratio summary ===\n")
print(step2_summary)

# ── Step 3: Top neighbors ─────────────────────────────────────────────────────
step3_top <- map_dfr(slugs, function(s) {
  d <- rds_list[[s]]
  crop_name <- if (length(d$crop) == 0 || nchar(d$crop) == 0) str_to_title(s) else d$crop
  d$neighbor_df |>
    slice_head(n = 10) |>
    mutate(crop = crop_name, slug = s, rank = row_number()) |>
    select(crop, slug, rank, bigram, n_auto, log2_lift)
})

cat("\n=== STEP 3: Top-10 neighbors per crop ===\n")
step3_top |> select(crop, rank, bigram, n_auto, log2_lift) |>
  mutate(log2_lift = round(log2_lift, 1)) |>
  print(n = 70)

# ── Summary CSV ───────────────────────────────────────────────────────────────
summary_csv <- step1 |>
  left_join(step2_summary |> select(slug, ratio_2020, ratio_2025, ratio_change, direction),
            by = "slug") |>
  arrange(desc(n_jos))

write.csv(summary_csv, "processed/text/automation_crosscrop_summary.csv", row.names = FALSE)
cat("\nSaved: automation_crosscrop_summary.csv\n")

# ==============================================================================
# FIGURES
# ==============================================================================

crop_colors <- c(
  "Citrus"      = "#f4a520",
  "Watermelon"  = "#e63946",
  "Tomato"      = "#c1121f",
  "Lettuce"     = "#52b788",
  "Strawberry"  = "#e07a5f",
  "Blueberry"   = "#3d405b",
  "Tobacco"     = "#8d6e63",
  "Apple"       = "#6a994e",
  "Grape"       = "#7b2d8b",
  "Cherry"      = "#d62246",
  "Brassica"    = "#386641",
  "Cucumber"    = "#4cc9a0"
)

# ── Panel A: Step 1 lollipop ──────────────────────────────────────────────────
p_step1 <- step1 |>
  mutate(crop = fct_reorder(crop, pct_mech)) |>
  ggplot(aes(x = pct_mech, y = crop, color = crop)) +
  geom_segment(aes(x = 0, xend = pct_mech, y = crop, yend = crop),
               linewidth = 1, color = "grey70") +
  geom_point(size = 5) +
  geom_text(aes(label = paste0(pct_mech, "%")), hjust = -0.3,
            size = 3.5, fontface = "bold") +
  scale_color_manual(values = crop_colors, guide = "none") +
  scale_x_continuous(limits = c(0, 75),
                     labels = function(x) paste0(x, "%")) +
  labs(
    title    = "Step 1: Share of Job Orders with Direct Mechanization Mentions (Tier B)",
    subtitle = "H-2A job orders 2019–2026 | No Tier A (robotic/autonomous) or Tier C (GPS/sensor) detected in any crop",
    x = "% of JOs with Tier B mention", y = NULL,
    caption  = "Tier B: machine harvest, mechanical harvest, conveyor, self-propelled, harvest platform, automated"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold", size = 12),
    plot.subtitle = element_text(colour = "grey40", size = 8),
    plot.caption  = element_text(colour = "grey50", size = 7.5),
    plot.background = element_rect(fill = "white", colour = NA),
    panel.grid.major.y = element_blank()
  )

# ── Panel B: Step 2 hand/machine ratio over time ─────────────────────────────
step2_plot <- step2_ratios |>
  filter(year >= 2020, year <= 2025) |>
  left_join(step1 |> select(slug, crop), by = c("slug", "crop"))

p_step2 <- step2_plot |>
  ggplot(aes(x = year, y = hand_machine_ratio, color = crop, group = crop)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  geom_text_repel(
    data = step2_plot |> filter(year == 2025),
    aes(label = crop),
    nudge_x = 0.3, direction = "y", size = 3.2, segment.size = 0.3,
    min.segment.length = 0.2
  ) +
  scale_color_manual(values = crop_colors, guide = "none") +
  scale_x_continuous(breaks = 2020:2025) +
  scale_y_continuous(labels = function(x) round(x, 2)) +
  labs(
    title    = "Step 2: Hand-Task / Machine-Op Bigram Ratio, 2020–2025",
    subtitle = "Lower ratio = fewer hand-task bigrams relative to machine-op bigrams",
    x = NULL, y = "Hand-task / Machine-op ratio",
    caption  = "Employer-year deduplicated verb-anchored bigrams."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title    = element_text(face = "bold", size = 12),
    plot.subtitle = element_text(colour = "grey40", size = 8),
    plot.caption  = element_text(colour = "grey50", size = 7.5),
    plot.background = element_rect(fill = "white", colour = NA)
  )

# ── Panel C: Step 3 semantic neighbors faceted ───────────────────────────────
# Cap log2_lift at 17 for readability (infinite lifts compress plot)
step3_plot <- step3_top |>
  mutate(log2_lift_cap = pmin(log2_lift, 17),
         bigram        = str_trunc(bigram, 22),
         crop          = factor(crop, levels = step1$crop))  # ordered by mech rate

p_step3 <- step3_plot |>
  mutate(bigram = reorder_within(bigram, log2_lift_cap, crop)) |>
  ggplot(aes(x = log2_lift_cap, y = bigram, color = crop)) +
  geom_segment(aes(x = 0, xend = log2_lift_cap, y = bigram, yend = bigram),
               color = "grey75", linewidth = 0.6) +
  geom_point(size = 2.8) +
  scale_y_reordered() +
  scale_color_manual(values = crop_colors, guide = "none") +
  facet_wrap(~ crop, scales = "free_y", ncol = 4, nrow = 3) +
  labs(
    title    = "Step 3: Top-10 Semantic Neighbors of Automation Keywords, by Crop",
    subtitle = "Bigrams enriched in sentences containing mechanization terms vs. non-automation JOs  |  Log₂ lift capped at 17",
    x = "Log₂ lift (capped at 17)", y = NULL,
    caption  = paste0("Lift = share in automation-keyword sentences / share in non-automation JO sentences.\n",
                      "Min 3 occurrences in automation context. Verb-anchored bigrams only.")
  ) +
  theme_minimal(base_size = 10) +
  theme(
    plot.title      = element_text(face = "bold", size = 12),
    plot.subtitle   = element_text(colour = "grey40", size = 8),
    plot.caption    = element_text(colour = "grey50", size = 7.5),
    plot.background = element_rect(fill = "white", colour = NA),
    strip.text      = element_text(face = "bold", size = 9),
    panel.grid.major.y = element_blank()
  )

# ── Combine ───────────────────────────────────────────────────────────────────
p_combined <- (p_step1 / p_step2 / p_step3) +
  plot_layout(heights = c(1.2, 1.4, 2.8)) +
  plot_annotation(
    title    = "Automation Signals in H-2A Specialty Crop Job Orders (2019–2026)",
    subtitle = "Seven crops: three-step detection (direct mentions → task displacement → semantic context)",
    caption  = "Source: DOL H-2A job orders (JSON + gap PDFs + OFLC disclosure). Fernando Brito, UF/FRED 2026.",
    theme = theme(
      plot.title      = element_text(face = "bold", size = 15),
      plot.subtitle   = element_text(colour = "grey40", size = 10),
      plot.caption    = element_text(colour = "grey50", size = 8),
      plot.background = element_rect(fill = "white", colour = NA)
    )
  )

ggsave("processed/images/plot_crosscrop_automation_comparison.png",
       p_combined, width = 16, height = 28, dpi = 180, bg = "white")
cat("Saved: plot_crosscrop_automation_comparison.png\n")

# ── Also save individual panels ───────────────────────────────────────────────
ggsave("processed/images/plot_crosscrop_step1_mech_rate.png",
       p_step1, width = 10, height = 5, dpi = 180, bg = "white")
ggsave("processed/images/plot_crosscrop_step2_ratio.png",
       p_step2, width = 10, height=  6, dpi = 180, bg = "white")
ggsave("processed/images/plot_crosscrop_step3_neighbors.png",
       p_step3, width = 18, height = 18, dpi = 180, bg = "white")
cat("Saved: individual panel PNGs\nAll done.\n")
