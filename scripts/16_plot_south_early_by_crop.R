###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Active workers in South Early states (FL LA AZ MS AL NM) broken down
# by crop group — to explain the bimodal seasonal pattern.
#
# Crop groups (from duties text keywords):
#   Citrus & Tree Fruit   : citrus, orange, grapefruit, tangerine, pecan
#   Berries & Vegetables  : strawberr, blueberr, tomato, pepper, cucumber,
#                           squash, watermelon, vegetable
#   Field Crops           : sugarcane, sugar cane, corn, cotton, soybean,
#                           rice, sweet potato, bean, tobacco
#   Aquaculture & Livestock: crawfish, cattle, sheep, livestock
#   Nursery & Greenhouse  : nursery, greenhouse
#   Other                 : everything else
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "lubridate", "scales", "patchwork")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# 1. LOAD & PREPARE
# ==============================================================================
enr      <- readRDS("processed/text/df_jobs_enriched.rds")
txt_full <- readRDS("processed/text/df_text_gap_full.rds")

duties_tbl <- txt_full |>
  filter(addmcSectionNumber == "A.8a") |>
  mutate(text_clean = str_squish(addmcSectionDetails),
         is_see     = str_detect(tolower(text_clean), "^see addendum c")) |>
  group_by(caseNumber) |>
  summarise(
    duties_pdf = if_else(
      any(!is_see & nchar(text_clean) > 5),
      paste(text_clean[!is_see & nchar(text_clean) > 5], collapse = " "),
      paste(text_clean[nchar(text_clean) > 5],            collapse = " ")
    ) |> str_squish(),
    .groups = "drop"
  ) |>
  mutate(duties_pdf = na_if(duties_pdf, ""))

south_early <- c("FL","LA","AZ","MS","AL","NM")

enr_s <- enr |>
  filter(!is.na(empFein), jobState %in% south_early,
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate <= jobEndDate,
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30"),
         !is.na(jobWrksNeeded)) |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf))

# ==============================================================================
# 2. CROP CLASSIFICATION
# ==============================================================================
classify_crop <- function(d) {
  d <- tolower(coalesce(d, ""))
  case_when(
    str_detect(d, "citrus|orange|grapefruit|tangerine|pecan|lemon|lime") ~ "Citrus & Tree Fruit",
    str_detect(d, "strawberr|blueberr|tomato|pepper|cucumber|squash|watermelon|vegetable|eggplant|zucchini") ~ "Berries & Vegetables",
    str_detect(d, "sugarcane|sugar cane")                                ~ "Sugarcane",
    str_detect(d, "crawfish|shrimp|catfish|aqua")                        ~ "Aquaculture",
    str_detect(d, "rice|soybean|corn|cotton|sweet potato|tobacco|wheat|sorghum|peanut|bean|sugar beet") ~ "Field Crops",
    str_detect(d, "cattle|sheep|livestock|dairy|poultry|swine|hog|goat") ~ "Livestock",
    str_detect(d, "nursery|greenhouse|ornamental|turf|sod|plant propagat") ~ "Nursery & Greenhouse",
    TRUE ~ "Other"
  )
}

enr_s <- enr_s |> mutate(crop_group = classify_crop(duties))

cat("Crop group distribution (South Early):\n")
enr_s |> count(crop_group, sort=TRUE) |>
  mutate(pct = round(n/sum(n)*100,1)) |> print()

# ==============================================================================
# 3. MONTH SPINE
# ==============================================================================
months_spine <- seq(as.Date("2019-01-01"), as.Date("2025-12-01"), by = "month")

# geom_area stacks in reverse factor order: first level ends up on top
crop_levels <- c("Sugarcane", "Berries & Vegetables",
                 "Citrus & Tree Fruit", "Field Crops",
                 "Aquaculture", "Livestock",
                 "Nursery & Greenhouse", "Other")
crop_pal <- c(
  "Citrus & Tree Fruit"  = "#f4a520",
  "Berries & Vegetables" = "#d73027",
  "Sugarcane"            = "#7b2d8b",
  "Aquaculture"          = "#74add1",
  "Field Crops"          = "#a6d96a",
  "Livestock"            = "#d9853b",
  "Nursery & Greenhouse" = "#1a9850",
  "Other"                = "#bababa"
)

cat("Building month spine...\n")
wrk_crop <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_s |> filter(jobBeginDate <= m_end, jobEndDate >= m) |>
    group_by(crop_group) |>
    summarise(workers = sum(jobWrksNeeded, na.rm=TRUE), .groups="drop") |>
    mutate(month = m)
})

# ==============================================================================
# 4. STACKED AREA — South Early aggregate by crop
# ==============================================================================
p_crop <- wrk_crop |>
  mutate(crop_group = factor(crop_group, levels = crop_levels)) |>
  ggplot(aes(x = month, y = workers, fill = crop_group)) +
  geom_area(position = "stack", alpha = 0.9) +
  scale_fill_manual(values = crop_pal, name = NULL,
                    guide = guide_legend(nrow = 2, reverse = FALSE)) +
  scale_x_date(date_breaks = "6 months", date_labels = "%b %Y", expand = c(0.01,0)) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "H-2A Workers Active in South Early States by Crop Group",
    subtitle = "States: FL LA AZ MS AL NM  —  bimodal pattern explained by crop type",
    x = NULL, y = "Workers active",
    caption  = "Source: DOL H-2A job orders (JSON + gap-period PDFs + OFLC disclosure). Crop classified from duties text."
  ) +
  theme_minimal(base_size = 11) +
  theme(plot.title       = element_text(face = "bold", size = 13),
        plot.subtitle    = element_text(colour = "grey40", size = 9),
        plot.caption     = element_text(colour = "grey50", size = 7.5),
        legend.position  = "top",
        axis.text.x      = element_text(angle = 45, hjust = 1, size = 8),
        panel.grid.minor = element_blank(),
        plot.background  = element_rect(fill = "white", colour = NA))

ggsave("processed/images/plot_south_early_by_crop.png", p_crop,
       width = 12, height = 6, dpi = 180, bg = "white")
cat("Saved: plot_south_early_by_crop.png\n")

# ==============================================================================
# 5. FACET BY STATE, coloured by crop
# ==============================================================================
wrk_crop_state <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_s |> filter(jobBeginDate <= m_end, jobEndDate >= m) |>
    group_by(jobState, crop_group) |>
    summarise(workers = sum(jobWrksNeeded, na.rm=TRUE), .groups="drop") |>
    mutate(month = m)
})

state_order <- wrk_crop_state |> group_by(jobState) |>
  summarise(tot = sum(workers)) |> arrange(desc(tot)) |> pull(jobState)

p_facet <- wrk_crop_state |>
  mutate(crop_group = factor(crop_group, levels = rev(crop_levels)),
         jobState   = factor(jobState, levels = state_order)) |>
  ggplot(aes(x = month, y = workers, fill = crop_group)) +
  geom_area(position = "stack", alpha = 0.9) +
  scale_fill_manual(values = crop_pal, name = NULL,
                    guide = guide_legend(nrow = 2, reverse = FALSE)) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y", expand = c(0.01,0)) +
  scale_y_continuous(labels = comma) +
  facet_wrap(~jobState, scales = "free_y", ncol = 3) +
  labs(
    title    = "H-2A Workers Active by Crop Group — South Early States",
    subtitle = "Each state free y-scale; ordered by total workers",
    x = NULL, y = "Workers active",
    caption  = "Source: DOL H-2A job orders. Crop classified from duties text."
  ) +
  theme_minimal(base_size = 10) +
  theme(plot.title       = element_text(face = "bold", size = 13),
        plot.subtitle    = element_text(colour = "grey40", size = 9),
        plot.caption     = element_text(colour = "grey50", size = 7.5),
        legend.position  = "top",
        strip.text       = element_text(face = "bold"),
        axis.text.x      = element_text(angle = 45, hjust = 1, size = 7),
        panel.grid.minor = element_blank(),
        plot.background  = element_rect(fill = "white", colour = NA))

ggsave("processed/images/plot_south_early_crop_by_state.png", p_facet,
       width = 14, height = 10, dpi = 180, bg = "white")
cat("Saved: plot_south_early_crop_by_state.png\n")
cat("All done.\n")
