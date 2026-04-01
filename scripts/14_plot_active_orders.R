###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Rebuild activity charts with full enriched data:
#   - plot_activities_active_orders.png  (job order count per month)
#   - plot_activities_active_workers.png (workers requested per month)
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "lubridate", "scales")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# --- load enriched + full text (same pipeline as script 13) ---
enr      <- readRDS("processed/text/df_jobs_enriched.rds")
txt_full <- readRDS("processed/text/df_text_gap_full.rds")

addc_prefix <- regex(
  paste0("^3[.]\\s*Details\\s+of\\s+Material\\s+Term\\s+or\\s+Condition",
         "\\s*[(]up\\s+to\\s+3[,.]500\\s+characters?[)]\\s*"),
  ignore_case = TRUE
)

duties_tbl <- txt_full |>
  filter(addmcSectionNumber == "A.8a") |>
  mutate(
    text_clean  = str_remove(addmcSectionDetails, addc_prefix) |> str_squish(),
    is_see_addc = str_detect(tolower(text_clean), "^see addendum c")
  ) |>
  group_by(caseNumber) |>
  summarise(
    duties_pdf = if_else(
      any(!is_see_addc & nchar(text_clean) > 5),
      paste(text_clean[!is_see_addc & nchar(text_clean) > 5], collapse = " "),
      paste(text_clean[nchar(text_clean) > 5],                 collapse = " ")
    ) |> str_squish(),
    .groups = "drop"
  ) |>
  mutate(duties_pdf = na_if(duties_pdf, ""))

enr <- enr |>
  left_join(duties_tbl, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf))

classify_activity <- function(d) {
  d <- tolower(coalesce(d, ""))
  case_when(
    str_detect(d, "harvest|pick|picking|reap|cutting cane|cut cane") ~ "Harvesting",
    str_detect(d, "pack|sort|grade|bin|box|process|wash|cool|wrap")  ~ "Packing & Sorting",
    str_detect(d, "plant|sow|seed|transplant|propagat")              ~ "Planting & Sowing",
    str_detect(d, "prune|thin|train|trellis|tie|string")             ~ "Pruning & Trimming",
    str_detect(d, "weed|cultiv|hoe|till|disk|bed")                   ~ "Weeding & Cultivation",
    str_detect(d, "irrigat|fertil|spray|apply|pesticide|fungicide")  ~ "Irrigation & Fertilizing",
    str_detect(d, "load|unload|haul|transport|truck|move|transfer")  ~ "Loading & Hauling",
    TRUE ~ "Other"
  )
}

enr <- enr |>
  mutate(
    activity = classify_activity(duties),
    activity = if_else(
      activity == "Other" &
        str_detect(tolower(coalesce(duties, "")), "see addendum c|addendum c|^$"),
      classify_activity(socTitle),
      activity
    )
  )

enr_act <- enr |>
  filter(!is.na(empFein), !is.na(duties),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate <= jobEndDate,
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30"))

# --- month spine: count job ORDERS (rows) active each month ---
cat("Building month spine (orders)...\n")
months_spine <- seq(as.Date("2019-01-01"), as.Date("2025-12-01"), by = "month")

orders_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_act |>
    filter(jobBeginDate <= m_end, jobEndDate >= m) |>
    count(activity, name = "orders") |>
    mutate(month = m)
})

cat("\nGap months check:\n")
orders_df |>
  filter(month >= as.Date("2023-11-01"), month <= as.Date("2024-03-01")) |>
  group_by(month) |> summarise(total = sum(orders)) |> print()

act_levels <- c("Harvesting", "Packing & Sorting", "Irrigation & Fertilizing",
                "Loading & Hauling", "Planting & Sowing", "Weeding & Cultivation",
                "Pruning & Trimming", "Other")
act_pal <- c(
  "Harvesting"               = "#d73027",
  "Packing & Sorting"        = "#4575b4",
  "Irrigation & Fertilizing" = "#74add1",
  "Loading & Hauling"        = "#fee090",
  "Planting & Sowing"        = "#fdae61",
  "Weeding & Cultivation"    = "#a6d96a",
  "Pruning & Trimming"       = "#1a9850",
  "Other"                    = "#bababa"
)

p_orders <- orders_df |>
  mutate(activity = factor(activity, levels = rev(act_levels))) |>
  ggplot(aes(x = month, y = orders, fill = activity)) +
  geom_area(position = "stack", alpha = 0.9) +
  scale_fill_manual(values = act_pal, name = NULL,
                    guide = guide_legend(nrow = 2)) +
  scale_x_date(date_breaks = "6 months", date_labels = "%b %Y",
               expand = c(0.01, 0)) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "Active H-2A Job Orders by Farm Activity",
    subtitle = "Orders counted in every month they are under contract (begin-end date), 2019-2025",
    x = NULL, y = "Active job orders",
    caption  = paste0("Source: DOL H-2A job orders (JSON + gap-period PDFs + OFLC disclosure). ",
                      "Full PDF re-parse: Addendum C job duties recovered.")
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(colour = "grey40", size = 9),
    plot.caption     = element_text(colour = "grey50", size = 7.5),
    legend.position  = "top",
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 8),
    panel.grid.minor = element_blank(),
    plot.background  = element_rect(fill = "white", colour = NA)
  )

ggsave("processed/images/plot_activities_active_orders.png", p_orders,
       width = 12, height = 6, dpi = 180, bg = "white")
cat("Saved: plot_activities_active_orders.png\n")

# ==============================================================================
# FIGURE 2 — Active workers by farm activity (sum jobWrksNeeded per month)
# ==============================================================================
cat("Building month spine (workers)...\n")

workers_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_act |>
    filter(jobBeginDate <= m_end, jobEndDate >= m,
           !is.na(jobWrksNeeded)) |>
    group_by(activity) |>
    summarise(workers = sum(jobWrksNeeded, na.rm = TRUE), .groups = "drop") |>
    mutate(month = m)
})

cat("\nGap months check (workers):\n")
workers_df |>
  filter(month >= as.Date("2023-11-01"), month <= as.Date("2024-03-01")) |>
  group_by(month) |> summarise(total = sum(workers)) |> print()

p_workers <- workers_df |>
  mutate(activity = factor(activity, levels = rev(act_levels))) |>
  ggplot(aes(x = month, y = workers, fill = activity)) +
  geom_area(position = "stack", alpha = 0.9) +
  scale_fill_manual(values = act_pal, name = NULL,
                    guide = guide_legend(nrow = 2)) +
  scale_x_date(date_breaks = "6 months", date_labels = "%b %Y",
               expand = c(0.01, 0)) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "H-2A Workers Active by Farm Activity",
    subtitle = "Workers counted in every month they are under contract (begin-end date), 2019-2025",
    x = NULL, y = "Workers active",
    caption  = paste0("Source: DOL H-2A job orders (JSON + gap-period PDFs + OFLC disclosure). ",
                      "Full PDF re-parse: Addendum C job duties recovered.")
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(colour = "grey40", size = 9),
    plot.caption     = element_text(colour = "grey50", size = 7.5),
    legend.position  = "top",
    axis.text.x      = element_text(angle = 45, hjust = 1, size = 8),
    panel.grid.minor = element_blank(),
    plot.background  = element_rect(fill = "white", colour = NA)
  )

ggsave("processed/images/plot_activities_active_workers.png", p_workers,
       width = 12, height = 6, dpi = 180, bg = "white")
cat("Saved: plot_activities_active_workers.png\n")
