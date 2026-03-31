###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Rebuild three figures using the gap-filled patched dataset:
#   1. Active employers by farm activity (stacked area)
#   2. Employer dynamics by FY (new / returning / exits)
#   3. Employer re-entry curves
###############################################################################

cat("\014"); rm(list = ls())

mydir <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tidyverse", "lubridate", "patchwork", "scales")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
pat <- readRDS("processed/text/df_jobs_patched.rds")
txt <- readRDS("processed/text/df_text_gap.rds") |>
  filter(addmcSectionNumber == "A.8a") |>
  select(caseNumber, duties_pdf = addmcSectionDetails)

pat <- pat |>
  left_join(txt, by = "caseNumber") |>
  mutate(duties = coalesce(jobDuties, duties_pdf))

# ==============================================================================
# 2. ACTIVITY CLASSIFICATION
# ==============================================================================
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

pat <- pat |>
  mutate(activity = classify_activity(duties)) |>
  filter(!is.na(empFein), !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate <= jobEndDate,
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30"))

cat("Activity distribution:\n")
pat |> count(activity, sort = TRUE) |>
  mutate(pct = round(n / sum(n) * 100, 1)) |> print()

# ==============================================================================
# 3. FIGURE 1 — Active employers by farm activity (month spine)
# ==============================================================================
cat("\nBuilding month spine...\n")
months_spine <- seq(as.Date("2019-01-01"), as.Date("2025-12-01"), by = "month")

active_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  pat |>
    filter(jobBeginDate <= m_end, jobEndDate >= m) |>
    distinct(empFein, activity) |>
    count(activity, name = "employers") |>
    mutate(month = m)
})

act_levels <- c("Harvesting", "Packing & Sorting", "Irrigation & Fertilizing",
                "Loading & Hauling", "Planting & Sowing", "Weeding & Cultivation",
                "Pruning & Trimming", "Other")
act_pal <- c(
  "Harvesting"             = "#d73027",
  "Packing & Sorting"      = "#4575b4",
  "Irrigation & Fertilizing" = "#74add1",
  "Loading & Hauling"      = "#fee090",
  "Planting & Sowing"      = "#fdae61",
  "Weeding & Cultivation"  = "#a6d96a",
  "Pruning & Trimming"     = "#1a9850",
  "Other"                  = "#bababa"
)

p_act <- active_df |>
  mutate(activity = factor(activity, levels = rev(act_levels))) |>
  ggplot(aes(x = month, y = employers, fill = activity)) +
  geom_area(position = "stack", alpha = 0.9) +
  scale_fill_manual(values = act_pal, name = NULL,
                    guide = guide_legend(nrow = 2)) +
  scale_x_date(date_breaks = "6 months", date_labels = "%b %Y",
               expand = c(0.01, 0)) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "Active H-2A Employers by Farm Activity",
    subtitle = "Unique employers with at least one active contract each month, 2019-2025",
    x = NULL, y = "Active employers",
    caption  = "Source: DOL H-2A job orders (JSON feed + gap-period PDFs). Gap (Dec 2023-Feb 2024) filled from PDF parsing."
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

ggsave("processed/images/plot_activities_active_employers.png", p_act,
       width = 12, height = 6, dpi = 180, bg = "white")
cat("Saved: plot_activities_active_employers.png\n")

# ==============================================================================
# 4. FIGURE 2 — Employer dynamics by FY
# ==============================================================================
emp_fy <- pat |>
  filter(!is.na(dateSubmitted)) |>
  mutate(fy = if_else(month(dateSubmitted) >= 10,
                      year(dateSubmitted) + 1L,
                      year(dateSubmitted))) |>
  filter(fy >= 2020, fy <= 2025) |>
  distinct(empFein, fy)

all_fy     <- sort(unique(emp_fy$fy))
first_seen <- emp_fy |> group_by(empFein) |>
  summarise(first_fy = min(fy), .groups = "drop")

emp_fy <- emp_fy |>
  left_join(first_seen, by = "empFein") |>
  mutate(type = if_else(fy == first_fy, "New", "Returning"))

exits <- map_dfr(all_fy[-1], function(y) {
  prev <- emp_fy |> filter(fy == y - 1) |> pull(empFein)
  curr <- emp_fy |> filter(fy == y)     |> pull(empFein)
  tibble(fy = y, exits = sum(!prev %in% curr))
})

summary_df <- emp_fy |> count(fy, type) |>
  pivot_wider(names_from = type, values_from = n, values_fill = 0) |>
  mutate(total = New + Returning) |>
  left_join(exits, by = "fy")

cat("\nEmployer dynamics (patched):\n"); print(summary_df)

bar_long  <- summary_df |>
  select(fy, New, Returning) |>
  pivot_longer(c(New, Returning), names_to = "type", values_to = "n") |>
  mutate(type = factor(type, levels = c("New", "Returning")))
exit_long <- summary_df |> filter(!is.na(exits)) |>
  mutate(n = -exits, type = "Exits")

dyn_pal <- c("Returning" = "#2166ac", "New" = "#74add1", "Exits" = "#d73027")

p_dyn <- ggplot() +
  geom_col(data = bar_long,  aes(x = fy, y = n, fill = type),
           width = 0.7, colour = "white", linewidth = 0.2) +
  geom_col(data = exit_long, aes(x = fy, y = n, fill = type),
           width = 0.7, colour = "white", linewidth = 0.2) +
  geom_line(data  = summary_df,
            aes(x = fy, y = total, colour = "Total employers"), linewidth = 1.0) +
  geom_point(data = summary_df,
             aes(x = fy, y = total, colour = "Total employers"), size = 2.5) +
  geom_hline(yintercept = 0, colour = "grey30", linewidth = 0.4) +
  scale_fill_manual(values = dyn_pal, name = NULL, guide = guide_legend(order = 1)) +
  scale_colour_manual(values = c("Total employers" = "grey20"), name = NULL,
                      guide = guide_legend(order = 2)) +
  scale_x_continuous(breaks = all_fy) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "H-2A Employer Dynamics by Fiscal Year",
    subtitle = "Stacked: new vs returning  |  Below zero: exits  |  Gap period filled from PDFs",
    x = "Fiscal year (Oct-Sep)", y = "Number of employers",
    caption  = "Source: DOL H-2A job orders (JSON + gap PDFs). FY2026 excluded (partial year)."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title       = element_text(face = "bold", size = 13),
    plot.subtitle    = element_text(colour = "grey40", size = 9),
    plot.caption     = element_text(colour = "grey50", size = 7.5),
    legend.position  = "top",
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    plot.background  = element_rect(fill = "white", colour = NA)
  )

ggsave("processed/images/employer_dynamics_fy.png", p_dyn,
       width = 10, height = 6, dpi = 180, bg = "white")
cat("Saved: employer_dynamics_fy.png\n")

# ==============================================================================
# 5. FIGURE 3 — Employer re-entry curves
# ==============================================================================
lag_cols <- paste0("lag", 1:5)

exit_records <- map_dfr(all_fy[-1], function(y) {
  prev  <- emp_fy |> filter(fy == y - 1) |> pull(empFein)
  curr  <- emp_fy |> filter(fy == y)     |> pull(empFein)
  exits <- setdiff(prev, curr)
  if (length(exits) == 0) return(NULL)
  rows <- tibble(exit_fy = y, empFein = exits)
  for (fy2 in all_fy[all_fy > y])
    rows[[paste0("lag", fy2 - y)]] <- exits %in%
      (emp_fy |> filter(fy == fy2) |> pull(empFein))
  rows
})

curve_df <- exit_records |>
  filter(exit_fy %in% c(2021, 2022, 2023)) |>
  group_by(exit_fy) |>
  summarise(n_exits = n(),
            across(any_of(lag_cols), mean, .names = "p_{.col}"),
            .groups = "drop") |>
  pivot_longer(starts_with("p_lag"), names_to = "lag", values_to = "p") |>
  mutate(lag_n = as.integer(str_extract(lag, "[0-9]+"))) |>
  arrange(exit_fy, lag_n) |>
  group_by(exit_fy) |>
  mutate(cum_returned = {
    fy_val <- exit_fy[1]
    lags   <- exit_records |> filter(exit_fy == fy_val) |> select(any_of(lag_cols))
    sapply(lag_n, function(n) {
      avail <- lags[, paste0("lag", 1:n)[paste0("lag", 1:n) %in% names(lags)], drop = FALSE]
      if (ncol(avail) == 0) return(NA_real_)
      mean(rowSums(avail, na.rm = TRUE) > 0) * 100
    })
  }) |>
  ungroup()

re_pal <- c("2021" = "#1b7837", "2022" = "#762a83", "2023" = "#e08214")

pa <- ggplot(curve_df |> filter(!is.na(cum_returned)),
             aes(x = lag_n, y = cum_returned,
                 colour = factor(exit_fy), group = factor(exit_fy))) +
  geom_line(linewidth = 1.1) + geom_point(size = 3) +
  geom_text(aes(label = sprintf("%.0f%%", cum_returned)),
            vjust = -0.8, size = 3, show.legend = FALSE) +
  scale_colour_manual(values = re_pal, name = "Exit cohort") +
  scale_x_continuous(breaks = 1:5, labels = paste0("+", 1:5, " FY")) +
  scale_y_continuous(limits = c(0, 35), labels = function(x) paste0(x, "%")) +
  labs(title    = "A.  Cumulative re-entry rate by exit cohort",
       subtitle = "% of exiting employers returning within N fiscal years",
       x = "Years since exit", y = "% returned (cumulative)") +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor   = element_blank(),
        legend.position    = "top",
        plot.title         = element_text(face = "bold"),
        plot.background    = element_rect(fill = "white", colour = NA))

ever_df <- exit_records |>
  group_by(exit_fy) |>
  summarise(n_exits  = n(),
            ever_back = mean(rowSums(across(any_of(lag_cols)), na.rm = TRUE) > 0) * 100,
            .groups  = "drop") |>
  mutate(fill_col = if_else(exit_fy == 2025, "nodata", "real"),
         label    = if_else(exit_fy == 2025, "No\nfollow-up",
                            sprintf("%.0f%%", ever_back)))

pb <- ggplot(ever_df, aes(x = factor(exit_fy), y = pmin(ever_back, 80), fill = fill_col)) +
  geom_col(width = 0.65, colour = "white") +
  geom_text(aes(label = label), vjust = -0.3, size = 3.2) +
  scale_fill_manual(values = c("real" = "#4393c3", "nodata" = "grey70"), guide = "none") +
  scale_y_continuous(limits = c(0, 85), labels = function(x) paste0(x, "%")) +
  labs(title    = "B.  Share of exiters ever returning (within data window)",
       subtitle = "True exit cohorts (2021-2024): 9-22% eventually return",
       x = "Fiscal year of exit", y = "% ever returned") +
  theme_minimal(base_size = 11) +
  theme(panel.grid.minor   = element_blank(),
        panel.grid.major.x = element_blank(),
        plot.title         = element_text(face = "bold"),
        plot.background    = element_rect(fill = "white", colour = NA))

p_reentry <- pa + pb +
  plot_annotation(
    title   = "H-2A Employer Re-entry Analysis (FY2020-2025, gap filled)",
    caption = "FY2026 excluded (partial year). FY2025 has no follow-up FY available.",
    theme   = theme(
      plot.title      = element_text(face = "bold", size = 13),
      plot.caption    = element_text(colour = "grey50", size = 7.5),
      plot.background = element_rect(fill = "white", colour = NA)
    )
  )

ggsave("processed/images/employer_reentry.png", p_reentry,
       width = 12, height = 5.5, dpi = 180, bg = "white")
cat("Saved: employer_reentry.png\n")
cat(sprintf("All done.\n"))
