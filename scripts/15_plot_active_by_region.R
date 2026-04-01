###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Active H-2A employers / orders / workers by macro farm region, month spine.
#
# Regions:
#   Southern RIM : FL GA SC NC VA AL MS LA TX AZ NM CA
#   Midwest      : IL IN OH MI WI MN IA MO ND SD NE KS OK AR
#   Other        : all remaining (PNW, Mountain West, Northeast, territories)
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
# 1. LOAD & PREPARE
# ==============================================================================
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

# ==============================================================================
# 2. REGION ASSIGNMENT
# ==============================================================================
# Southern RIM split into two seasonal modes (identified from state-level charts):
#   Early (Jan-May peak) : FL LA AZ MS AL NM
#   Late  (Jun-Oct peak) : CA GA NC TX SC VA
south_early  <- c("FL","LA","AZ","MS","AL","NM")
south_late   <- c("CA","GA","NC","TX","SC","VA")
midwest      <- c("IL","IN","OH","MI","WI","MN","IA","MO","ND","SD","NE","KS","OK","AR")

enr <- enr |>
  mutate(region = case_when(
    jobState %in% south_early ~ "South Early (Jan-May)",
    jobState %in% south_late  ~ "South Late (Jun-Oct)",
    jobState %in% midwest     ~ "Midwest",
    !is.na(jobState)          ~ "Other",
    TRUE                      ~ NA_character_
  ))

cat("Region distribution:\n")
enr |> filter(!is.na(empFein)) |> count(region, sort=TRUE) |>
  mutate(pct = round(n/sum(n)*100,1)) |> print()

# Base filter (same as script 13/14 — requires FEIN; duties not required for region)
enr_f <- enr |>
  filter(!is.na(empFein), !is.na(region),
         !is.na(jobBeginDate), !is.na(jobEndDate),
         jobBeginDate <= jobEndDate,
         jobBeginDate >= as.Date("2018-10-01"),
         jobEndDate   <= as.Date("2026-06-30"))

cat(sprintf("\nRecords with region: %s\n", format(nrow(enr_f), big.mark=",")))

# ==============================================================================
# 3. MONTH SPINE
# ==============================================================================
months_spine <- seq(as.Date("2019-01-01"), as.Date("2025-12-01"), by = "month")
reg_levels   <- c("South Early (Jan-May)", "South Late (Jun-Oct)", "Midwest", "Other")
reg_pal      <- c("South Early (Jan-May)" = "#d73027",
                  "South Late (Jun-Oct)"  = "#f46d43",
                  "Midwest"               = "#4575b4",
                  "Other"                 = "#a6d96a")

cat("Building month spine...\n")

spine_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  sub   <- enr_f |> filter(jobBeginDate <= m_end, jobEndDate >= m)
  bind_rows(
    sub |> distinct(empFein, region) |> count(region, name="employers"),
    sub |> count(region, name="orders") |> rename(n=orders) |> mutate(metric="orders"),
    sub |> filter(!is.na(jobWrksNeeded)) |>
      group_by(region) |> summarise(n=sum(jobWrksNeeded,na.rm=TRUE), .groups="drop")
  ) |> mutate(month=m)
})

# rebuild cleanly with three separate passes
emp_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_f |> filter(jobBeginDate <= m_end, jobEndDate >= m) |>
    distinct(empFein, region) |> count(region, name="value") |> mutate(month=m, metric="Employers")
})
ord_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_f |> filter(jobBeginDate <= m_end, jobEndDate >= m) |>
    count(region, name="value") |> mutate(month=m, metric="Job Orders")
})
wrk_df <- map_dfr(months_spine, function(m) {
  m_end <- ceiling_date(m, "month") - days(1)
  enr_f |> filter(jobBeginDate <= m_end, jobEndDate >= m, !is.na(jobWrksNeeded)) |>
    group_by(region) |> summarise(value=sum(jobWrksNeeded,na.rm=TRUE), .groups="drop") |>
    mutate(month=m, metric="Workers")
})

all_df <- bind_rows(emp_df, ord_df, wrk_df) |>
  mutate(region = factor(region, levels = reg_levels),
         metric = factor(metric, levels = c("Employers","Job Orders","Workers")))

# ==============================================================================
# 4. COMBINED FACET FIGURE (3 panels stacked)
# ==============================================================================
make_panel <- function(df, y_lab) {
  ggplot(df, aes(x=month, y=value, fill=region)) +
    geom_area(position="stack", alpha=0.9) +
    scale_fill_manual(values=reg_pal, name=NULL,
                      guide=guide_legend(reverse=FALSE)) +
    scale_x_date(date_breaks="6 months", date_labels="%b %Y", expand=c(0.01,0)) +
    scale_y_continuous(labels=comma) +
    labs(x=NULL, y=y_lab) +
    theme_minimal(base_size=11) +
    theme(legend.position   = "top",
          axis.text.x       = element_text(angle=45, hjust=1, size=8),
          panel.grid.minor  = element_blank(),
          plot.background   = element_rect(fill="white", colour=NA))
}

p_emp <- make_panel(emp_df, "Active employers")
p_ord <- make_panel(ord_df, "Active job orders")
p_wrk <- make_panel(wrk_df, "Workers active")

p_combined <- p_emp / p_ord / p_wrk +
  plot_annotation(
    title   = "H-2A Labor Demand by Macro Farm Region",
    subtitle= "South Early: FL LA AZ MS AL NM  |  South Late: CA GA NC TX SC VA  |  Midwest: IL IN OH MI WI MN IA MO ND SD NE KS OK AR  |  Other",
    caption = "Source: DOL H-2A job orders (JSON + gap-period PDFs + OFLC disclosure).",
    theme   = theme(
      plot.title    = element_text(face="bold", size=13),
      plot.subtitle = element_text(colour="grey40", size=8),
      plot.caption  = element_text(colour="grey50", size=7.5),
      plot.background = element_rect(fill="white", colour=NA)
    )
  )

ggsave("processed/images/plot_region_combined.png", p_combined,
       width=12, height=12, dpi=180, bg="white")
cat("Saved: plot_region_combined.png\n")
ggsave("processed/images/plot_region4_combined.png", p_combined,
       width=12, height=12, dpi=180, bg="white")

# ==============================================================================
# 5. INDIVIDUAL FIGURES (each metric separately, wider)
# ==============================================================================
fmt_panel <- function(p) {
  p + theme(
    plot.title    = element_text(face="bold", size=13),
    plot.subtitle = element_text(colour="grey40", size=9),
    plot.caption  = element_text(colour="grey50", size=7.5),
    legend.position = "top"
  )
}

caption_txt <- "Source: DOL H-2A job orders (JSON + gap-period PDFs + OFLC disclosure).\nSouth Early (Jan-May): FL LA AZ MS AL NM  |  South Late (Jun-Oct): CA GA NC TX SC VA  |  Midwest: IL IN OH MI WI MN IA MO ND SD NE KS OK AR"

p1 <- emp_df |> mutate(region=factor(region,levels=reg_levels)) |>
  ggplot(aes(x=month,y=value,fill=region)) +
  geom_area(position="stack",alpha=0.9) +
  scale_fill_manual(values=reg_pal,name=NULL,guide=guide_legend(reverse=FALSE)) +
  scale_x_date(date_breaks="6 months",date_labels="%b %Y",expand=c(0.01,0)) +
  scale_y_continuous(labels=comma) +
  labs(title="Active H-2A Employers by Farm Region",
       subtitle="Unique employers with at least one active contract each month, 2019-2025",
       x=NULL, y="Active employers", caption=caption_txt) +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=13),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="top",
        axis.text.x=element_text(angle=45,hjust=1,size=8),
        panel.grid.minor=element_blank(),
        plot.background=element_rect(fill="white",colour=NA))

p2 <- ord_df |> mutate(region=factor(region,levels=reg_levels)) |>
  ggplot(aes(x=month,y=value,fill=region)) +
  geom_area(position="stack",alpha=0.9) +
  scale_fill_manual(values=reg_pal,name=NULL,guide=guide_legend(reverse=FALSE)) +
  scale_x_date(date_breaks="6 months",date_labels="%b %Y",expand=c(0.01,0)) +
  scale_y_continuous(labels=comma) +
  labs(title="Active H-2A Job Orders by Farm Region",
       subtitle="Orders counted in every month they are under contract (begin-end date), 2019-2025",
       x=NULL, y="Active job orders", caption=caption_txt) +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=13),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="top",
        axis.text.x=element_text(angle=45,hjust=1,size=8),
        panel.grid.minor=element_blank(),
        plot.background=element_rect(fill="white",colour=NA))

p3 <- wrk_df |> mutate(region=factor(region,levels=reg_levels)) |>
  ggplot(aes(x=month,y=value,fill=region)) +
  geom_area(position="stack",alpha=0.9) +
  scale_fill_manual(values=reg_pal,name=NULL,guide=guide_legend(reverse=FALSE)) +
  scale_x_date(date_breaks="6 months",date_labels="%b %Y",expand=c(0.01,0)) +
  scale_y_continuous(labels=comma) +
  labs(title="H-2A Workers Active by Farm Region",
       subtitle="Workers counted in every month they are under contract (begin-end date), 2019-2025",
       x=NULL, y="Workers active", caption=caption_txt) +
  theme_minimal(base_size=11) +
  theme(plot.title=element_text(face="bold",size=13),
        plot.subtitle=element_text(colour="grey40",size=9),
        plot.caption=element_text(colour="grey50",size=7.5),
        legend.position="top",
        axis.text.x=element_text(angle=45,hjust=1,size=8),
        panel.grid.minor=element_blank(),
        plot.background=element_rect(fill="white",colour=NA))

ggsave("processed/images/plot_region_employers.png",  p1, width=12, height=6, dpi=180, bg="white")
ggsave("processed/images/plot_region_orders.png",     p2, width=12, height=6, dpi=180, bg="white")
ggsave("processed/images/plot_region_workers.png",    p3, width=12, height=6, dpi=180, bg="white")
cat("Saved: plot_region_employers.png\n")
cat("Saved: plot_region_orders.png\n")
cat("Saved: plot_region_workers.png\n")
cat("All done.\n")
