###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Last modified in 26/03/2025
# Food and Resource Economics Department,
# University of Florida.

# This code tracks keyword frequency over time in H-2A job order descriptions (section A.8a).

################################################################################

mydir = "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor/h2a_labor_demand"

library(dplyr)
library(ggplot2)
library(stringr)

text_temp = readRDS(paste0(mydir, "/processed/text/df_text_jo.rds"))

text_temp = text_temp %>% sample_n(nrow(.) * 0.9)
text_temp = text_temp %>% filter(str_detect(addmcSectionDetails, "[a-zA-Z]"))

words_m=c("by hand","hand-picking","manual harvesting","strength","pace","accuracy","effort")#,
words_l=c("monitor","judge","choose")


text_temp %>%
  mutate(t = substr(dateSubmitted, 1, 7)) %>%
  group_by(t) %>%

  dplyr::summarize(m = mean(str_detect(addmcSectionDetails, regex(paste0("\\b(", paste(words_m, collapse="|"), ")"),ignore_case = TRUE)), na.rm = TRUE),
                   l = mean(str_detect(addmcSectionDetails, regex(paste0("\\b(", paste(words_l, collapse="|"), ")"),ignore_case = TRUE)), na.rm = TRUE), .groups = "drop") %>%
  mutate(m=(m+dplyr::lag(m,1)+dplyr::lag(m,2)+dplyr::lag(m,3)+dplyr::lag(m,4)+dplyr::lag(m,5)+dplyr::lag(m,6)+dplyr::lag(m,7)+dplyr::lag(m,8)+dplyr::lag(m,9)+dplyr::lag(m,10)+dplyr::lag(m,11))/12) %>%
  mutate(l=(l+dplyr::lag(l,1)+dplyr::lag(l,2)+dplyr::lag(l,3)+dplyr::lag(l,4)+dplyr::lag(l,5)+dplyr::lag(l,6)+dplyr::lag(l,7)+dplyr::lag(l,8)+dplyr::lag(l,9)+dplyr::lag(l,10)+dplyr::lag(l,11))/12) %>%
  # mutate(s=(s+dplyr::lag(s,1)+dplyr::lag(s,2)+dplyr::lag(s,3)+dplyr::lag(s,4)+dplyr::lag(s,5)+dplyr::lag(s,6)+dplyr::lag(s,7)+dplyr::lag(s,8)+dplyr::lag(s,9)+dplyr::lag(s,10)+dplyr::lag(s,11))/12) %>%


  mutate(d = case_when(
    substr(t, 1, 4) == "2019" ~ 12.96,
    substr(t, 1, 4) == "2020" ~ 13.68,
    substr(t, 1, 4) == "2021" ~ 14.29,  # SOC 45-2092
    # substr(t, 1, 4) == "2021" ~ 15.30, # 2021 broader average (optional)
    substr(t, 1, 4) == "2022" ~ 15.56,
    substr(t, 1, 4) == "2023" ~ 16.62,
    substr(t, 1, 4) == "2024" ~ 16.98,
    substr(t, 1, 4) == "2025" ~ 17.74,
    TRUE ~ NA_real_
  )) %>%

  mutate(d=(d+dplyr::lag(d,1)+dplyr::lag(d,2)+dplyr::lag(d,3)+dplyr::lag(d,4)+dplyr::lag(d,5)+dplyr::lag(d,6)+dplyr::lag(d,7)+dplyr::lag(d,8)+dplyr::lag(d,9)+dplyr::lag(d,10)+dplyr::lag(d,11))/12) %>%


  filter(!is.na(l)) %>%
  mutate(d=d/first(d)) %>%
  mutate(m=m/first(m)) %>%
  mutate(l=l/first(l)) %>%
  # mutate(s=s/first(s)) %>%
   arrange(t) %>%
  ggplot() +
  # Raw points
  geom_point(aes(x = t, y = m, group = 1, color="m"),shape = 16, size = 1.6, alpha = 0.75) +
  geom_point(aes(x = t, y = l, group = 1, color="l"),shape = 16, size = 1.6, alpha = 0.75) +
  # geom_point(aes(x = t, y = s, group = 1, color="s"),shape = 16, size = 1.6, alpha = 0.75) +
  geom_line(aes(x=t, y=d, group=1))+
  scale_color_manual(
    values = c(m = "black", l = "red"),
    breaks = c("m", "l"),
    labels = c(paste0(words_m, collapse="+"), paste0(words_l, collapse="+"))
  ) +

  # geom_line(aes(x = t, y = (, group = 1),shape = 16, size = 1.6, alpha = 0.75, color = "black") +
  # geom_line(aes(x = t, y = (, group = 1),shape = 16, size = 1.6, alpha = 0.75, color = "red") +
  # geom_line(aes(x = t, y = (, group = 1),shape = 16, size = 1.6, alpha = 0.75, color = "blue") +
  # Line (your 3-pt MA note kept in subtitle; if you actually want MA, we can add it)
  # geom_line(linewidth = 0.9, color = "grey20") +
  # Axes & labels
  labs(
    title = "Series with 3-Point Moving Average",
    subtitle = "Points: raw observations • Line: centered 3-point rollmean",
    x = "Time (t)",
    y = "Measure (m)"
  ) +
  # Scales and spacing
  scale_x_discrete(expand = expansion(mult = c(0.01, 0.02))) +
  scale_y_continuous(expand = expansion(mult = c(0.02, 0.05))) +
  # Academic/minimal theme
  theme_minimal(base_size = 12) +
  theme(
    plot.title.position = "plot",
    plot.title = element_text(face = "bold"),
    plot.subtitle = element_text(margin = margin(b = 6)),
    axis.title = element_text(),
    panel.grid.minor = element_blank(),
    panel.grid.major = element_line(linewidth = 0.3, colour = "grey85"),
    axis.text = element_text(colour = "grey10"),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1),
    plot.caption = element_text(colour = "grey40", margin = margin(t = 6))
  )

ggsave(paste0(mydir, "/processed/images/plot_wordfrequency.png"), width = 10, height = 6)

#####
# rising words/terms

##############################################################################################################
