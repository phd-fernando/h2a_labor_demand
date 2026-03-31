###############################################################################
# Code written by Fernando Brito, f.britogonzalez@ufl.edu
# Dissertation work Spring 2026
# Food and Resource Economics Department, University of Florida.
#
# Extract task verbs and verb-object pairs from H-2A job order descriptions
# Pipeline (regex-based, no annotation model needed):
#   1. Boilerplate sentence removal  (corpus doc-frequency filter)
#   2. Gerund regex extraction       ("verb-ing + noun" patterns)
#   3. Infinitive extraction         ("to [ag-verb] + noun" patterns)
#   4. Agricultural verb-list filter (pre-defined domain verbs)
#   5. Per-job-order summaries + plots
###############################################################################

cat("\014"); rm(list = ls())

mydir   <- "C:/Users/Fer/UF Dropbox/Fernando Jose Brito-Gonzalez/Fernando_Brito_Dissertation/h2a_labor"
out_dir <- paste0(mydir, "/h2a_labor_demand/processed/text")
img_dir <- paste0(mydir, "/h2a_labor_demand/processed/images")
setwd(mydir)

options(repos = c(CRAN = "https://cloud.r-project.org"))
for (pkg in c("tokenizers", "tidyverse", "tidytext")) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
  library(pkg, character.only = TRUE)
}

# ------------------------------------------------------------------------------
# 1. Load data
# ------------------------------------------------------------------------------
df <- readRDS(paste0(out_dir, "/df_text_jo.rds")) %>%
  filter(!is.na(addmcSectionDetails), addmcSectionDetails != "") %>%
  filter(dateSubmitted >= as.Date("2019-01-01")) %>%
  mutate(year    = lubridate::year(dateSubmitted),
         quarter = lubridate::quarter(dateSubmitted),
         period  = paste0(year, "-Q", quarter))

cat("N docs after date filter:", nrow(df), "\n")

# ------------------------------------------------------------------------------
# 2. Spanish sentence filter
#    Drop sentences where >30% of words are unambiguously Spanish.
# ------------------------------------------------------------------------------
es_words <- c(
  "que","los","las","del","por","con","una","para","como","pero","este","esta",
  "estos","estas","también","cuando","donde","aunque","porque","sino",
  "entre","desde","hasta","sobre","bajo","ante","tras","según","durante","mediante",
  "cuyo","cuya","cuyos","cuyas","quién","quiénes","cuál","cuáles","cuánto",
  "tiene","pueden","debe","será","están","son","han","fue","ser","estar","hacer",
  "tendrá","podrá","deberá","tendrán","podrán","deberán","había","habría",
  "hará","harán","serán","siendo","haber","tener","poder","deber",
  "trabajar","realizar","llevar","tomar","seguir","incluir","aplicar","cumplir",
  "proporcionar","notificar","determinar","constituir","aceptar","indicar",
  "trabajador","trabajadores","empleador","empleadores","contrato","trabajo",
  "salario","vivienda","transporte","herramienta","herramientas","condición",
  "condiciones","período","acuerdo","disposición","disposiciones","ley","leyes",
  "reglamento","reglamentos","párrafo","sección","secciones","permiso","permisos",
  "autorización","notificación","aprobación","excepción","empresa","compañía",
  "cosecha","cultivo","siembra","irrigación","campo","campos","producto","productos",
  "temporada","jornada","hora","horas","día","días","semana","semanas",
  "número","cantidad","porcentaje","parte","partes","caso","casos","forma",
  "lugar","lugares","cargo","costo","costos","pago","pagos","tarifa","tarifas",
  "derecho","derechos","obligación","obligaciones","responsabilidad",
  "información","documentos","registro","registros","reportes","reporte",
  "utilizados","utilizada","utilizado","utilizadas","mediante","asimismo",
  "igualmente","conforme","siempre","nunca","solo","sólo","aquí","allí","así"
)

is_spanish <- function(text) {
  words <- str_split(str_to_lower(text), "\\s+")[[1]]
  words <- words[str_detect(words, "^[a-z]{2,}$")]
  if (length(words) == 0) return(FALSE)
  mean(words %in% es_words) > 0.30
}

# ------------------------------------------------------------------------------
# 3. Boilerplate sentence removal (>3% doc frequency = T&C boilerplate)
# ------------------------------------------------------------------------------
BOILER_THRESH <- 0.03

cat("Splitting into sentences + filtering Spanish...\n")
t0 <- Sys.time()

sent_df <- df %>%
  select(caseNumber, addmcSectionDetails) %>%
  mutate(sentences = tokenize_sentences(addmcSectionDetails, lowercase = FALSE)) %>%
  select(-addmcSectionDetails) %>%
  unnest(sentences) %>%
  filter(!vapply(sentences, is_spanish, logical(1))) %>%   # drop Spanish sentences
  mutate(sent_norm = str_squish(str_to_lower(
    str_remove_all(sentences, "[^a-z\\s]"))))

sent_freq <- sent_df %>%
  distinct(caseNumber, sent_norm) %>%
  count(sent_norm, name = "doc_freq")

boilerplate <- sent_freq %>%
  filter(doc_freq > BOILER_THRESH * nrow(df)) %>%
  pull(sent_norm)

cat(sprintf("  Boilerplate sentences identified: %d  (threshold=%.0f%%)\n",
            length(boilerplate), BOILER_THRESH * 100))

df_clean <- sent_df %>%
  filter(!sent_norm %in% boilerplate) %>%
  group_by(caseNumber) %>%
  summarise(text_clean = paste(sentences, collapse = " "), .groups = "drop") %>%
  left_join(df %>% select(caseNumber, period, dateSubmitted, year, quarter),
            by = "caseNumber") %>%
  filter(nchar(text_clean) > 20)

cat(sprintf("  Docs remaining after boilerplate removal: %d  (%.1f sec)\n",
            nrow(df_clean), as.numeric(Sys.time() - t0)))

cat("\n--- Sample cleaned text ---\n")
cat(str_trunc(df_clean$text_clean[1], 400), "\n\n")
cat(str_trunc(df_clean$text_clean[2], 400), "\n\n")

# ------------------------------------------------------------------------------
# 3. Agricultural task verb list (base forms)
# ------------------------------------------------------------------------------
ag_verbs <- c(
  # field crops
  "harvest","plant","cultivate","till","plow","irrigate","fertilize",
  "spray","apply","sow","seed","prune","thin","weed","mow","bale",
  "rake","disc","transplant","pollinate","fumigate","mulch","bed",
  # specialty crops / livestock
  "pick","pack","sort","grade","clip","trim","cut","bunch","wrap",
  "load","unload","haul","transport","drive","operate","handle",
  "feed","water","milk","shear","brand","vaccinate","castrate",
  "tag","weigh","pen","herd","move","rotate","breed","gather",
  # equipment / maintenance
  "maintain","repair","clean","inspect","calibrate","adjust",
  "install","remove","replace","check","service","lubricate",
  "mix","prepare","store","stack","transfer","assemble",
  # processing / packing
  "wash","cool","dry","box","crate","label","count","measure",
  "record","monitor","observe","spread","rake","dig","plant"
)
ag_verbs <- unique(ag_verbs)

# ------------------------------------------------------------------------------
# 4. Stopwords list for cleaning extracted objects
# ------------------------------------------------------------------------------
stop_words_en <- c(
  "the","a","an","and","or","of","to","in","on","at","for","with",
  "from","by","as","is","are","was","were","be","been","being",
  "that","this","these","those","it","its","their","they","them",
  "all","any","each","more","other","such","up","out","over","into",
  "through","about","after","before","between","under","during",
  "when","which","who","will","would","should","could","may","might",
  "not","no","nor","so","yet","but","if","than","then","too","very",
  "also","have","has","had","do","does","did","use","using","used"
)

# ------------------------------------------------------------------------------
# 5. Regex extraction: gerund + object
#    Pattern: word ending in -ing followed by up to 2 words
# ------------------------------------------------------------------------------
cat("Extracting gerund-object pairs via regex...\n")
t1 <- Sys.time()

gerund_pattern <- "\\b([a-z]+ing)\\s+((?:[a-z]+-?[a-z]*\\s+){0,1}[a-z]+-?[a-z]*)(?=[,;.(\\s]|$)"

extract_gerund_pairs <- function(text, case_id) {
  text_lower <- str_to_lower(text)
  matches <- str_match_all(text_lower, gerund_pattern)[[1]]
  if (nrow(matches) == 0) return(NULL)

  tibble(
    caseNumber = case_id,
    verb_gerund = matches[, 2],
    obj_raw     = str_squish(matches[, 3])
  ) %>%
    mutate(
      # strip -ing to get base: harvesting→harvest, cultivating→cultivate
      verb_base = str_remove(verb_gerund, "ing$"),
      verb_base = case_when(
        verb_base %in% ag_verbs                  ~ verb_base,
        paste0(verb_base, "e") %in% ag_verbs     ~ paste0(verb_base, "e"),
        paste0(verb_base, "ing") == verb_gerund &
          str_sub(verb_base, -1) == str_sub(verb_base, -2, -2) ~
          str_sub(verb_base, 1, -2),  # doubling: running→run
        TRUE ~ verb_base
      )
    ) %>%
    filter(verb_base %in% ag_verbs) %>%
    mutate(
      obj_lemma = str_remove_all(obj_raw,
                                 paste0("^(", paste(stop_words_en, collapse = "|"), ")\\s+"))
    ) %>%
    filter(nchar(obj_lemma) > 2, !obj_lemma %in% stop_words_en) %>%
    select(caseNumber, verb_base, obj_lemma)
}

gerund_pairs <- bind_rows(mapply(extract_gerund_pairs,
                                 text    = df_clean$text_clean,
                                 case_id = df_clean$caseNumber,
                                 SIMPLIFY = FALSE))

cat(sprintf("  Gerund pairs:     %d  (%.1f sec)\n",
            nrow(gerund_pairs), as.numeric(Sys.time() - t1)))

# ------------------------------------------------------------------------------
# 6. Infinitive extraction: "to [ag-verb] [noun]"
# ------------------------------------------------------------------------------
t2 <- Sys.time()
inf_pattern <- paste0(
  "\\bto\\s+(", paste(ag_verbs, collapse = "|"), ")\\s+",
  "((?:[a-z]+-?[a-z]*\\s+){0,1}[a-z]+-?[a-z]*)(?=[,;.(\\s]|$)"
)

extract_inf_pairs <- function(text, case_id) {
  text_lower <- str_to_lower(text)
  m <- str_match_all(text_lower, inf_pattern)[[1]]
  if (nrow(m) == 0) return(NULL)
  tibble(
    caseNumber = case_id,
    verb_base  = m[, 2],
    obj_lemma  = str_squish(m[, 3])
  ) %>%
    mutate(obj_lemma = str_remove_all(
      obj_lemma, paste0("^(", paste(stop_words_en, collapse = "|"), ")\\s+"))) %>%
    filter(nchar(obj_lemma) > 2, !obj_lemma %in% stop_words_en)
}

inf_pairs <- bind_rows(mapply(extract_inf_pairs,
                              text    = df_clean$text_clean,
                              case_id = df_clean$caseNumber,
                              SIMPLIFY = FALSE))

cat(sprintf("  Infinitive pairs: %d  (%.1f sec)\n",
            nrow(inf_pairs), as.numeric(Sys.time() - t2)))

# ------------------------------------------------------------------------------
# 7. Combine and attach metadata
# ------------------------------------------------------------------------------
verb_obj <- bind_rows(gerund_pairs, inf_pairs) %>%
  distinct() %>%
  left_join(df_clean %>% select(caseNumber, period, dateSubmitted, year, quarter),
            by = "caseNumber")

cat("Total verb-object pairs (after dedup):", nrow(verb_obj), "\n")

# ------------------------------------------------------------------------------
# 8. Summaries
# ------------------------------------------------------------------------------

top_verbs <- verb_obj %>%
  count(verb_base, sort = TRUE) %>%
  slice_head(n = 40)

top_vo <- verb_obj %>%
  count(verb_base, obj_lemma, sort = TRUE) %>%
  mutate(pair = paste(verb_base, obj_lemma, sep = " + ")) %>%
  slice_head(n = 50)

vo_per_case <- verb_obj %>%
  count(caseNumber, verb_base, obj_lemma) %>%
  mutate(pair = paste(verb_base, obj_lemma, sep = "_"))

cat("\n=== Top 40 task verbs ===\n");    print(as.data.frame(top_verbs))
cat("\n=== Top 50 verb-object pairs ===\n"); print(as.data.frame(top_vo))

verb_year <- verb_obj %>%
  filter(!is.na(year)) %>%
  count(year, verb_base) %>%
  group_by(year) %>%
  mutate(share = n / sum(n)) %>%
  ungroup()

top12_verbs <- top_verbs$verb_base[1:12]
cat("\n=== Verb share by year (top 12 verbs) ===\n")
verb_year %>%
  filter(verb_base %in% top12_verbs) %>%
  arrange(verb_base, year) %>%
  as.data.frame() %>%
  print()

# ------------------------------------------------------------------------------
# 9. Plots
# ------------------------------------------------------------------------------

# 9a. Top 30 verb-object pairs (lollipop)
p_vo <- top_vo %>%
  slice_head(n = 30) %>%
  mutate(pair = fct_reorder(pair, n)) %>%
  ggplot(aes(n, pair)) +
  geom_segment(aes(x = 0, xend = n, y = pair, yend = pair), color = "grey70") +
  geom_point(size = 3, color = "#2c7bb6") +
  labs(title = "Top 30 Verb-Object Pairs — H-2A Task Descriptions",
       subtitle = "Regex extraction (gerunds + infinitives), ag-verb filtered",
       x = "Count", y = NULL) +
  theme_minimal(base_size = 12)
ggsave(paste0(img_dir, "/plot_verb_obj_top30.png"), p_vo, width = 9, height = 8)

# 9b. Task verb trends over time
p_trend <- verb_year %>%
  filter(verb_base %in% top12_verbs, year >= 2019) %>%
  ggplot(aes(year, share, color = verb_base)) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 2) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  labs(title = "Task Verb Share Over Time — H-2A Job Orders",
       x = NULL, y = "Share of verb tokens", color = NULL) +
  theme_minimal(base_size = 11) +
  theme(legend.position = "bottom")
ggsave(paste0(img_dir, "/plot_verb_trends.png"), p_trend, width = 10, height = 6)

# 9c. Faceted top-6 objects per top-10 verb
p_facet <- verb_obj %>%
  filter(verb_base %in% top_verbs$verb_base[1:10]) %>%
  count(verb_base, obj_lemma, sort = TRUE) %>%
  group_by(verb_base) %>%
  slice_max(n, n = 6) %>%
  ungroup() %>%
  mutate(obj_lemma = reorder_within(obj_lemma, n, verb_base)) %>%
  ggplot(aes(n, obj_lemma, fill = verb_base)) +
  geom_col(show.legend = FALSE) +
  facet_wrap(~verb_base, scales = "free_y", ncol = 2) +
  scale_y_reordered() +
  labs(title = "Top Objects per Task Verb — H-2A Job Orders",
       x = "Count", y = NULL) +
  theme_minimal(base_size = 10)
ggsave(paste0(img_dir, "/plot_verb_facet_objects.png"), p_facet,
       width = 10, height = 12)

cat("\nPlots saved to:", out_dir, "\n")

# ------------------------------------------------------------------------------
# 10. Save
# ------------------------------------------------------------------------------
saveRDS(
  list(
    verb_obj    = verb_obj,
    vo_per_case = vo_per_case,
    top_verbs   = top_verbs,
    top_vo      = top_vo,
    verb_year   = verb_year,
    df_clean    = df_clean %>% select(caseNumber, period, dateSubmitted,
                                      year, quarter, text_clean)
  ),
  file = paste0(out_dir, "/tasks_jo.rds")
)
cat("Results saved to tasks_jo.rds\n")
cat(sprintf("Total time: %.1f sec\n", as.numeric(Sys.time() - t0)))
