###############################################################################
# analyze_crop_bigrams.R — per-crop TF-IDF wordcloud on cleaned single-crop set.
#
# Pipeline (steps 8-12 of the workflow, runs in ~5 min on the full corpus):
#   8. Recount n_tc / n_farm per sentence using the manual bigram dictionary.
#   9. Filter sentences: n_tc == 0 & n_farm > 0 & n_crops == 1.
#  10. Tokenize remaining sentences into bigrams; drop stopwords, short words,
#      and Spanish (using the same expanded list as filter_language.R).
#  11. TF-IDF per crop (document = single-crop sentences for each canonical
#      crop in the filtered set).
#  12. Save PNG wordcloud(s).
#
# Usage:
#   Rscript pipeline/analyze_crop_bigrams.R tomato
#   Rscript pipeline/analyze_crop_bigrams.R tomato blueberry
#   Rscript pipeline/analyze_crop_bigrams.R --all-pairs   # not implemented yet
###############################################################################

suppressPackageStartupMessages({
  library(arrow); library(data.table); library(tidytext); library(dplyr)
  library(tidyr); library(stringr); library(wordcloud); library(RColorBrewer)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  cat("Usage: Rscript analyze_crop_bigrams.R <crop1> [crop2] ...\n")
  cat("Example: Rscript analyze_crop_bigrams.R tomato blueberry\n")
  quit(status = 1)
}
crops_to_plot <- tolower(args)

DICT_PATH <- "output/text/dictionaries/bigram_dictionary_manual.tsv"
SENT_PATH <- "output/text/core/sentences.parquet"
OUT_DIR   <- "output/results"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ---- Manual bigram dictionary (step 7 output, frozen) ---------------------
d <- fread(DICT_PATH)
tc_set   <- d$bigram[d$label == "tc"]
farm_set <- d$bigram[d$label == "farm"]
cat(sprintf("Bigram dict: tc=%d  farm=%d  other=%d\n",
            length(tc_set), length(farm_set), sum(d$label == "other")))

# ---- Spanish + stop word lists (mirrors filter_language.R) ----------------
es_function <- c("los","las","del","una","uno","para","por","sin","con","hasta",
                 "desde","que","porque","cuando","donde","aunque","sus","ella",
                 "ellos","ustedes","nosotros","como","esta","este","estos","estas",
                 "tambien","esto","eso","aqui","alli","muy","mas","menos")
es_content  <- c("trabajadores","empleador","empleadores","vivienda","cosecha",
                 "siembra","salario","herramientas","operador","empleado",
                 "trabajador","ranchero","cuidador","tareas","pastoreo")
es_h2a_extra <- c(
  "animales","ovejas","cabras","caballo","caballos","ganado","perros","pastor",
  "dias","horas","semana","tener","experiencia","contra","evitar",
  "ilegales","armas","traer","drogas","ausencias","excesivas","residencia",
  "deberes","debe","puede","pueden","alimentos","alimento",
  "etiquetar","vacunar","marcar","castrar","sanitarias","comunes",
  "aplicables","aplicable","prohibido","durante",
  "trabajos","trabajo","individuales","asignaciones","equipos",
  "realizar","mantenimiento","reparaciones","reparar","mantener",
  "malezas","hierbas","nocivas","venenosas","latente","tabaco","maduro",
  "repitiendo","despido","inmediato","inmediata",
  "temperaturas","extremas","invernales","suplementarios","suficiente","forraje",
  "positiva","prueba","posibles","problemas",
  "todos","todas","todo","tienen","organizadas",
  "cosechadas","cosechar","cosechados","cosechador","mecanico",
  "vehculos","descargar","mencionados","necesarios","ayudar","cortar",
  "sera","pagado","largos","perodos",
  "agacharse","doblarse","alcanzar","doblar","poder","levantar",
  "restringir","madres","terrenos","cuartas","cuartos","momento",
  "dispuestas","examinar","instrucciones","estacionales","codigo","diversificados",
  "melones","podadora","alrededor","controle","esten","congeladas","mueren",
  "congelados","distribuya","diarias","bandadas",
  "entre","otros","otra","otras","necesidades","nutricionales",
  "mientras","monta","facilitan","ranchos","miembros","equivalente","extranjero",
  "jornada","laboral","laborales","capacidad","incluyen","cticas",
  "operar","trabajar","cercas","ofrecidas","bajo","supervisar",
  "numero","necesitar","transportar","ratones","moscas","camas","bajan",
  "entrenados","conducir","camiones","mover","suministros",
  "aire","libre","prolongada","estar","cualquier","alimentar","gente",
  "manera","segura","encebramiento",
  "aplicar","cumplir","limpiar","limpieza","antes","despues","contrato",
  "puedes","cabezas","cabeza","traila","hojas","piscar","reuniones",
  "trabajadora","aproximadamente","listados","escogidas","empacadas",
  "ofrecer","obstaculizar","comprometerse","propio","propia","propios","propias"
)
es_words <- unique(c(es_function, es_content, es_h2a_extra))
is_spanish <- function(w) {
  w %in% es_words |
  grepl("ci[oó]n$", w) | grepl("mente$", w) | grepl("[ñáéíóú]", w)
}
extra_stop <- c("worker","workers","employer","employee","employees",
                "employment","job","jobs","work","working","required","must",
                "shall","will","may","etc","perform","performed","performing",
                "na","n","h2a","h-2a","ust","include","including","wage",
                "hours","hour","day","days","week","time")

# ---- Load sentences -------------------------------------------------------
cat("Loading sentences.parquet...\n")
s <- as.data.table(read_parquet(SENT_PATH))
s <- s[!is.na(crops) & nchar(crops) > 0]
s[, sid := .I]
# Drop pre-existing n_tc/n_farm (computed against an older dict)
s[, c("n_tc","n_farm") := NULL]
cat("Sentences with a crop tag:", nrow(s), "\n")

# ---- Step 8 + 10a: tokenize once, recount n_tc/n_farm per sid -------------
cat("Tokenizing all sentences into bigrams...\n")
t0 <- Sys.time()
bg_all <- s[, .(sid, sentence)] |>
  unnest_tokens(bigram, sentence, token = "ngrams", n = 2)
setDT(bg_all)
bg_all[, is_tc   := bigram %in% tc_set]
bg_all[, is_farm := bigram %in% farm_set]
cnt <- bg_all[, .(n_tc = sum(is_tc), n_farm = sum(is_farm)), by = sid]
s <- merge(s, cnt, by = "sid", all.x = TRUE)
s[is.na(n_tc),   n_tc   := 0L]
s[is.na(n_farm), n_farm := 0L]
cat(sprintf("  tokenize+score: %.1f min\n",
            as.numeric(Sys.time() - t0, units = "mins")))

# ---- Step 9: filter sentences ---------------------------------------------
keep <- s[n_tc == 0 & n_farm > 0 & n_crops == 1, .(sid, crops)]
cat(sprintf("After filters [n_tc=0 & n_farm>0 & n_crops=1]: %d sentences\n",
            nrow(keep)))

# ---- Step 10b: clean bigrams ----------------------------------------------
bg <- bg_all[sid %in% keep$sid]
bg[, c("w1","w2") := tstrsplit(bigram, " ", fixed = TRUE)]
bg <- bg[!is.na(w1) & !is.na(w2) &
         !w1 %in% stop_words$word & !w2 %in% stop_words$word &
         !w1 %in% extra_stop & !w2 %in% extra_stop &
         str_detect(w1, "^[a-z]{4,}$") & str_detect(w2, "^[a-z]{4,}$") &
         !is_spanish(w1) & !is_spanish(w2)]
bg <- merge(bg[, .(sid, bigram)], keep, by = "sid")
cat(sprintf("Clean bigram-rows: %d\n", nrow(bg)))

# ---- Step 11: per (crop, bigram) frequency, IDF ---------------------------
crop_bg <- bg[, .N, by = .(crops, bigram)]
n_crops <- uniqueN(crop_bg$crops)
cat(sprintf("Distinct crops in filtered set: %d\n", n_crops))
df_bg <- crop_bg[, .(df = uniqueN(crops)), by = bigram]
df_bg[, idf := log(n_crops / df)]

tfidf_for <- function(crop_name, min_n = 5) {
  doc <- crop_bg[crops == crop_name][, .(bigram, N)]
  if (nrow(doc) == 0) return(NULL)
  total <- sum(doc$N)
  doc[, tf := N / total]
  doc <- merge(doc, df_bg[, .(bigram, idf)], by = "bigram", all.x = TRUE)
  doc[is.na(idf), idf := 0]
  doc[, tfidf := tf * idf]
  doc <- doc[N >= min_n & is.finite(tfidf) & tfidf > 0]
  doc[order(-tfidf)]
}

# ---- Step 12: wordclouds + console preview --------------------------------
results <- list()
for (cn in crops_to_plot) {
  res <- tfidf_for(cn)
  if (is.null(res) || nrow(res) == 0) {
    cat(sprintf("\n[!] No data for crop '%s' (no single-crop sentences passed filters).\n", cn))
    next
  }
  results[[cn]] <- res
  cat(sprintf("\n=== Top 25 %s (TF-IDF) ===\n", toupper(cn)))
  print(res[1:min(25, nrow(res)), .(bigram, n = N, idf = round(idf, 2),
                                     tfidf = round(tfidf, 5))])
}

if (length(results) > 0) {
  png_path <- file.path(OUT_DIR,
    sprintf("wordcloud_%s_tfidf.png", paste(crops_to_plot, collapse = "_vs_")))
  n_panels <- length(results)
  png(png_path, width = max(1600, 1200 * n_panels), height = 1400, res = 200)
  par(mfrow = c(1, n_panels), mar = c(0, 0, 2, 0))
  palettes <- c("Dark2","Set1","Set2","Set3")
  for (i in seq_along(results)) {
    res <- results[[i]]
    top <- head(res, 200)
    wordcloud(words = top$bigram, freq = top$tfidf,
              min.freq = 0, max.words = 200,
              random.order = FALSE, rot.per = 0.15,
              colors = brewer.pal(8, palettes[((i - 1) %% length(palettes)) + 1]),
              scale = c(3.5, 0.4))
    title(sprintf("%s (clean filters, TF-IDF)", names(results)[i]),
          cex.main = 1)
  }
  dev.off()
  cat(sprintf("\nSaved: %s\n", png_path))
}
