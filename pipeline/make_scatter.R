# Fast filter-and-plot: any subset of master A01 patents -> F2-style scatter.
# Re-runs in seconds (no spaCy, no patent re-pull).
#
# Usage from command line:
#   Rscript 44_filter_and_plot.R "<filter_expr_text>" [<output_name>]
#
# filter_expr_text is an R expression on metadata columns:
#   patent_id, year, patent_title, patent_abstract, cpc_list, n_cpc
# Examples:
#   "TRUE"                                    -> all A01 patents
#   "grepl('A01D', cpc_list)"                 -> harvesters
#   "year >= 2015"                            -> recent only
#   "grepl('drone', tolower(patent_abstract))"-> drone in abstract
#   "year %in% 2020:2025 & grepl('A01D', cpc_list)"
#
# Or call filter_and_plot() interactively.

library(data.table)
library(ggplot2)
library(ggrepel)
library(ggforce)
library(scales)
library(cluster)
library(arrow)

ROOT <- "output"
OUT  <- file.path(ROOT, "results")

# ---- Load master inputs ONCE ---------------------------------------
# Surface (verb, noun) match. IDF is pre-computed in extract_task_pairs.R
# and lives in task_pairs.parquet.
KEY_COLS <- c("verb","noun")

PIPE <- "pipeline"
PATH_PAT_PAIRS <- file.path(ROOT, "filtered/patent_pairs.parquet")
PATH_TASK_PAIRS <- file.path(ROOT, "pairs/task_pairs.parquet")
PATH_FWD_CITES <- file.path(ROOT, "cache/fwd_cites_2yr.parquet")

# Bootstrap: rebuild missing dependencies.
if (!file.exists(PATH_PAT_PAIRS))  source(file.path(PIPE, "extract_patent_pairs.R"))
if (!file.exists(PATH_TASK_PAIRS)) source(file.path(PIPE, "extract_task_pairs.R"))

META  <- as.data.table(read_parquet(file.path(ROOT, "filtered/patent_metadata.parquet")))
PAIRS <- as.data.table(read_parquet(PATH_PAT_PAIRS))
TP    <- as.data.table(read_parquet(PATH_TASK_PAIRS))
LK    <- fread(file.path(ROOT, "pairs/task_lookup.tsv"))
setnames(PAIRS, "doc_id", "patent_id")
PAIRS[, patent_id := as.character(patent_id)]
FWD <- NULL
if (file.exists(PATH_FWD_CITES)) {
  FWD <- as.data.table(read_parquet(PATH_FWD_CITES))
  FWD[, patent_id := as.character(patent_id)]
}

# FTE side (FY2020-2025, Farmworker-Crop). single_crop=TRUE keeps only n_crops_jo==1 JOs.
compute_fte <- function(single_crop = FALSE,
                        jo_year_min = 2020L, jo_year_max = 2025L,
                        q1q2_only = FALSE,
                        group_by = c("crop","state","jo")) {
  group_by <- match.arg(group_by)
  jof   <- as.data.table(arrow::read_parquet("output/text/core/jo_full.parquet"))
  jof[, sub_month := as.integer(format(as.Date(dateSubmitted), "%m"))]
  jof   <- jof[fiscalYear >= jo_year_min & fiscalYear <= jo_year_max]
  if (q1q2_only) jof <- jof[sub_month %in% c(10L,11L,12L,1L,2L,3L)]
  jof[, contract_days := as.integer(jobEndDate - jobBeginDate)]
  jof[, hrs_per_wk    := suppressWarnings(as.numeric(jobHoursTotal))]
  jof[, fte_per_jo    := wrksCertified * hrs_per_wk * (contract_days / 7) / 2080]
  jof_soc <- jof[grepl("^45-2092", socCode)]

  if (group_by == "state") {
    s <- jof_soc[, .(fte_total = sum(fte_per_jo, na.rm = TRUE),
                     n_jos_fte = .N), by = jobState]
    s <- s[!is.na(jobState) & jobState != "" & fte_total > 0]
    setnames(s, "jobState", "group_id")
  } else if (group_by == "jo") {
    s <- jof_soc[!is.na(fte_per_jo) & fte_per_jo > 0,
                 .(group_id = caseNumber, fte_total = fte_per_jo, n_jos_fte = 1L)]
    if (single_crop) {
      crops <- as.data.table(arrow::read_parquet("output/text/core/jo_crops.parquet"))
      npj <- crops[, .(n_crops_jo = uniqueN(crop_canonical)), by = caseNumber]
      s <- merge(s, npj, by.x = "group_id", by.y = "caseNumber", all.x = FALSE)
      s <- s[n_crops_jo == 1][, n_crops_jo := NULL]
    }
  } else {
    crops <- as.data.table(arrow::read_parquet("output/text/core/jo_crops.parquet"))
    npj <- crops[, .(n_crops_jo = uniqueN(crop_canonical)), by = caseNumber]
    jc <- merge(jof_soc[, .(caseNumber, fte_per_jo)],
                crops[, .(caseNumber, crop_canonical)],
                by = "caseNumber", allow.cartesian = TRUE)
    jc <- merge(jc, npj, by = "caseNumber")
    if (single_crop) jc <- jc[n_crops_jo == 1]
    jc[, fte_share := fte_per_jo / n_crops_jo]
    s <- jc[, .(fte_total = sum(fte_share, na.rm = TRUE),
                n_jos_fte = uniqueN(caseNumber)),
            by = crop_canonical]
    non_crops <- c("date","prune","cattle","sheep","hog","chicken","turkey","goat",
                   "dairy","duck","poultry","aquaculture","apicultura","nursery")
    s <- s[!crop_canonical %in% non_crops & fte_total > 0]
    setnames(s, "crop_canonical", "group_id")
  }
  setorder(s, fte_total)
  s[, fte_pct := 100 * cumsum(n_jos_fte) / sum(n_jos_fte)]
  s
}
fte_summ <- compute_fte(single_crop = FALSE)
cat(sprintf("Master loaded: %d patents, %d pairs, %d groups in FTE side.\n",
            nrow(META), nrow(PAIRS), nrow(fte_summ)))

# ---- Core function -------------------------------------------------
filter_and_plot <- function(filter_expr_text = "TRUE",
                            out_name = NULL,
                            label = filter_expr_text,
                            single_crop = FALSE,
                            jo_year_min = 2020L,
                            jo_year_max = 2025L,
                            weight_cites = FALSE,
                            q1q2_only = FALSE,
                            group_by = c("crop","state","jo"),
                            ai_score_lookup = NULL) {
  group_by <- match.arg(group_by)
  t0 <- Sys.time()
  expr <- parse(text = filter_expr_text)
  keep_ids <- META[eval(expr), patent_id]
  cat(sprintf("Filter '%s' kept %d / %d patents.\n",
              filter_expr_text, length(keep_ids), nrow(META)))
  if (length(keep_ids) < 100) {
    warning("Fewer than 100 patents kept; scores may be noisy.")
  }

  # Recompute FTE side every time (cheap; honors group_by + filters)
  fte_use <- compute_fte(single_crop = single_crop,
                         jo_year_min = jo_year_min, jo_year_max = jo_year_max,
                         q1q2_only = q1q2_only, group_by = group_by)
  cat(sprintf("FTE side (group_by=%s): %s JOs, FY%d-%d%s (%d groups).\n",
              group_by,
              if (single_crop) "single-crop" else "all",
              jo_year_min, jo_year_max,
              if (q1q2_only) " Q1+Q2 only" else "",
              nrow(fte_use)))

  # Patent pair frequency at the chosen match level
  pp <- PAIRS[patent_id %in% keep_ids]
  pp[, w := 1.0]
  if (weight_cites) {
    if (is.null(FWD)) stop("weight_cites=TRUE but fwd_cites_2yr.parquet missing.")
    pp <- merge(pp, FWD, by = "patent_id", all.x = TRUE)
    pp[is.na(fwd_cites_2yr), fwd_cites_2yr := 0]
    pp[, w := w * (1 + log1p(fwd_cites_2yr))]
  }
  if (!is.null(ai_score_lookup)) {
    pp <- merge(pp, ai_score_lookup, by = "patent_id", all.x = TRUE)
    pp[is.na(ai_score), ai_score := 0]
    pp[, w := w * ai_score]
  }
  total_pat <- sum(pp$w)
  pf <- pp[, .(pat_count = sum(w)), by = c(KEY_COLS)]
  cat(sprintf("  weighted: total weight=%.1f over %d pair instances\n",
              total_pat, nrow(pp)))
  if (total_pat <= 0) stop("Total weight is zero; check filters / AI score column.")
  pf[, pat_share := pat_count / total_pat]

  # Score task pairs -> sentence -> JO x crop -> crop.
  # Pair-level score = pat_share * idf  (IDF kills pair-level T&C residue).
  scored <- merge(TP, pf[, c(KEY_COLS, "pat_share"), with = FALSE],
                  by = KEY_COLS, all.x = TRUE)
  scored[is.na(pat_share), pat_share := 0]
  scored[, weighted := pat_share * idf]
  ss <- scored[, .(sent_score = mean(weighted)), by = .(sent_uid = doc_id)]
  lk_use <- LK[, .(sent_uid, caseNumber, crops, year, month, n_crops, jobState)]
  if (single_crop) lk_use <- lk_use[n_crops == 1]
  if (q1q2_only) lk_use <- lk_use[month %in% c(10L,11L,12L,1L,2L,3L)]
  ss <- merge(ss, lk_use, by = "sent_uid")
  if (group_by == "state") {
    long <- ss[!is.na(jobState) & jobState != "" &
                 year >= jo_year_min & year <= jo_year_max,
                .(group_id = jobState),
                by = .(sent_uid, caseNumber, sent_score)]
  } else if (group_by == "jo") {
    long <- ss[year >= jo_year_min & year <= jo_year_max,
                .(group_id = caseNumber),
                by = .(sent_uid, caseNumber, sent_score)]
  } else {
    long <- ss[!is.na(crops) & crops != "" &
                 year >= jo_year_min & year <= jo_year_max,
                .(group_id = unlist(strsplit(crops, ";"))),
                by = .(sent_uid, caseNumber, sent_score)]
  }
  jo_sc <- long[, .(jo_score = mean(sent_score)), by = .(caseNumber, group_id)]
  cs <- jo_sc[, .(n_jos = uniqueN(caseNumber),
                   mean_score = mean(jo_score)), by = group_id]
  setorder(cs, mean_score)
  cs[, exp_pct := 100 * cumsum(n_jos) / sum(n_jos)]

  dt <- merge(fte_use, cs[, .(group_id, exp_pct, n_jos)], by = "group_id")
  dt[, log_fte := log10(fte_total)]
  dt[, target := FALSE]   # no crop highlighted; uniform style

  if (group_by == "jo") {
    # Many points (~thousands). Hexbin density + LOESS trend.
    fig <- ggplot(dt, aes(x = exp_pct, y = fte_total)) +
      geom_hex(bins = 60, colour = NA) +
      scale_fill_viridis_c(option = "B", trans = "log10",
                           name = "JOs / hex", labels = label_comma()) +
      geom_smooth(method = "loess", span = 0.5, se = TRUE,
                  colour = "white", fill = "white", alpha = 0.25,
                  linewidth = 0.7) +
      scale_y_log10(labels = label_comma()) +
      labs(title    = sprintf("JO H-2A FTE vs mechanization-patent exposure"),
           subtitle = sprintf("Filter: %s   |   %d patents, %d JOs, %.0f total pat-pairs",
                               label, length(keep_ids), nrow(dt), total_pat),
           x = "Exposure-to-mechanization-patents percentile (JO rank)",
           y = "H-2A FTE workers per JO (log scale)") +
      theme_minimal(base_size = 11)
  } else {
    # k-means hulls (k=5) on (exp_pct, log_fte) -- crop / state mode.
    X <- as.matrix(dt[, .(exp_pct, log_fte)])
    set.seed(7)
    km <- kmeans(X, centers = min(5, nrow(dt) - 1), nstart = 25)
    dt[, cluster := factor(km$cluster)]
    fig <- ggplot(dt, aes(x = exp_pct, y = fte_total)) +
      geom_mark_hull(aes(group = cluster, fill = cluster),
                     expand = unit(3, "mm"),
                     alpha = 0.10, colour = NA, concavity = 5) +
      geom_point(aes(colour = target), size = 2.2, alpha = .8) +
      geom_text_repel(aes(label = group_id, colour = target),
                      size = 2.7, max.overlaps = Inf,
                      segment.size = 0.2, segment.alpha = 0.4,
                      min.segment.length = 0.1, force = 1.2,
                      fontface = "plain") +
      scale_color_manual(values = c("FALSE"="grey45","TRUE"="firebrick"),
                         guide = "none") +
      scale_fill_brewer(palette = "Set2", guide = "none") +
      scale_y_log10(labels = label_comma()) +
      labs(title    = sprintf("Crop's H-2A FTE workers vs mechanization-patent exposure"),
           subtitle = sprintf("Filter: %s   |   %d patents, %d crops, %.0f total pat-pairs",
                               label, length(keep_ids), nrow(dt), total_pat),
           x = "Exposure-to-mechanization-patents percentile",
           y = "H-2A FTE workers (log scale)") +
      theme_minimal(base_size = 11)
  }

  if (is.null(out_name)) {
    safe <- gsub("[^A-Za-z0-9_]+", "_", filter_expr_text)
    safe <- substr(safe, 1, 60)
    out_name <- paste0("scatter_", safe, ".png")
  }
  out_path <- file.path(OUT, out_name)
  ggsave(out_path, fig, width = 10, height = 6.5, dpi = 150)
  cat(sprintf("Saved: %s   (%.1fs)\n", out_path,
              as.numeric(Sys.time() - t0, units = "secs")))
  invisible(dt)
}

# ---- CLI dispatch --------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1 && !exists("MAKE_SCATTER_NO_DISPATCH", envir = globalenv())) {
  sc <- if (length(args) >= 3) toupper(args[3]) %in% c("TRUE","T","1","SINGLE","SINGLE_CROP") else FALSE
  ymin <- if (length(args) >= 4) as.integer(args[4]) else 2020L
  ymax <- if (length(args) >= 5) as.integer(args[5]) else 2025L
  wc <- if (length(args) >= 6) toupper(args[6]) %in% c("TRUE","T","1","CITES","WEIGHT_CITES") else FALSE
  qq <- if (length(args) >= 7) toupper(args[7]) %in% c("TRUE","T","1","Q1Q2") else FALSE
  gb <- if (length(args) >= 8) tolower(args[8]) else "crop"
  filter_and_plot(filter_expr_text = args[1],
                  out_name = if (length(args) >= 2) args[2] else NULL,
                  single_crop = sc,
                  jo_year_min = ymin, jo_year_max = ymax,
                  weight_cites = wc,
                  q1q2_only = qq,
                  group_by = gb)
}
