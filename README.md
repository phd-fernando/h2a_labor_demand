# H-2A Labor Demand — Master Job Order Dataset

**Project:** Fernando Brito, PhD Dissertation
**Department:** Food and Resource Economics, University of Florida
**Last updated:** 2026-04-01

---

## Master Dataset: `processed/text/df_jobs_enriched.rds`

This is the authoritative, analysis-ready dataset. **Always use this file** for dissertation analyses.

| Attribute | Value |
|---|---|
| Rows | 185,983 |
| Unit of observation | One H-2A job order (JO) |
| Coverage | FY2006 – Feb 2026 |
| Key identifier | `caseNumber` (unique, no duplicates) |

### How it is constructed

The master dataset is built in three layers, then enriched with OFLC disclosure data:

```
Layer 1: JSON snapshots (raw/data/jo/)
    ↓  script 6_extract_structured_fields.R
Layer 2: PDF gap fill (raw/data/pdf_gap/)
    ↓  scripts 7–8_parse_gap_pdfs.R
    = df_jobs_patched.rds  [135,730 rows; 2019–2026]
    ↓  script 10_enrich_from_disclosure.R
Layer 3: OFLC disclosure (raw/data/disclosure/h2a_combined.rds)
    = df_jobs_enriched.rds [185,983 rows; 2006–2026]
```

---

## Data Sources

### Layer 1 — JSON API Snapshots

- **Location:** `raw/data/jo/`
- **Format:** One JSON file per bi-weekly snapshot from `seasonaljobs.dol.gov`
- **Coverage:** 160 snapshots, 2019–2026
- **Per-year snapshot counts:**

| Year | Snapshots |
|------|-----------|
| 2019 | 5 |
| 2020 | 19 |
| 2021 | 19 |
| 2022 | 20 |
| 2023 | 19 |
| 2024 | 38 |
| 2025 | 35 |
| 2026 | 5 |

- Each snapshot is the full set of *currently active* job orders at that date.
- Deduplication: one row per `caseNumber`, keeping the earliest-snapshot record (most reliable `dateSubmitted`).

### Layer 2 — PDF Gap Fill

- **Location:** `raw/data/pdf_gap/` (10,048 PDF files)
- **Purpose:** Fills coverage gaps in 2023–2024 where JSON snapshots missed job orders that had already closed.
- **Contribution:** 6,136 records in 2023; 3,910 in 2024.

### Layer 3 — Out-of-Project ZIP Backfill

- **Location:** `C:/Data/DOL_SeasonalJobs/zip_backfill_15day/` (28 zip files, 2024–2026)
- **Purpose:** Extended the JSON coverage with 15-day-interval snapshots stored outside the project.
- **Net contribution:** 238 records (8 for 2025; 230 for 2026).
- **Note:** 99.3% of these records were already in the dataset; only 238 were genuinely new.

### Layer 4 — OFLC Disclosure Data

- **Location:** `raw/data/disclosure/`
- **Files:** `h2a_combined.rds` (255,304 rows; FY2006–FY2025)
- **Built by:** `scripts/harmonize_h2a.R` → `scripts/0_append_disclosed_h2a.R`
- **Fiscal years:** FY2006–FY2025 Q4 (20 xlsx files)
- **Purpose (dual role):**
  1. **Enrichment:** Adds disclosure-sourced fields (FEIN, NAICS, SOC, certified workers, wages, case status) to existing JSON/PDF records via left-join on `caseNumber`.
  2. **Pre-2019 backfill:** 50,253 case numbers that predate the JSON feed (FY2006–FY2018) are appended as disclosure-only records.

---

## Per-Year Record Counts (authoritative)

From `df_jobs_patched.rds` (the JSON+PDF+ZIP layer, before disclosure enrichment):

| Year | Source | N | Notes |
|------|--------|---|-------|
| 2019 | JSON | 4,409 | |
| 2020 | JSON | 15,524 | |
| 2021 | JSON | 18,126 | |
| 2022 | JSON | 20,161 | |
| 2023 | JSON + PDF gap | 22,319 | 6,136 from PDF |
| 2024 | JSON + PDF gap | 24,845 | 3,910 from PDF |
| 2025 | JSON + ZIP | 25,333 | 8 from ZIP; 1,964 shutdown-flagged |
| 2026 | JSON + ZIP | 5,013 | 230 from ZIP; partial year (through Feb 14) |

---

## Important Data Notes

### October–November 2025 Government Shutdown

The federal government shutdown ran **October 1 – November 12, 2025** (43 days — longest in US history). The DOL filing portal was unavailable during this period.

- **Zero filings** recorded October 2–30, 2025.
- **411 JOs** were batch-stamped November 1, 2025 when the portal restored (backlog flush).
- Backfill ISO timestamps confirm: these 411 Nov-1 cases cannot be rerouted to October — November 1 is the genuine DOL batch timestamp.
- **`shutdown_flag` column** in `df_jobs_patched.rds` and `df_jobs_enriched.rds` marks all JOs with `dateSubmitted` in the Oct 1 – Nov 12, 2025 window (`TRUE` for 1,964 cases).

**Recommended usage:**
- Monthly time-series: exclude `shutdown_flag == TRUE` or pool Oct+Nov 2025 into a single period.
- Annual totals: no exclusion needed — all remain 2025 records.

### `df_jobs_enriched.rds` vs `df_jobs_patched.rds`

| File | Rows | Use for |
|------|------|---------|
| `df_jobs_enriched.rds` | 185,983 | All analyses — most complete; has FEIN, NAICS, SOC, wages, certified workers |
| `df_jobs_patched.rds` | 135,730 | Duties-text analyses; JO counts; confirmed-complete 2019–2026 coverage |

The enriched file has more rows because it includes 50,253 pre-2019 disclosure-only records (no duties text, no JSON fields).

### FY2025 Disclosure Note

The OFLC FY2025 disclosure file covers only **Q4 of FY2025** (approximately July–September 2025, released 2026). The full FY2025 file (October 2024 – June 2025) was not available at the time of writing. For 2025 JO counts and duties-text analysis, `df_jobs_patched.rds` is complete regardless, as it is sourced from the JSON API — not from disclosure.

---

## Key Variables

| Variable | Source | Description |
|---|---|---|
| `caseNumber` | JSON | Unique job order ID (e.g., `JO-A-123456789`) |
| `dateSubmitted` | JSON → backfill-corrected | Employer submission date |
| `jobDuties` | JSON / PDF | Free-text duties description (used for automation analysis) |
| `jobWrksNeeded` | JSON / disclosure | Workers requested |
| `wrksCertified` | Disclosure | Workers certified by DOL |
| `empFein` | JSON / disclosure | Employer Federal EIN (9-digit, no dashes) |
| `empNaics` | JSON / disclosure | NAICS industry code |
| `socCode` | JSON / disclosure | SOC occupation code |
| `jobWageOffer` | JSON / disclosure | Offered wage rate |
| `jobWageHourly` | Derived | Wage normalized to hourly |
| `primaryCrop` | Disclosure | Declared primary crop (FY2015–FY2024 only) |
| `caseStatus` | Disclosure | Certification status |
| `shutdown_flag` | Derived | TRUE if dateSubmitted falls in Oct 1 – Nov 12, 2025 shutdown window |
| `source` | Derived | `"json"`, `"pdf_gap"`, or `"disclosure"` |

---

## Rebuild Instructions

To fully rebuild the master dataset from scratch:

```r
# 1. Build h2a_combined.rds (disclosure layer)
source("scripts/harmonize_h2a.R")
harmonize_h2a("raw/data/disclosure/")

# 2. Build df_jobs_patched.rds (JSON + PDF + ZIP layers)
#    Run scripts 1–8 sequentially (see scripts/ folder)

# 3. Rebuild df_jobs_enriched.rds (master enriched file)
source("scripts/10_enrich_from_disclosure.R")
```

To add a new disclosure FY file:
1. Place xlsx in `raw/data/disclosure/`
2. Add its path to `files_to_load` in `scripts/harmonize_h2a.R`
3. Run steps 1 and 3 above.

---

## File Structure

```
h2a_labor_demand/
├── raw/
│   └── data/
│       ├── jo/                    # JSON API snapshots (2019–2026)
│       ├── pdf_gap/               # PDF gap-fill files (2023–2024)
│       └── disclosure/
│           ├── H-2A_Disclosure_Data_FY2025_Q4.xlsx  ← newest
│           ├── H-2A_Disclosure_Data_FY2024.xlsx
│           ├── ... (FY2006–FY2023)
│           └── h2a_combined.rds   # harmonized disclosure (255,304 rows)
├── processed/
│   └── text/
│       ├── df_jobs_enriched.rds   # MASTER DATASET (185,983 rows)
│       ├── df_jobs_patched.rds    # JSON+PDF+ZIP only (135,730 rows)
│       └── automation_signals_*.rds  # crop-level automation results
├── scripts/
│   ├── harmonize_h2a.R            # disclosure harmonization
│   ├── 10_enrich_from_disclosure.R  # builds master enriched file
│   ├── 18–30_automation_signals_*.R # automation analysis by crop
│   └── ...
└── notes/
    └── backfill_audit_2025_2026.txt  # shutdown artifact documentation
```
