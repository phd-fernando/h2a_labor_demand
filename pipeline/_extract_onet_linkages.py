"""Extract O*NET linkage matrices from HumRRO PDF appendices.
- AnalystProc Appendix B  -> GWA x Abilities, WC x Abilities  (52 abilities)
- AOSkills_Proc Appendix E -> WC x Skills (35 skills)

Cell encoding (per legend):
  digit 1-4 = rater count below threshold (NOT linked)
  digit 5-8 = rater count at/above threshold, no consensus review needed (LINKED)
  'X'       = linked (>4 raters, consensus confirmed)
  '#'       = de-linked by consensus (NOT linked)
  ''/None   = no rater linked (NOT linked)

Final 0/1 rule: linked iff cell == 'X' OR cell digit >= 5; de-linked otherwise.
"""
import pdfplumber, csv, os
from pathlib import Path

OUT_DIR = Path("output/text/dictionaries")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ABILITIES_52 = [
    "Oral Comprehension","Written Comprehension","Oral Expression","Written Expression",
    "Fluency of Ideas","Originality","Problem Sensitivity","Deductive Reasoning",
    "Inductive Reasoning","Information Ordering","Category Flexibility",
    "Mathematical Reasoning","Number Facility","Memorization","Speed of Closure",
    "Flexibility of Closure","Perceptual Speed","Spatial Orientation","Visualization",
    "Selective Attention","Time Sharing","Arm-Hand Steadiness","Manual Dexterity",
    "Finger Dexterity","Control Precision","Multilimb Coordination",
    "Response Orientation","Rate Control","Reaction Time","Wrist-Finger Speed",
    "Speed of Limb Movement","Static Strength","Explosive Strength","Dynamic Strength",
    "Trunk Strength","Stamina","Extent Flexibility","Dynamic Flexibility",
    "Gross Body Coordination","Gross Body Equilibrium","Near Vision","Far Vision",
    "Visual Color Discrimination","Night Vision","Peripheral Vision","Depth Perception",
    "Glare Sensitivity","Hearing Sensitivity","Auditory Attention","Sound Localization",
    "Speech Recognition","Speech Clarity",
]
assert len(ABILITIES_52) == 52

SKILLS_35 = [
    "Reading Comprehension","Active Listening","Writing","Speaking","Mathematics","Science",
    "Critical Thinking","Active Learning","Learning Strategies","Monitoring",
    "Social Perceptiveness","Coordination","Persuasion","Negotiation","Instructing",
    "Service Orientation","Complex Problem Solving","Operations Analysis","Technology Design",
    "Equipment Selection","Installation","Programming","Operations Monitoring",
    "Operation and Control","Equipment Maintenance","Troubleshooting","Repairing",
    "Quality Control Analysis","Judgment and Decision Making","Systems Analysis",
    "Systems Evaluation","Time Management","Management of Financial Resources",
    "Management of Material Resources","Management of Personnel Resources",
]
assert len(SKILLS_35) == 35

GWA_NAMES = [
    "Getting Information","Identifying Objects, Actions, and Events",
    "Monitoring Processes, Materials, or Surroundings",
    "Inspecting Equipment, Structures, or Materials",
    "Estimating the Quantifiable Characteristics of Products, Events, or Information",
    "Judging the Qualities of Objects, Services, or People",
    "Evaluating Information to Determine Compliance with Standards",
    "Processing Information","Analyzing Data or Information",
    "Making Decisions and Solving Problems","Thinking Creatively",
    "Updating and Using Relevant Knowledge","Developing Objectives and Strategies",
    "Scheduling Work and Activities","Organizing, Planning, and Prioritizing Work",
    "Performing General Physical Activities","Handling and Moving Objects",
    "Controlling Machines and Processes",
    "Working with Computers","Operating Vehicles, Mechanized Devices, or Equipment",
    "Drafting, Laying Out, and Specifying Technical Devices, Parts, and Equipment",
    "Repairing and Maintaining Mechanical Equipment",
    "Repairing and Maintaining Electronic Equipment",
    "Documenting/Recording Information","Interpreting the Meaning of Information for Others",
    "Communicating with Supervisors, Peers, or Subordinates",
    "Communicating with People Outside the Organization",
    "Establishing and Maintaining Interpersonal Relationships",
    "Assisting and Caring for Others","Selling or Influencing Others",
    "Resolving Conflicts and Negotiating with Others",
    "Performing for or Working Directly with the Public",
    "Coordinating the Work and Activities of Others",
    "Developing and Building Teams","Training and Teaching Others",
    "Guiding, Directing, and Motivating Subordinates","Coaching and Developing Others",
    "Providing Consultation and Advice to Others","Performing Administrative Activities",
    "Staffing Organizational Units","Monitoring and Controlling Resources",
]
assert len(GWA_NAMES) == 41


def is_linked(cell):
    """Graded linkage strength: digit/8, X=1, #=0, blank=0.
    Returns float in [0, 1]."""
    if cell is None:
        return 0.0
    s = str(cell).strip()
    if s == "":
        return 0.0
    if s == "X":
        return 1.0
    if s == "#":
        return 0.0
    if s.isdigit():
        return int(s) / 8.0
    return 0.0


def parse_matrix_pages(pdf_path, page_indices, n_cols_per_page, target_cols, n_rows):
    """Read a multi-page numeric matrix.

    page_indices: list of 0-indexed PDF pages containing the matrix
    n_cols_per_page: list of int -- expected number of data columns on each page
    target_cols: list of column names (length = sum(n_cols_per_page))
    n_rows: expected number of data rows

    Returns: list-of-lists of length [n_rows][len(target_cols)] of 0/1.
    """
    assert sum(n_cols_per_page) == len(target_cols)
    grid = [[0] * len(target_cols) for _ in range(n_rows)]
    col_offset = 0
    with pdfplumber.open(pdf_path) as pdf:
        for pg_idx, n_cols in zip(page_indices, n_cols_per_page):
            page = pdf.pages[pg_idx]
            tables = page.extract_tables()
            if not tables:
                raise RuntimeError(f"No tables on page {pg_idx+1}")
            t = tables[0]
            # Find the rows that are numeric data: row[1] (or similar early col) is the row index 1..n_rows
            data_rows = []
            for row in t:
                # find any cell that is purely a digit and within 1..n_rows -- that signals start of data row
                candidate = None
                for ci, c in enumerate(row[:5]):
                    if c is not None and str(c).strip().isdigit():
                        v = int(str(c).strip())
                        if 1 <= v <= n_rows:
                            candidate = (ci, v)
                            break
                if candidate is not None:
                    data_rows.append((candidate[1], candidate[0], row))
            if len(data_rows) != n_rows:
                # take first n_rows
                data_rows = data_rows[:n_rows]
            # AnalystProc tables: pdfplumber returns exactly 27 cols per page row =
            # 1 GWA-id col + 26 ability cols (positionally aligned, empties preserved).
            # For these PDFs, id_ci is always 0; columns 1..26 map to abilities in order.
            for row_id, id_ci, row in data_rows:
                values = [(str(c).strip() if c is not None else "") for c in row[id_ci+1:id_ci+1+n_cols]]
                if len(values) < n_cols:
                    values = values + [""] * (n_cols - len(values))
                for j, val in enumerate(values):
                    grid[row_id - 1][col_offset + j] = is_linked(val)
            col_offset += n_cols
    return grid


def main():
    # ---- GWA x Abilities (AnalystProc pages 36-37, 0-indexed 35-36) ----
    gwa_abil = parse_matrix_pages(
        "papers/AnalystProc.pdf",
        page_indices=[35, 36],
        n_cols_per_page=[26, 26],
        target_cols=ABILITIES_52,
        n_rows=41,
    )
    write_matrix(OUT_DIR / "onet_gwa_x_abilities.tsv",
                 row_label="GWA", row_names=GWA_NAMES,
                 col_names=ABILITIES_52, grid=gwa_abil)

    # ---- WC x Abilities (AnalystProc pages 32-35, 0-indexed 31-34) ----
    # Skipping for now; user asked specifically for GWA->{abilities,skills}.
    # Could be added later by inspecting page structure.

    # ---- WC x Skills (AOSkills pages 39-41, 0-indexed 38-40) ----
    # Skipping the WC matrices for the immediate goal too.

    print("Done. GWA x Abilities matrix saved.")
    print(f"  rows = {len(gwa_abil)}, cols = {len(gwa_abil[0])}")
    n_links = sum(sum(r) for r in gwa_abil)
    print(f"  cells linked: {n_links} / {len(gwa_abil) * len(gwa_abil[0])}")


def write_matrix(path, row_label, row_names, col_names, grid):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([row_label] + col_names)
        for name, row in zip(row_names, grid):
            w.writerow([name] + row)
    # also long format
    long_path = str(path).replace(".tsv", "_long.tsv")
    with open(long_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow([row_label.lower(), "target", "link"])
        for name, row in zip(row_names, grid):
            for cn, v in zip(col_names, row):
                w.writerow([name, cn, v])
    print(f"  wrote {path}")
    print(f"  wrote {long_path}")


if __name__ == "__main__":
    main()
