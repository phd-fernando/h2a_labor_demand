"""Extract the 2012 (Update) GWA x Abilities linkage matrix from AnalystProcUpdate.pdf
and compare to the 2003 version we already have.
"""
import pdfplumber, csv, sys, io
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

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

def is_linked(c):
    if c is None: return 0
    s = str(c).strip()
    if s == "": return 0
    if s == "X": return 1
    if s == "#": return 0
    if s.isdigit(): return 1 if int(s) >= 5 else 0
    return 0

OUT_DIR = Path("output/text/dictionaries")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# The 2012 matrix is on pages 44-45 (0-indexed 43-44).
grid = [[0]*52 for _ in range(41)]
with pdfplumber.open("papers/AnalystProcUpdate.pdf") as pdf:
    col_offset = 0
    for pg_idx in [43, 44]:
        page = pdf.pages[pg_idx]
        t = page.extract_tables()[0]
        # find rows where row[0] is a digit 1..41
        for row in t:
            if row[0] is None: continue
            s = str(row[0]).strip()
            if s.isdigit() and 1 <= int(s) <= 41:
                gid = int(s)
                values = [(str(c).strip() if c is not None else "")
                          for c in row[1:1+26]]
                if len(values) < 26:
                    values = values + [""]*(26-len(values))
                for j, v in enumerate(values):
                    grid[gid-1][col_offset + j] = is_linked(v)
        col_offset += 26

# Compare with 2003 matrix
import csv
with open("output/text/dictionaries/onet_gwa_x_abilities.tsv", encoding='utf-8') as f:
    r = list(csv.reader(f, delimiter='\t'))
hdr = r[0]
old_grid = []
for row in r[1:]:
    old_grid.append([int(x) for x in row[1:]])

# Diff
n_changed = 0
diffs = []
for gi in range(41):
    for ai in range(52):
        if grid[gi][ai] != old_grid[gi][ai]:
            n_changed += 1
            diffs.append((GWA_NAMES[gi], ABILITIES_52[ai], old_grid[gi][ai], grid[gi][ai]))

print(f"Cells changed between 2003 and 2012: {n_changed}")
print(f"2003 total links: {sum(sum(r) for r in old_grid)}")
print(f"2012 total links: {sum(sum(r) for r in grid)}")

# focus on previously zero-coverage abilities
zero_2003 = ["Speed of Closure","Speed of Limb Movement","Explosive Strength",
             "Dynamic Flexibility","Gross Body Equilibrium"]
print("\nLinks for previously zero-coverage abilities in 2012:")
for a in zero_2003:
    ai = ABILITIES_52.index(a)
    linked = [GWA_NAMES[g] for g in range(41) if grid[g][ai] == 1]
    print(f"  {a}: {len(linked)} linked GWAs")
    for g in linked:
        print(f"    - {g}")

# Save 2012 matrix
with open(OUT_DIR / "onet_gwa_x_abilities_2012.tsv", "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f, delimiter='\t')
    w.writerow(["GWA"] + ABILITIES_52)
    for gname, row in zip(GWA_NAMES, grid):
        w.writerow([gname] + row)
print(f"\nSaved 2012 matrix -> {OUT_DIR}/onet_gwa_x_abilities_2012.tsv")
