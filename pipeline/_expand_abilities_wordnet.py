"""Expand each ability's keyword list with WordNet synonyms.
Strategy: use the top-3 synsets per word (most common senses).
Apply light-verb / generic stoplist after expansion to drop noise.
Output: TSV with one row per (ability, term, source).
"""
import csv
from nltk.corpus import wordnet as wn

ABILITIES = {
  "Trunk Strength":               ["abdominal","fatigue","fatiguing","lower back","back muscles","give out"],
  "Arm-Hand Steadiness":          ["steady","hand and arm","arm steady","hand steady","one position"],
  "Control Precision":            ["adjust","controls","adjust the controls","exact positions"],
  "Extent Flexibility":           ["bend","stretch","twist","reach"],
  "Manual Dexterity":             ["grasp","manipulate","assemble","two hands"],
  "Multilimb Coordination":       ["coordinate","limbs","two or more limbs"],
  "Finger Dexterity":             ["fingers","grasp","manipulate","assemble","small objects"],
  "Static Strength":              ["exert","muscle force","push pull","maximum muscle"],
  "Dynamic Strength":             ["exert","muscular endurance","muscle fatigue","muscle force"],
  "Near Vision":                  ["close range","see details","few feet"],
  "Rate Control":                 ["anticipation","piece of equipment","speed and direction","moving object"],
  "Stamina":                      ["exert","physically","winded","long periods","out of breath"],
  "Explosive Strength":           ["propel","sprinting","short bursts","throw an object"],
  "Oral Expression":              ["communicate","information and ideas","in speaking"],
  "Reaction Time":                ["respond","quickly respond"],
  "Far Vision":                   ["see details","at a distance"],
  "Gross Body Coordination":      ["coordinate","torso","body is in motion","whole body"],
  "Oral Comprehension":           ["listen","spoken words","spoken sentences"],
  "Deductive Reasoning":          ["general rules","specific problems","produce answers"],
  "Gross Body Equilibrium":       ["regain","upright","unstable","body balance","unstable position"],
  "Problem Sensitivity":          ["go wrong","something is wrong"],
  "Speech Clarity":               ["speak clearly"],
  "Speech Recognition":           ["speech","speech of another"],
  "Category Flexibility":         ["sets of rules","combining or grouping"],
  "Inductive Reasoning":          ["combine pieces","general rules","unrelated events"],
  "Written Expression":           ["communicate","writing","in writing"],
  "Information Ordering":         ["certain order","set of rules","mathematical operations"],
  "Response Orientation":         ["respond","two or more movements"],
  "Selective Attention":          ["concentrate","distracted","without being distracted"],
  "Visualization":                ["imagine","rearranged","moved around"],
  "Dynamic Flexibility":          ["repeatedly bend","repeatedly stretch","repeatedly twist","repeatedly reach"],
  "Flexibility of Closure":       ["known pattern","distracting material","hidden in"],
  "Fluency of Ideas":             ["number of ideas"],
  "Originality":                  ["unusual","clever","creative","creative ways"],
  "Spatial Orientation":          ["in relation to","location in relation"],
  "Written Comprehension":        ["read and understand","in writing"],
  "Depth Perception":             ["farther","judge the distance"],
  "Time Sharing":                 ["back and forth","sources of information"],
  "Wrist-Finger Speed":           ["wrist","wrists","fast simple repeated"],
  "Perceptual Speed":             ["similarities and differences","compare similarities"],
  "Speed of Closure":             ["meaningful patterns","make sense of"],
  "Speed of Limb Movement":       ["quickly move the arms"],
  "Visual Color Discrimination":  ["color","colors","shades","brightness","shades of color"],
  "Auditory Attention":           ["source of sound","single source"],
  "Hearing Sensitivity":          ["pitch","loudness","pitch and loudness"],
  "Mathematical Reasoning":       ["mathematical","formulas","mathematical methods"],
  "Memorization":                 ["remember","remember information","remember procedures"],
  "Number Facility":              ["subtract","multiply","divide"],
  "Peripheral Vision":            ["looking ahead","peripheral vision"],
  "Glare Sensitivity":            ["glare","bright lighting"],
  "Sound Localization":           ["sound originated","direction from which"],
  "Night Vision":                 ["low light","low-light"],
}

LIGHT_STOP = {
  "keep","make","use","do","get","take","give","go","come","look","move","hold",
  "set","put","find","see","tell","say","leave","run","work","stand","sit","lie",
  "lay","bring","send","know","apply","choose","regain","arrange","produce",
  "develop","combine","generate","change","shift","focus","read","general","specific",
  "different","certain","particular","single","simple","short","long","small",
  "large","common","various","other","wrong","likely","quickly","clearly","fast",
  "be","have","that","this","one","two","up","off","on","out","over","ability",
  "thing","way","time","part","kind","number","amount","state","object","item",
  "matter","stuff","material","piece","point","level"
}

def expand(term, max_synsets=3):
  """Return set of single-word synonyms for term, top-N synsets, all POS."""
  out = set()
  if " " in term or "-" in term:
    return out  # skip multi-word; WordNet works on single tokens
  syns = wn.synsets(term)[:max_synsets]
  for s in syns:
    for lemma in s.lemma_names():
      l = lemma.replace("_", " ").lower().strip()
      if not l or l == term:
        continue
      # skip multi-word lemmas (could keep but adds noise)
      if " " in l or "-" in l:
        continue
      if l in LIGHT_STOP:
        continue
      if not l.isalpha():
        continue
      if len(l) < 3:
        continue
      out.add(l)
  return out

rows = []
for ability, terms in ABILITIES.items():
  seen = set()
  for t in terms:
    seen.add(t)
    rows.append((ability, t, "onet"))
  # expand single-word terms
  for t in terms:
    if " " in t or "-" in t:
      continue
    for syn in expand(t):
      if syn not in seen:
        seen.add(syn)
        rows.append((ability, syn, "wordnet"))

with open("output/text/dictionaries/onet_ability_terms_wn.tsv", "w",
          newline="", encoding="utf-8") as f:
  w = csv.writer(f, delimiter="\t")
  w.writerow(["ability","term","source"])
  for r in rows:
    w.writerow(r)

# Summary
from collections import Counter
src_counts = Counter(r[2] for r in rows)
print(f"Total rows: {len(rows)}")
print(f"Sources: {dict(src_counts)}")
print(f"Unique abilities: {len(ABILITIES)}")
ab_counts = Counter(r[0] for r in rows)
print("Top expansions:")
for a, n in ab_counts.most_common(10):
  print(f"  {a}: {n} terms")
