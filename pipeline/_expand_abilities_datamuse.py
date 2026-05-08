"""Expand each O*NET ability's single-word terms via Datamuse 'means like' API.
Caches results in a JSON to avoid re-fetching.
Writes expanded term list to output/text/dictionaries/onet_ability_terms_dm.tsv.
"""
import csv, json, os, sys, time
from pathlib import Path
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

ABILITIES = {
  "Trunk Strength":            ["abdominal","back","muscle","support","body","repeatedly","continuously","fatigue"],
  "Arm-Hand Steadiness":       ["hand","arm","steady","moving","holding","position"],
  "Control Precision":         ["quickly","repeatedly","adjust","control","machine","vehicle","exact","position"],
  "Extent Flexibility":        ["bend","stretch","twist","reach","body","arm","leg"],
  "Manual Dexterity":          ["move","hand","arm","grasp","manipulate","assemble","object"],
  "Multilimb Coordination":    ["coordinate","limb","arm","leg","sitting","standing","lying","motion"],
  "Finger Dexterity":          ["coordinated","movement","finger","hand","grasp","manipulate","assemble","small","object"],
  "Static Strength":           ["exert","maximum","muscle","force","lift","push","pull","carry","object"],
  "Dynamic Strength":          ["exert","muscle","force","repeatedly","continuously","endurance","resistance","fatigue"],
  "Near Vision":               ["see","detail","close","range","foot","observer"],
  "Rate Control":              ["movement","equipment","anticipation","change","speed","direction","moving","object","scene"],
  "Stamina":                   ["exert","physically","long","period","winded","breath"],
  "Explosive Strength":        ["short","burst","muscle","force","propel","jumping","sprinting","throw","object"],
  "Oral Expression":           ["communicate","information","idea","speaking","understand"],
  "Reaction Time":             ["respond","hand","finger","foot","signal","sound","light","picture"],
  "Far Vision":                ["see","detail","distance"],
  "Gross Body Coordination":   ["coordinate","movement","arm","leg","torso","body","motion"],
  "Oral Comprehension":        ["listen","understand","information","idea","spoken","word","sentence"],
  "Deductive Reasoning":       ["apply","general","rule","specific","problem","answer","sense"],
  "Gross Body Equilibrium":    ["keep","regain","body","balance","upright","unstable","position"],
  "Problem Sensitivity":       ["wrong","likely","solving","problem","recognizing"],
  "Speech Clarity":            ["speak","clearly","understand"],
  "Speech Recognition":        ["identify","understand","speech","person"],
  "Category Flexibility":      ["generate","different","set","rule","combining","grouping"],
  "Inductive Reasoning":       ["combine","piece","information","general","rule","conclusion","relationship","event"],
  "Written Expression":        ["communicate","information","idea","writing","understand"],
  "Information Ordering":      ["arrange","action","order","pattern","rule","number","letter","word","picture","mathematical","operation"],
  "Response Orientation":      ["choose","movement","response","signal","light","sound","picture","speed","hand","foot","body"],
  "Selective Attention":       ["concentrate","task","period","distracted"],
  "Visualization":             ["imagine","look","moved","part","rearranged"],
  "Dynamic Flexibility":       ["bend","stretch","twist","reach","body","arm","leg"],
  "Flexibility of Closure":    ["identify","detect","pattern","figure","object","word","sound","distracting","material"],
  "Fluency of Ideas":          ["number","idea","topic","quality","correctness","creativity"],
  "Originality":               ["unusual","clever","idea","topic","situation","develop","creative","solve","problem"],
  "Spatial Orientation":       ["know","location","relation","environment","object"],
  "Written Comprehension":     ["read","understand","information","idea","writing"],
  "Depth Perception":          ["judge","object","closer","farther","distance"],
  "Time Sharing":              ["shift","activity","source","information","speech","sound","touch"],
  "Wrist-Finger Speed":        ["fast","simple","repeated","movement","finger","hand","wrist"],
  "Perceptual Speed":          ["compare","similarity","difference","letter","number","object","picture","pattern","presented","remembered"],
  "Speed of Closure":          ["make","sense","combine","organize","information","meaningful","pattern"],
  "Speed of Limb Movement":    ["move","arm","leg"],
  "Visual Color Discrimination":["match","detect","difference","color","shade","brightness"],
  "Auditory Attention":        ["focus","single","source","sound","distracting"],
  "Hearing Sensitivity":       ["detect","difference","sound","pitch","loudness"],
  "Mathematical Reasoning":    ["choose","mathematical","method","formula","solve","problem"],
  "Memorization":              ["remember","information","word","number","picture","procedure"],
  "Number Facility":           ["add","subtract","multiply","divide"],
  "Peripheral Vision":         ["object","movement","side","eye","looking","ahead"],
  "Glare Sensitivity":         ["object","glare","bright","lighting"],
  "Sound Localization":        ["direction","sound","originated"],
  "Night Vision":              ["see","low","light","condition"],
}

LIGHT_STOP = {
  "keep","make","use","do","get","take","give","go","come","look","move","hold",
  "set","put","find","see","tell","say","leave","run","work","stand","sit","lie",
  "lay","bring","send","know","apply","choose","regain","arrange","produce",
  "develop","combine","generate","change","shift","focus","read","identify",
  "detect","judge",
  "general","specific","different","certain","particular","single","simple",
  "short","long","small","large","common","various","other","wrong","likely",
  "exact","maximum","close","far",
  "the","of","and","or","to","be","have","that","this","one","two","up","off",
  "on","out","over","ability","thing","way","time","part","kind","amount",
  "state","object","item","matter","stuff","material","piece","point","level",
}

CACHE_PATH = "output/cache/datamuse_synonyms.json"
OUT_TSV    = "output/text/dictionaries/onet_ability_terms_dm.tsv"
MAX_RES    = 8

def fetch(word):
  url = f"https://api.datamuse.com/words?ml={word}&max={MAX_RES}"
  try:
    r = requests.get(url, timeout=10)
    if r.status_code == 200:
      return [d.get('word','').lower() for d in r.json() if 'word' in d]
  except Exception as e:
    print(f"  fetch error for {word}: {e}", file=sys.stderr)
  return []

# Load cache if exists
cache = {}
if os.path.exists(CACHE_PATH):
  with open(CACHE_PATH, "r", encoding="utf-8") as f:
    cache = json.load(f)
  print(f"Cache loaded: {len(cache)} terms")

# Single-word terms only
all_terms = sorted({t for v in ABILITIES.values() for t in v if " " not in t})
to_fetch = [t for t in all_terms if t not in cache]
print(f"Terms total: {len(all_terms)}, to fetch: {len(to_fetch)}")

# Parallel fetch
if to_fetch:
  t0 = time.time()
  with ThreadPoolExecutor(max_workers=20) as ex:
    futs = {ex.submit(fetch, t): t for t in to_fetch}
    for fut in as_completed(futs):
      term = futs[fut]
      cache[term] = fut.result()
  print(f"Fetched in {time.time()-t0:.1f}s")
  Path(CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
  with open(CACHE_PATH, "w", encoding="utf-8") as f:
    json.dump(cache, f, indent=1)
  print(f"Cached: {CACHE_PATH}")

# Build expanded TSV
rows = []
for ability, seeds in ABILITIES.items():
  seen = set()
  for t in seeds:
    if t not in seen:
      seen.add(t)
      rows.append((ability, t, "onet"))
  for t in seeds:
    if " " in t or "-" in t:
      continue
    for syn in cache.get(t, []):
      syn = syn.lower().strip()
      if not syn or " " in syn or not syn.isalpha() or len(syn) < 3:
        continue
      if syn in LIGHT_STOP or syn in seen:
        continue
      seen.add(syn)
      rows.append((ability, syn, "datamuse"))

Path(OUT_TSV).parent.mkdir(parents=True, exist_ok=True)
with open(OUT_TSV, "w", newline="", encoding="utf-8") as f:
  w = csv.writer(f, delimiter="\t")
  w.writerow(["ability","term","source"])
  for r in rows:
    w.writerow(r)

# Summary
from collections import Counter
src = Counter(r[2] for r in rows)
print(f"Output: {OUT_TSV}")
print(f"Total rows: {len(rows)} | onet: {src['onet']} | datamuse: {src['datamuse']}")
ab_counts = Counter(r[0] for r in rows)
print("Top expansions:")
for a, n in ab_counts.most_common(5):
  print(f"  {a}: {n} terms")
print("Sample expansions for 'lift':")
print("  ", cache.get('lift', [])[:6])
print("Sample expansions for 'exert':")
print("  ", cache.get('exert', [])[:6])
