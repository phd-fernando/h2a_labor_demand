"""
Aggregate verb/noun pairs to WordNet concept ancestors (Webb level 3).

For each noun, climb the hypernym (IS-A) tree to the synset whose distance
from the root ('entity') equals the requested level. Same for verbs, with
the caveat that verb WordNet is shallower and noisier.

Adds columns:   verb_concept, noun_concept
Output rows:    one row per (doc_id, verb, noun, verb_concept, noun_concept).

Usage:
    python _wordnet_aggregate.py <input_pairs_tsv> <output_tsv>
        [--noun-level 3] [--verb-level 1]
        [--first-sense-only]   (default true; skip word-sense disambiguation)
"""

import argparse, csv, sys
from functools import lru_cache
from nltk.corpus import wordnet as wn

@lru_cache(maxsize=None)
def concept_for(lemma, pos, level):
    """Return the lemma of the Nth WordNet ancestor counted FROM THE LEAF
       (Webb 2020 convention). Level 0 = the lemma itself. Level 1 = parent.
       Level 3 = grandgrand-parent. Falls back to lemma if no synset."""
    syns = wn.synsets(lemma, pos=pos)
    if not syns:
        return lemma
    s = syns[0]                         # first sense (most common)
    paths = s.hypernym_paths()
    if not paths:
        return s.lemmas()[0].name().lower()
    # pick the longest path so we don't bottom out too quickly
    path = max(paths, key=len)
    # path is root -> ... -> leaf; index from leaf is len-1-level (clamped to 0)
    idx = max(len(path) - 1 - level, 0)
    return path[idx].lemmas()[0].name().lower()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument("--noun-level", type=int, default=3)
    ap.add_argument("--verb-level", type=int, default=1)
    ap.add_argument("--no-verb-aggregate", action="store_true",
                    help="Leave verbs as raw lemma; only aggregate nouns")
    args = ap.parse_args()

    n_in = n_out = 0
    with open(args.input,  encoding="utf-8", newline="") as fi, \
         open(args.output, "w", encoding="utf-8", newline="") as fo:
        r = csv.DictReader(fi, delimiter="\t", quoting=csv.QUOTE_NONE)
        w = csv.writer(fo, delimiter="\t", quoting=csv.QUOTE_NONE,
                       escapechar="\\")
        w.writerow(["doc_id", "verb", "noun", "verb_concept", "noun_concept"])
        for row in r:
            n_in += 1
            v = row["verb"];  n = row["noun"]
            vc = v if args.no_verb_aggregate else concept_for(v, "v", args.verb_level)
            nc = concept_for(n, "n", args.noun_level)
            w.writerow([row["doc_id"], v, n, vc, nc])
            n_out += 1
    sys.stderr.write(f"Rows in: {n_in}  Rows out: {n_out}\n")
    sys.stderr.write(f"Cache: {concept_for.cache_info()}\n")

if __name__ == "__main__":
    main()
