"""
Extract verb -> direct-object pairs (Webb 2020 method) using spaCy with
multi-process pipeline. Called from R as a system command.

Usage:
    python _extract_pairs.py <input_tsv> <output_tsv>
        [--text-col COL] [--id-col COL]
        [--prepend "The worker will "] [--n-process 24]
        [--batch-size 500]

Input TSV must have a header. Output is always tab-separated:
    doc_id  verb  noun
"""

import argparse, csv, sys, os
import spacy

NON_IMPERATIVE_STARTS = {
    "the","a","an","this","that","these","those",
    "workers","worker","employees","employee","hands","hand",
    "pickers","picker","crews","crew","supervisor","supervisors",
    "he","she","it","they","we","i","you","one",
    "after","before","during","while","when","if","although","because",
    "since","once","unless","whenever","whereas",
    "job","task","work","crop","stake","fruit","tree","field","row",
    "all","some","many","each","every","most","few","both","several",
    "due","also","additionally","furthermore","however","therefore",
    "his","her","its","their","our","my","your"
}
STOP_VERBS = {
    "use","have","be","do","make","provide","include","comprise",
    "perform","operate","assist","comply","get","follow"
}
# Preps treated as "Webb-equivalent" object markers. Beyond-Webb extension.
WEBB_PREPS = {"with", "to", "into", "onto", "from"}

def first_word(s):
    s = s.strip()
    i = 0
    while i < len(s) and not s[i].isalpha():
        i += 1
    j = i
    while j < len(s) and (s[j].isalpha() or s[j] == "'"):
        j += 1
    return s[i:j].lower()

def needs_prefix(s):
    fw = first_word(s)
    return bool(fw) and fw not in NON_IMPERATIVE_STARTS

def extract_pairs_from_doc(doc):
    """Return list of (verb_lemma, noun_lemma)."""
    pairs = []
    # Step A: direct verb -> dobj  +  passive subject  +  prep object (Webb prep set)
    base = []  # list of (verb_token, noun_token)
    for tok in doc:
        if tok.pos_ != "NOUN":
            continue
        # A1: direct object
        if tok.dep_ == "dobj" and tok.head.pos_ == "VERB":
            base.append((tok.head, tok))
        # A2: passive subject acts as semantic object
        elif tok.dep_ == "nsubjpass" and tok.head.pos_ == "VERB":
            base.append((tok.head, tok))
        # A3: prep object whose preposition is in WEBB_PREPS, governed by a VERB
        elif tok.dep_ == "pobj":
            prep = tok.head
            if prep.dep_ == "prep" and prep.text.lower() in WEBB_PREPS \
                    and prep.head.pos_ == "VERB":
                base.append((prep.head, tok))

    # Build a quick lookup: verb_token -> list of noun_tokens
    verb2nouns = {}
    for v, n in base:
        verb2nouns.setdefault(v.i, []).append(n)
    noun2verb = {n.i: v for v, n in base}

    # Step B: conjoined verbs share the object (both directions)
    # forward: head's obj -> conj
    # back:    conj's obj -> head
    extra_v = []
    for tok in doc:
        if tok.pos_ == "VERB" and tok.dep_ == "conj" and tok.head.pos_ == "VERB":
            head_v = tok.head
            if head_v.i in verb2nouns:
                for n in verb2nouns[head_v.i]:
                    extra_v.append((tok, n))
            if tok.i in verb2nouns:
                for n in verb2nouns[tok.i]:
                    extra_v.append((head_v, n))

    # Step C: conjoined nouns share the verb
    extra_n = []
    all_pairs = base + extra_v
    for tok in doc:
        if tok.pos_ == "NOUN" and tok.dep_ == "conj" and tok.head.pos_ == "NOUN":
            head_n = tok.head
            for v, n in all_pairs:
                if n.i == head_n.i:
                    extra_n.append((v, tok))

    # Combine + de-duplicate + filter
    seen = set()
    out = []
    for v, n in base + extra_v + extra_n:
        verb = v.lemma_.lower()
        noun = n.lemma_.lower()
        if verb in STOP_VERBS:
            continue
        key = (verb, noun)
        if key in seen:
            continue
        seen.add(key)
        out.append((verb, noun))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("output")
    ap.add_argument("--text-col", default="sentence")
    ap.add_argument("--id-col",   default="sent_uid")
    ap.add_argument("--prepend",  default="")
    ap.add_argument("--n-process", type=int, default=1)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    nlp = spacy.load("en_core_web_sm", disable=["ner"])

    # Read input
    rows = []
    with open(args.input, encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for i, row in enumerate(r):
            if args.limit and i >= args.limit:
                break
            rows.append(row)
    sys.stderr.write(f"Rows in: {len(rows)}\n")

    # Build text iterator with optional prepend
    def items():
        for row in rows:
            txt = row[args.text_col]
            if args.prepend and needs_prefix(txt):
                txt = args.prepend + txt
            yield (txt, row[args.id_col])

    # Process in parallel
    out = open(args.output, "w", encoding="utf-8", newline="")
    w = csv.writer(out, delimiter="\t", quoting=csv.QUOTE_NONE,
                   escapechar="\\")
    w.writerow(["doc_id", "verb", "noun"])

    n_pairs = 0
    n_done  = 0
    for doc, doc_id in nlp.pipe(items(), as_tuples=True,
                                 n_process=args.n_process,
                                 batch_size=args.batch_size):
        for verb, noun in extract_pairs_from_doc(doc):
            w.writerow([doc_id, verb, noun])
            n_pairs += 1
        n_done += 1
        if n_done % 50000 == 0:
            sys.stderr.write(f"  {n_done:>9d} / {len(rows)} processed, "
                             f"{n_pairs} pairs\n")
    out.close()
    sys.stderr.write(f"Done. Rows={n_done}  pairs={n_pairs}\n")

if __name__ == "__main__":
    main()
