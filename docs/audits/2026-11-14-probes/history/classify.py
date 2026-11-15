import re, subprocess, sys, collections
WT = sys.argv[1]
def git(*a):
    return subprocess.run(["git", "-C", WT, *a], capture_output=True, check=True).stdout.decode("utf-8", "replace")
FEATURE = set(range(1, 17)) | {22, 23, 56}
REDESIGN = {40, 54, 71, 72, 73, 74, 75, 76, 77, 78, 79}
DOCSPEC = {52, 53}
MANUAL = {
 # hash: category
 "1545113c": "T", "b1a4df2f": "C", "53b68203": "C", "a8e91fac": "C", "eb47fbcc": "C", "83550f01": "C",
 "af5a4b2a": "T", "2ff9de9e": "C", "9f4d4dfd": "C", "b2528383": "C", "624d817f": "C", "31dda162": "C",
 "95945c0f": "C", "1faac952": "C", "979a29ca": "D", "9add5c5b": "D", "2bfdcede": "D", "fda134a4": "D",
 "22c967a2": "D", "5ec584b2": "D", "11304617": "D", "a0f44b5f": "B", "cdcb2e19": "B", "2fc408fc": "B",
 "68caacf1": "B", "9bedc379": "D", "a85b6f08": "D", "ec0986f5": "D", "d2153aeb": "D", "f447f513": "D",
 "3a4d337d": "D", "eb800994": "D", "7018d5c5": "D", "561a9f66": "D", "0fc94b92": "D", "690d2a55": "D",
 "86f8fcc9": "T", "f17edd4a": "C", "1581cf6f": "B", "58b04297": "C", "37b9779e": "B", "1b98d149": "D",
 "e227b3e7": "F", "6dd1702b": "D", "ad898fc8": "D", "bd0cd9a2": "D", "866dfa5e": "D", "d9524100": "F",
 "20b01fe2": "D", "3c2bb2c1": "B", "b04a9258": "B", "0673ac9c": "C", "6f746630": "F", "10402204": "B", "1ba7cb25": "B", "02ce3de9": "B",
 "e565e221": "B", "76db0f17": "T", "e86e6bb8": "T", "e71f1973": "T", "0c88a717": "T", "aa861f31": "B",
 "16b827eb": "B",
}
rows = []
for line in git("log", "--reverse", "--format=%h%x09%ad%x09%s", "--date=short").splitlines():
    h, d, s = line.split("\t", 2)
    m = re.search(r"SPEC-(\d{3})", s)
    cat = None
    if h in MANUAL:
        cat = MANUAL[h]
    elif re.match(r"(build|ci)(\(|:)|chore\(deps", s):
        cat = "B"
    elif re.match(r"(test|tests)(\(|:)|fix\(tests\)|test\+docs", s):
        cat = "T"
    elif re.match(r"(docs|spec)(\(|:)|docs-lint", s) or "(Draft" in s or "Draft:" in s or "completion ritual" in s:
        cat = "D"
    elif m:
        n = int(m.group(1))
        if "restate FR-001" in s: cat = "D"
        elif n in FEATURE: cat = "F"
        elif n in REDESIGN: cat = "R"
        elif n in DOCSPEC: cat = "D"
        else: cat = "C"
    elif re.match(r"fix(\(|:)", s):
        cat = "C"
    rows.append((h, d, cat, s))
unc = [r for r in rows if r[2] is None]
for r in unc: print("UNCLASSIFIED", *r)
tags = {t: git("rev-list", "-n1", t).strip()[:8] for t in ("v1.0.0", "v1.1.0", "v1.2.0")}
order = [r[0] for r in rows]
idx = {h: i for i, h in enumerate(order)}
def window(h):
    i = idx[h]
    if i <= idx[tags["v1.0.0"]]: return "..v1.0.0"
    if i <= idx[tags["v1.1.0"]]: return "v1.0.0..v1.1.0"
    return "v1.1.0..v1.2.0"
tab = collections.defaultdict(collections.Counter)
for h, d, c, s in rows: tab[window(h)][c] += 1
for w in ("..v1.0.0", "v1.0.0..v1.1.0", "v1.1.0..v1.2.0"):
    print(w, dict(tab[w]), "total", sum(tab[w].values()))
# src absorption for C commits
for w in ("..v1.0.0", "v1.0.0..v1.1.0", "v1.1.0..v1.2.0", "ALL"):
    cnt = collections.Counter(); churn = collections.Counter()
    for h, d, c, s in rows:
        if c != "C" or (w != "ALL" and window(h) != w): continue
        for l in git("show", "--numstat", "--format=", h).splitlines():
            p = l.split("\t")
            if len(p) == 3 and p[2].startswith("src/") and p[0] != "-":
                cnt[p[2]] += 1; churn[p[2]] += int(p[0]) + int(p[1])
    print("==", w, "C commits touching file (top 8):")
    for f, n in cnt.most_common(8): print("   ", n, churn[f], f)
    print("   by churn:", [(f.split('/')[-1], n) for f, n in churn.most_common(5)])
with open(sys.argv[2], "w") as fh:
    for r in rows: fh.write("\t".join((r[0], r[1], r[2] or "?", window(r[0]), r[3])) + "\n")
