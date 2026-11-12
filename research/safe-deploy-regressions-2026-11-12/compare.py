#!/usr/bin/env python3
"""Compare independently executed native replays; print aggregate diagnostics only."""
from collections import Counter
import json
from pathlib import Path
import sys


def load(path):
    result = {}
    for line in Path(path).read_text().splitlines():
        row = json.loads(line)
        if "table" in row:
            key = (row["phase"], row["table"], row.get("key", row.get("index", "")))
            assert key not in result
            result[key] = row
    return result


a, b = map(load, sys.argv[1:3])
counts = Counter()
diffs = []
for key in sorted(a.keys() | b.keys()):
    if key not in a or key not in b:
        counts[(key[0], "old-only" if key in a else "new-only")] += 1
        diffs.append({"key": key, "kind": "missing", "old": a.get(key), "new": b.get(key)})
        continue
    x, y = a[key], b[key]
    fields = [f for f in x.keys() | y.keys() if x.get(f) != y.get(f)]
    meaningful = [f for f in fields if f not in ("raw",)]
    counts[(key[0], "same" if not fields else "raw-only" if not meaningful else "DIFFERENT")] += 1
    if meaningful:
        diffs.append({"key": key, "fields": fields, "old": x, "new": y})
    for name, row in (("old", x), ("new", y)):
        if row.get("error"):
            counts[(key[0], name+"-error")] += 1
if len(sys.argv) > 3:
    Path(sys.argv[3]).write_text(json.dumps(diffs, indent=2)+"\n")
for (phase, kind), count in sorted(counts.items()):
    print(phase, kind, count)
print("difference_tables", sorted({d["key"][1] for d in diffs}))
print("initial_tenant_count", len({r["tenant"] for r in a.values() if r["phase"] == "initial" and "tenant" in r}))
