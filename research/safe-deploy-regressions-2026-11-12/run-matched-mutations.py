#!/usr/bin/env python3
"""Replay full-size seeds after omitting only known unequal mutation rows.

Keep every input/output private. This control supplements the unfiltered soak;
it must never replace or erase failures from that soak.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
assert root.name.startswith("fire-edb-regressions-")
source = root / "mutation-runs"
dest = root / "mutation-matched-full"
dest.mkdir(mode=0o700, exist_ok=False)
progress = [json.loads(s) for s in (source / "progress.jsonl").read_text().splitlines(keepends=True) if s.endswith("\n")]
# Seed 3 was an earlier 16-row smoke pass. Do not replay it as a 256-row case.
full_count = max(p["old_records"] for p in progress)
candidates = []
original_counts = {}
for p in progress:
    if p["old_records"] != full_count:
        continue
    differences = json.loads((source / f'{p["seed"]:06}-diffs.json').read_text())
    if any(d["key"][0] == "mutated-index" and d["kind"] == "semantic_difference" for d in differences):
        candidates.append(p["seed"])
        original_counts[p["seed"]] = p
assert len(candidates) >= 8
seeds = [candidates[i * (len(candidates) - 1) // 7] for i in range(8)]
results = []
for seed in seeds:
    differences = json.loads((source / f"{seed:06}-diffs.json").read_text())
    skipped = {}
    for d in differences:
        phase, table, key = d["key"]
        if d["kind"] == "error_difference" or phase == "mutation" and d["kind"] == "semantic_difference":
            if d["kind"] == "error_difference":
                assert d["old"].get("error") and not d["new"].get("error")
            else:
                field = d["old"].get("field", "")
                assert "SocialPlatform" in field or "SocialMediaPlatforms" in field
            skipped[table + "/" + key] = True
    skip_path = dest / f"{seed:06}-skip.json"
    skip_path.write_text(json.dumps(skipped))
    env = os.environ.copy()
    env["AUDIT_SKIP_MUTATIONS"] = str(skip_path)

    def run(version):
        stem = dest / f"{seed:06}-{version}"
        database = stem.with_suffix(".db")
        subprocess.run(["cp", "-c", str(root / "prepared-old.db"), str(database)], check=True)
        with stem.with_suffix(".jsonl").open("w") as out, stem.with_suffix(".stderr").open("w") as err:
            subprocess.run([str(root / f"native-filtered-{version}"), "mutate", str(database), str(seed), "256"], env=env, stdout=out, stderr=err, check=True)
        rows = {}
        for line in stem.with_suffix(".jsonl").read_text().splitlines():
            row = json.loads(line)
            if "table" in row:
                key = (row["phase"], row["table"], row.get("key", row.get("index")))
                assert key not in rows
                rows[key] = row
        assert len(rows) == original_counts[seed][version + "_records"] - len(skipped)
        return rows

    with ThreadPoolExecutor(max_workers=2) as pool:
        a, b = pool.map(run, ["old", "new"])
    counts = Counter()
    unexpected = []
    for key in sorted(a.keys() | b.keys()):
        x, y = a.get(key), b.get(key)
        if x == y:
            counts["equal"] += 1
            continue
        fields = {f for f in (x or {}).keys() | (y or {}).keys() if (x or {}).get(f) != (y or {}).get(f)}
        if x is None and y["phase"] == "mutated-index" and y["count"] == 0 and y["table"] in {"plugins", "plugin-revisions", "mcp_connections"}:
            counts["new_empty_index"] += 1
        elif x and y and fields <= {"mod_delta", "put_mod_delta"}:
            counts["revision_only"] += 1
        else:
            unexpected.append({"key": key, "fields": sorted(fields), "old": x, "new": y})
    (dest / f"{seed:06}-unexpected.json").write_text(json.dumps(unexpected, indent=2))
    result = {"seed": seed, "skipped_rows": len(skipped), "old_records": len(a), "new_records": len(b), "counts": dict(counts), "unexpected": len(unexpected)}
    results.append(result)
    print(json.dumps(result), flush=True)
    assert not unexpected, "unexplained matched-row result"
    for version in ["old", "new"]:
        (dest / f"{seed:06}-{version}.db").unlink()
(dest / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
