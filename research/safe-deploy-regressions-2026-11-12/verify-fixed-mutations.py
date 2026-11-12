#!/usr/bin/env python3
"""Check only the intended nil rejection, then replay identical valid writes.

Run after run-mutations.py with native-old = audited candidate and native-new =
fixed candidate. Private files retain all original differences and exclusions.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
assert root.name.startswith("fire-edb-regressions-")
source = root / "mutation-runs"
dest = root / "matched-fixes"
dest.mkdir(mode=0o700, exist_ok=True)
progress = [json.loads(s) for s in (source / "progress.jsonl").read_text().splitlines()]
patterns = {
    "products": r"\.Variants\[\d+\]",
    "coupons": r"\.Coupons\[\d+\]",
    "changes_points": r"\.Coupons\[\d+\]",
    "ownerships": r"\.LogicalCoupons\[\d+\](?:\*\.Coupons\[\d+\])?",
    "ReferralContact": r"\.LogicalCoupons\[\d+\](?:\*\.Coupons\[\d+\])?",
}
totals = Counter()
rejected = Counter()


def load(path):
    rows = {}
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if "table" in r:
            key = (r["phase"], r["table"], r.get("key", r.get("index")))
            assert key not in rows
            rows[key] = r
    assert any(k[0] == "mutated-index" for k in rows)
    return rows


def run(version, seed, exclusions):
    stem = dest / f"{seed:06}-{version}"
    db = stem.with_suffix(".db")
    assert not db.exists()
    subprocess.run(["cp", "-c", str(root / "prepared-old.db"), str(db)], check=True)
    env = dict(os.environ, AUDIT_SKIP_MUTATIONS=str(exclusions))
    with stem.with_suffix(".jsonl").open("w") as out, stem.with_suffix(".stderr").open("w") as err:
        subprocess.run([str(root / f"native-{version}"), "mutate", str(db), str(seed), "256"], env=env, stdout=out, stderr=err, check=True)
    return load(stem.with_suffix(".jsonl")), db


with ThreadPoolExecutor(max_workers=2) as pool:
    for result in progress:
        seed = result["seed"]
        diffs = json.loads((source / f"{seed:06}-diffs.json").read_text())
        skip = {}
        for d in diffs:
            if d["kind"] != "error_difference":
                continue
            a, b = d["old"], d["new"]
            assert a and b and a["phase"] == "mutation"
            assert not a["error"] and b["error"] == "string"
            assert a["before"] == b["before"] and a["field"] == b["field"]
            assert re.fullmatch(patterns[a["table"]], a["field"])
            skip[a["table"] + "/" + a["key"]] = True
            rejected[a["table"]] += 1
        for d in diffs:
            if d["kind"] in {"revision_only", "error_difference"}:
                continue
            assert d["kind"] == "semantic_difference"
            phase, table, _ = d["key"]
            assert phase == "mutated-index" and any(k.startswith(table + "/") for k in skip)
        exclusions = dest / f"{seed:06}-excluded.json"
        exclusions.write_text(json.dumps(skip) + "\n")
        futures = [pool.submit(run, version, seed, exclusions) for version in ("old", "new")]
        (a, adb), (b, bdb) = [f.result() for f in futures]
        assert len(a) == result["old_records"] - len(skip), "control did not apply exclusions"
        assert len(b) == result["new_records"] - len(skip), "fixed probe did not apply exclusions"
        assert a.keys() == b.keys()
        for key in a:
            fields = {f for f in a[key].keys() | b[key].keys() if a[key].get(f) != b[key].get(f)}
            assert fields <= {"mod_delta", "put_mod_delta"}, (seed, key[0], key[1], sorted(fields))
            totals["revision_only" if fields else "equal"] += 1
            if key[0] == "mutated-index":
                totals["equal_index_buckets"] += 1
        adb.unlink()
        bdb.unlink()
        totals["seeds"] += 1
        print(json.dumps({"seed": seed, "excluded": len(skip), "totals": dict(totals)}), flush=True)
summary = {"totals": dict(totals), "rejected_nil_writes": dict(rejected)}
(dest / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary), flush=True)
