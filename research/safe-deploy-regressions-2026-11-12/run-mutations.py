#!/usr/bin/env python3
"""Continuous independent mutation replay on private disposable databases.

Every seed starts from the same old-version-reindexed sample. Keep all hashes,
error streams and difference details private; they are audit evidence, not logs
for publication. The time limit bounds continuous useful work, without sleeps.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("directory", type=Path)
    ap.add_argument("--seconds", type=int, default=9600)
    ap.add_argument("--rows-per-table", type=int, default=256)
    ap.add_argument("--first-seed", type=int, default=3)
    args = ap.parse_args()
    root = args.directory.resolve()
    assert root.name.startswith("fire-edb-regressions-")
    dest = root / "mutation-runs"
    dest.mkdir(mode=0o700, exist_ok=True)
    started = time.monotonic()
    seed = args.first_seed
    kept = 0
    totals = Counter()

    def run(flavor, seed):
        stem = dest / f"{seed:06}-{flavor}"
        database = stem.with_suffix(".db")
        assert not database.exists()
        subprocess.run(["cp", "-c", str(root / "prepared-old.db"), str(database)], check=True)
        with stem.with_suffix(".jsonl").open("w") as out, stem.with_suffix(".stderr").open("w") as err:
            subprocess.run([str(root / f"native-{flavor}"), "mutate", str(database), str(seed), str(args.rows_per_table)], stdout=out, stderr=err, check=True)
        rows = {}
        for line in stem.with_suffix(".jsonl").open():
            r = json.loads(line)
            if "table" in r:
                key = (r["phase"], r["table"], r.get("key", r.get("index")))
                assert key not in rows
                rows[key] = r
        assert any(k[0] == "mutated-index" for k in rows), "incomplete replay"
        return rows, database

    with ThreadPoolExecutor(max_workers=2) as pool, (dest / "progress.jsonl").open("a") as progress:
        while seed == args.first_seed or time.monotonic()-started < args.seconds:
            futures = [pool.submit(run, flavor, seed) for flavor in ("old", "new")]
            (a, old_db), (b, new_db) = [f.result() for f in futures]
            counts = Counter()
            diffs = []
            for key in sorted(a.keys() | b.keys()):
                x, y = a.get(key), b.get(key)
                if x == y:
                    counts["equal"] += 1
                    if x.get("error"):
                        counts["equal_errors"] += 1
                    continue
                fields = sorted(f for f in (x or {}).keys() | (y or {}).keys() if (x or {}).get(f) != (y or {}).get(f))
                if x is None or y is None:
                    kind = "missing"
                elif set(fields) <= {"mod_delta", "put_mod_delta"}:
                    kind = "revision_only"
                elif x.get("error") or y.get("error"):
                    kind = "error_difference"
                else:
                    kind = "semantic_difference"
                counts[kind] += 1
                diffs.append({"key": key, "kind": kind, "fields": fields, "old": x, "new": y})
            (dest / f"{seed:06}-diffs.json").write_text(json.dumps(diffs, indent=2)+"\n")
            totals.update(counts)
            summary = {"seed": seed, "seconds": round(time.monotonic()-started, 2), "old_records": len(a), "new_records": len(b), "counts": dict(counts), "total": dict(totals)}
            progress.write(json.dumps(summary)+"\n")
            progress.flush()
            print(json.dumps(summary), flush=True)
            if counts["semantic_difference"] and kept < 20:
                kept += 1
            else:
                old_db.unlink()
                new_db.unlink()
            seed += 1


if __name__ == "__main__":
    main()
