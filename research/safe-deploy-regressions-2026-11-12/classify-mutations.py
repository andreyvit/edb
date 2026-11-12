#!/usr/bin/env python3
"""Summarize every completed differential seed; fail on unclassified outcomes.

This classifies the observed audit, not arbitrary future semantics. Scalar
differences must still be reviewed against enum-forward.go's valid-value matrix.
Error streams and individual hashes remain private. A table-level index
difference is attributed only when that same seed contains an old-only failed
write to that table; this is recorded evidence, not blanket error suppression.
"""
from collections import Counter
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
assert root.name.startswith("fire-edb-regressions-")
counts = Counter()
old_errors = Counter()
scalar_fields = Counter()
index_tables = Counter()
unclassified = []
progress = (root / "mutation-runs" / "progress.jsonl").read_text()
completed = {json.loads(line)["seed"] for line in progress.splitlines(keepends=True) if line.endswith("\n")}
files = sorted(path for path in (root / "mutation-runs").glob("*-diffs.json") if int(path.name.split("-")[0]) in completed)
assert len(files) == len(completed), "missing completed-seed evidence"
for path in files:
    differences = json.loads(path.read_text())
    failed_tables = {
        d["key"][1] for d in differences
        if d["kind"] == "error_difference"
        and (d.get("old") or {}).get("error")
        and not (d.get("new") or {}).get("error")
    }
    for d in differences:
        phase, table, _ = d["key"]
        kind = d["kind"]
        a, b = d.get("old"), d.get("new")
        if kind == "missing":
            if a is None and table in {"plugins", "plugin-revisions", "mcp_connections"} and phase == "mutated-index" and b["count"] == 0:
                counts["new_empty_index"] += 1
                continue
        elif kind == "revision_only":
            if set(d["fields"]) <= {"mod_delta", "put_mod_delta"}:
                counts["revision_only"] += 1
                continue
        elif kind == "error_difference":
            if a.get("error") and not b.get("error"):
                counts["old_only_failed_write"] += 1
                old_errors[table] += 1
                continue
        elif kind == "semantic_difference":
            if phase == "mutated-index" and table in failed_tables:
                counts["index_after_old_only_failed_write"] += 1
                index_tables[table] += 1
                continue
            field = a.get("field", "")
            if (phase == "mutation" and table in {"importconfs", "TenantSettings", "assets"}
                    and field == b.get("field")
                    and any(name in field for name in ("SocialPlatform", "SocialMediaPlatforms"))):
                if set(d["fields"]) <= {"after", "in_tx"}:
                    counts["platform_encoding_difference"] += 1
                    scalar_fields[table] += 1
                    continue
        unclassified.append({"seed_file": path.name, "phase": phase, "table": table, "kind": kind, "fields": d.get("fields")})

result = {
    "complete_seeds": len(files), "differences": dict(counts),
    "old_only_failed_write_tables": dict(old_errors),
    "platform_difference_tables": dict(scalar_fields),
    "index_after_old_failure_tables": dict(index_tables),
    "unclassified_count": len(unclassified),
}
(root / "mutation-classification.json").write_text(json.dumps(result, indent=2) + "\n")
(root / "mutation-unclassified.json").write_text(json.dumps(unclassified, indent=2) + "\n")
print(json.dumps(result, indent=2))
if unclassified:
    raise SystemExit(1)
