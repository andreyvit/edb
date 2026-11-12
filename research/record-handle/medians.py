#!/usr/bin/env python3
"""Summarize the committed Go benchmark output, without third-party packages."""

import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

root = Path(__file__).parent
pattern = re.compile(
    r"(Benchmark\S+)-\d+\s+\d+\s+([\d.]+) ns/op.*?\s+(\d+) B/op\s+(\d+) allocs/op"
)
report = {}
for filename in ("baseline.txt", "untagged-layouts.txt", "tagged-layouts.txt", "scopes.txt"):
    text = (root / filename).read_text()
    if "\nPASS\n" not in text or "FAIL" in text:
        raise SystemExit(f"incomplete or failed benchmark: {filename}")
    samples = defaultdict(list)
    for line in text.splitlines():
        if match := pattern.match(line):
            samples[match[1]].append(tuple(map(float, match.groups()[1:])))
    report[filename] = {
        name: {
            "samples": len(rows),
            **{
                metric: statistics.median(row[i] for row in rows)
                for i, metric in enumerate(("ns/op", "B/op", "allocs/op"))
            },
        }
        for name, rows in sorted(samples.items())
    }
    if any(len(rows) != 5 for rows in samples.values()):
        raise SystemExit(f"expected five samples per case: {filename}")
(root / "medians.json").write_text(json.dumps(report, indent=2) + "\n")
for filename, cases in report.items():
    print(f"{filename}: {len(cases)} cases, five samples each")
