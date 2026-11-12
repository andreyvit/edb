#!/usr/bin/env python3
"""Summarize the committed old/new Go benchmark outputs (standard library only)."""

import json
from pathlib import Path
import re
from statistics import median

ROOT = Path(__file__).resolve().parent


def read_samples(names):
    result = {}
    for name in names:
        raw = (ROOT / name).read_text()
        if "\nPASS\n" not in raw or "FAIL" in raw:
            raise ValueError(f"{name}: benchmark run did not pass")
        for line in raw.splitlines():
            match = re.match(r"Benchmark(\S+)-\d+\s+", line)
            if not match:
                continue
            sample = []
            for unit in ("ns/op", "B/op", "allocs/op"):
                sample.append(float(re.search(r"([\d.]+) " + unit, line)[1]))
            result.setdefault(match[1], []).append(sample)
    return {
        key: {"samples": len(samples), "median": [median(v[i] for v in samples) for i in range(3)]}
        for key, samples in result.items()
    }


old = read_samples(["old.txt", "old-workloads.txt"])
new = read_samples(["new.txt", "new-workloads.txt"])
if old.keys() != new.keys():
    raise ValueError(f"unmatched benchmarks: {old.keys() ^ new.keys()}")
report = {}
for key, after in new.items():
    before = old[key]
    if before["samples"] != 6 or after["samples"] != 6:
        raise ValueError(f"{key}: expected six samples per version")
    a, b = before["median"], after["median"]
    report[key] = {"old": before, "new": after, "time_ratio": b[0] / a[0]}
    print(f"{key:52} {a[0]:10.2f} -> {b[0]:10.2f} ns  {b[0]/a[0]:5.2f}x  "
          f"{a[1]:.0f}/{a[2]:.0f} -> {b[1]:.0f}/{b[2]:.0f} B/allocs")
(ROOT / "medians.json").write_text(json.dumps(report, indent=2) + "\n")
