#!/usr/bin/env python3
"""Rerun the recorded case inventory with alternating old/new test binaries.

Build /tmp/shared-{old,new}-{record,evm}.test first, as documented in README.md.
Each pair uses the same case, duration and machine; order alternates each round.
"""

from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent
inventory = []
for group, suffix in (("record", ""), ("evm", "-workloads")):
    names = list(dict.fromkeys(re.findall(r"^Benchmark(\S+)-\d+\s", (ROOT / f"old{suffix}.txt").read_text(), re.M)))
    inventory.extend((group, suffix, name) for name in names)

outputs = {(version, suffix): (ROOT / f"{version}{suffix}.txt").open("w")
           for version in ("old", "new") for suffix in ("", "-workloads")}
try:
    for i, (group, suffix, name) in enumerate(inventory):
        pattern = "/".join("^" + re.escape(part) + "$" for part in ("Benchmark" + name).split("/"))
        for repeat in range(6):
            order = ("old", "new") if repeat % 2 == 0 else ("new", "old")
            for version in order:
                result = subprocess.run([
                    f"/tmp/shared-{version}-{group}.test", "-test.run=^$",
                    "-test.bench=" + pattern, "-test.benchmem", "-test.benchtime=100ms",
                    "-test.count=1",
                ], capture_output=True, text=True, check=True)
                if not re.search(r"^Benchmark" + re.escape(name) + r"-\d+\s", result.stdout, re.M):
                    raise ValueError(f"no result for {name}: {result.stdout}")
                output = outputs[version, suffix]
                output.write(result.stdout)
                output.flush()
        if i % 10 == 0 or i == len(inventory)-1:
            print(f"Completed {i+1}/{len(inventory)} paired cases: {name}", flush=True)
finally:
    for output in outputs.values():
        output.close()
