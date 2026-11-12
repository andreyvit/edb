"""Run checked binaries one at a time. Rotate the first engine and reverse the order."""
import argparse
import os
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
BENCH = HERE.parents[1]
DATA = BENCH / "internal/workload/testdata"
NATIVE_COUNTS = {"Price": 6000, "Mandelbrot": 900, "Fib": 6000, "Sieve": 2000, "Scan": 40000}
ROW = re.compile(r"^Benchmark(\S+)\s+\d+\s+([\d.]+) ns/op(?:[ \t]+[\d.]+ [^ \t\n]+)*?[ \t]+(\d+) B/op\s+(\d+) allocs/op$", re.M)


def sample(tmp, name, workload, benchtime):
    if name in ("lua", "jit", "jit-off"):
        binary = tmp / ("lua" if name == "lua" else "jit")
        count = NATIVE_COUNTS[workload]
        env = dict(os.environ, LUAJIT_MODE="off" if name == "jit-off" else "on")
        out = subprocess.check_output([binary, DATA / "guest.lua", DATA / "expected.txt", workload, str(count)], env=env, text=True)
        label, n, ns, unit = out.split()
        assert label == workload and int(n) == count and unit == "ns/op"
        return ns, "-", "-"
    runner = {"gopher": "Gopher", "lunar": "Lunar", "yaegi": "Yaegi"}.get(name, "EVM")
    path = (runner + "/" if workload in NATIVE_COUNTS else "") + workload
    pattern = "/".join("^" + re.escape(w) + "$" for w in path.split("/"))
    binary = tmp / name / "bench"
    out = subprocess.check_output([binary, "-test.run=^$", "-test.cpu=1", "-test.benchmem",
                                   "-test.benchtime=" + benchtime, "-test.bench=" + "^Benchmark" + pattern[1:]], cwd=BENCH, text=True)
    rows = ROW.findall(out)
    if len(rows) != 1 or rows[0][0] != path:
        raise ValueError(out)
    return rows[0][1:]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("out", type=Path)
    p.add_argument("variants", help="comma-separated binary directories or interpreter names (gopher, lunar, yaegi, lua, jit, jit-off)")
    p.add_argument("workloads", help="comma-separated benchmark paths, e.g. Price,KVO/ReadCustomers")
    p.add_argument("--rounds", type=int, default=6)
    duration = p.add_mutually_exclusive_group()
    duration.add_argument("--ms", type=int, default=250)
    duration.add_argument("--iterations", type=int, help="fixed Go iteration count for matching database histories")
    a = p.parse_args()
    if a.iterations is not None and a.iterations < 1:
        p.error("--iterations must be positive")
    benchtime = f"{a.iterations}x" if a.iterations is not None else f"{a.ms}ms"
    names = a.variants.split(",")
    workloads = a.workloads.split(",")
    print("round\tvariant\tworkload\tns/op\tB/op\tallocs/op", flush=True)
    for i in range(a.rounds):
        # Each workload gets the same ordering every other round, reversed in
        # the next round. Rotate pairs to vary the middle positions as well.
        w = i % len(workloads)
        for work in workloads[w:] + workloads[:w]:
            start = (i // 2 + workloads.index(work)) % len(names)
            order = names[start:] + names[:start]
            for name in (order if i % 2 == 0 else reversed(order)):
                print(i + 1, name, work, *sample(a.out, name, work, benchtime), sep="\t", flush=True)


if __name__ == "__main__":
    main()
