"""Run the checked interpreter workloads in rotating order. Output is TSV."""
import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile

here = Path(__file__).resolve().parent
bench = here.parent
data = bench / "internal/workload/testdata"
entries = {"Price": 6000, "Scan": 40000, "Fib": 8000, "Sieve": 2500, "Mandelbrot": 1200}
go_row = re.compile(r"^Benchmark(\w+)/(\w+)\s+\d+\s+([.\d]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op$", re.M)


def go_sample(binary, runtime):
    group = {"EVM": "(EVM|RawUnary)", "EVM before": "EVM"}.get(runtime, runtime)
    out = subprocess.check_output(
        [binary, "-test.run=^$", "-test.bench=^Benchmark" + group + "$", "-test.cpu=1",
         "-test.benchtime=200ms", "-test.benchmem"], cwd=bench, text=True)
    rows = go_row.findall(out)
    expected = {(runtime, name) for name in entries}
    if runtime == "EVM":
        expected.add(("EVM RawUnary", "Scan"))
    found = [("EVM RawUnary" if group == "RawUnary" else runtime, name, ns, size, count)
             for group, name, ns, size, count in rows]
    if {(rt, name) for rt, name, *_ in found} != expected or len(found) != len(expected):
        raise ValueError(out)
    return found


def native_sample(binary, runtime):
    for name, count in entries.items():
        out = subprocess.check_output(
            [binary, data / "guest.lua", data / "expected.txt", name, str(count)],
            env={**os.environ, "LUAJIT_MODE": "off" if runtime == "LuaJIT off" else "on"}, text=True)
        label, calls, ns, unit = out.split()
        if label != name or int(calls) != count or unit != "ns/op":
            raise ValueError(out)
        yield runtime, name, ns, "-", "-"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lua-prefix", type=Path, required=True)
    parser.add_argument("--luajit-prefix", type=Path, required=True)
    parser.add_argument("--baseline-bin", type=Path, help="EDB bench test binary with the same five workloads")
    parser.add_argument("--rounds", type=int)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for package, binary in [(bench, "evm"), (here / "yaegi", "yaegi"), (here / "golua", "golua")]:
            subprocess.run(["go", "test", "-c", "-o", tmp / binary], cwd=package, check=True)
        for binary, prefix in [("puc", args.lua_prefix), ("jit", args.luajit_prefix)]:
            subprocess.run([here / "native/run.sh", "--build", tmp / binary],
                           env={**os.environ, "LUA_PREFIX": str(prefix.resolve())}, check=True)
        engines = [("EVM", tmp / "evm", go_sample), ("Yaegi", tmp / "yaegi", go_sample),
                   ("Lunar", tmp / "golua", go_sample), ("Gopher", tmp / "golua", go_sample),
                   ("PUC Lua", tmp / "puc", native_sample), ("LuaJIT", tmp / "jit", native_sample),
                   ("LuaJIT off", tmp / "jit", native_sample)]
        if args.baseline_bin:
            engines.insert(0, ("EVM before", args.baseline_bin.resolve(), go_sample))
        rounds = len(engines) if args.rounds is None else args.rounds
        if rounds < 1:
            parser.error("--rounds must be positive")
        print("series\tround\truntime\tworkload\tns/op\tB/op\tallocs/op", flush=True)
        for i in range(rounds):
            start = i % len(engines)
            for runtime, binary, sample in engines[start:] + engines[:start]:
                for rt, name, ns, size, count in sample(binary, runtime):
                    print("kernels", i + 1, "GopherLua" if rt == "Gopher" else rt,
                          name, ns, size, count, sep="\t", flush=True)


if __name__ == "__main__":
    main()
