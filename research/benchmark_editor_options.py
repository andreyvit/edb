#!/usr/bin/env python3
"""Compare EVM precomputed write flags with runtime defaults, without runtime hooks."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
METHOD = "TrySetValueWithModelDefaultsBenchmark"


def overlay(directory, mode):
    if mode == "explicit":
        return []
    replacements = {}
    if mode == "cached":
        resolve = """
    layout := e.view.layout
    if layout == nil && e.view.plan != nil { layout = e.view.plan.layout }
    for i := range path {
        field := layout.field(path[i].Key)
        path[i].Options = field.writeOptions
        layout = field.child
    }
"""
    else:
        resolve = """
    model := e.Model()
    for i := range path {
        path[i].Options = WriteOptions(model.PropFlags(path[i].Key))
        if i + 1 < len(path) { model = model.MustPropByCode(path[i].Key).TypeModel() }
    }
"""
    original = ROOT / "kvo/record.go"
    target = directory / (mode + "_record.go")
    target.write_text(original.read_text() + "\nfunc (e *Editor) " + METHOD +
                      "(path []row2.Step, x values.Value, w *row2.Writer) error {\n" +
                      resolve + "\nreturn e.TryFastSetPathValueWithWriter(path, x, w)\n}\n")
    replacements[str(original)] = str(target)
    for name in ["records.go", "record_values.go"]:
        original = ROOT / "evm" / name
        target = directory / (mode + "_" + name)
        source = original.read_text()
        assert ".TryFastSetPathValueWithWriter(" in source
        target.write_text(source.replace(".TryFastSetPathValueWithWriter(", "." + METHOD + "("))
        replacements[str(original)] = str(target)
    path = directory / (mode + ".json")
    path.write_text(json.dumps({"Replace": replacements}))
    return ["-overlay", str(path)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--benchtime", default="200ms")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    with tempfile.TemporaryDirectory(prefix="edb-options-") as tmp:
        configs = {mode: overlay(Path(tmp), mode) for mode in ["explicit", "cached", "schema"]}
        for mode, flags in configs.items():
            print("Validating " + mode, flush=True)
            with (args.output / (mode + "-tests.txt")).open("w") as log:
                subprocess.run(["go", "test", *flags, "./evm", "./evm/evmcompiler", "./evm/bench"],
                               cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
            (args.output / (mode + ".txt")).write_text("")
        for sample in range(args.samples):
            # Rotate modes to avoid consistently giving one mode a warmer machine.
            modes = list(configs)
            modes = modes[sample % len(modes):] + modes[:sample % len(modes)]
            for mode in modes:
                print(f"Sample {sample + 1}/{args.samples}: {mode}", flush=True)
                with (args.output / (mode + ".txt")).open("a") as log:
                    for pattern in ["^BenchmarkRecordWriteOptions$",
                                    "^BenchmarkAccountState$/(EVM|MessagePackEVM)$/WriteCustomers$/RecordIndexer$"]:
                        subprocess.run(["go", "test", *configs[mode], "./evm/bench", "-run", "^$",
                                        "-bench", pattern, "-benchmem", "-benchtime", args.benchtime, "-count=1"],
                                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    print("Results: " + str(args.output), flush=True)


if __name__ == "__main__":
    main()
