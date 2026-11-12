#!/usr/bin/env python3
"""Kill disposable writers around transaction boundaries and verify recovery."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("directory", type=Path)
    ap.add_argument("--cycles", type=int, default=256)
    ap.add_argument("--first-seed", type=int, default=0)
    args = ap.parse_args()
    root = args.directory.resolve()
    assert root.name.startswith("fire-edb-regressions-")
    dest = root / "crash-runs"
    dest.mkdir(mode=0o700, exist_ok=True)
    started = time.monotonic()
    for seed in range(args.first_seed, args.first_seed + args.cycles):
        for version in ("old", "new"):
            stem = dest / f"{seed:06}-{version}"
            db = stem.with_suffix(".db")
            assert not db.exists()
            subprocess.run(["cp", "-c", str(root / "crash-base.db"), str(db)], check=True)
            env = dict(os.environ, AUDIT_REUSE_TX_BUFFERS="true" if seed % 2 else "false")
            ack = 0
            with stem.with_suffix(".stderr").open("w") as err:
                writer = subprocess.Popen([str(root / f"crash-{version}"), "write", str(db)], stdout=subprocess.PIPE, stderr=err, text=True, env=env)
                target = 3 + seed % 17
                while True:
                    line = writer.stdout.readline()
                    assert line, "writer exited before kill boundary"
                    item = json.loads(line)
                    if item["phase"] == "ack":
                        ack = item["generation"]
                    if item["phase"] == "row" and item["generation"] == target and item["row"] == seed % 32:
                        time.sleep((seed % 7) * 0.00025)
                        writer.kill()
                        break
                for line in writer.stdout:
                    item = json.loads(line)
                    if item["phase"] == "ack":
                        ack = item["generation"]
                assert writer.wait() == -9
            check = subprocess.run([str(root / f"crash-{version}"), "check", str(db), str(ack)], capture_output=True, text=True, env=env)
            stem.with_suffix(".check.jsonl").write_text(check.stdout)
            stem.with_suffix(".check.stderr").write_text(check.stderr)
            assert check.returncode == 0, f"recovery failure, see private files for seed {seed}, version {version}"
            result = json.loads(check.stdout)
            result.update(seed=seed, version=version, reuse_tx_buffers=seed % 2 == 1, target_row=seed % 32, elapsed_seconds=round(time.monotonic()-started, 3))
            print(json.dumps(result), flush=True)
            db.unlink()


if __name__ == "__main__":
    main()
