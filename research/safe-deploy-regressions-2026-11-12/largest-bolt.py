#!/usr/bin/env python3
"""Find the largest rows from leaf metadata before reading selected values.

Uses the independently validated read-only Bolt page reader. Complete traversal
is bounded by a byte budget and page count; exceeding either fails explicitly.
"""
import argparse
import heapq
import json
import os
from pathlib import Path
import random
import struct
import subprocess
import sys
import time


def largest(snapshot, names, limit, capture, max_read_mb, max_value_mb):
    assert "/.zfs/snapshot/" in snapshot or os.environ.get("BOLT_SAMPLE_LOCAL_FIXTURE") == "1"
    started = time.monotonic()
    db = Bolt(snapshot, max_read_mb << 20, max_value_mb << 20)
    roots = {k: v for k, v, f in db.sample(db.page(db.root), 1024, 65536, random.Random(1)) if f == 1}
    summaries = []
    for name in names:
        encoded = name.encode()
        root, _ = db.bucket_page(roots[encoded])
        entries = db.sample(root, 1024, 65536, random.Random(2))
        children = {k: v for k, v, f in entries if f == 1}
        data, _ = db.bucket_page(children[b"data"])
        heap = []
        pages = 0
        rows = 0
        total = 0

        def walk(page, depth=0):
            nonlocal pages, rows, total
            pages += 1
            assert pages <= 100000 and depth < 20
            raw, base, size = page
            _, flags, count, _ = struct.unpack_from("<QHHI", raw)
            if flags == 1:
                _, entries = db.entries(page)
                for key, child in entries:
                    walk(db.page(child), depth+1)
                return
            assert flags == 2
            for i in range(count):
                off = 16+16*i
                flag, pos, ksize, vsize = struct.unpack_from("<IIII", raw, off)
                assert flag == 0 and off+pos+ksize+vsize <= size and ksize <= 65536
                rows += 1
                total += vsize
                if len(heap) == limit and vsize < heap[0][0]:
                    continue
                key = raw[off+pos:off+pos+ksize] if base is None else db.read(base+off+pos, ksize)
                value_off = -1 if base is None else base+off+pos+ksize
                inline = raw[off+pos+ksize:off+pos+ksize+vsize] if base is None else b""
                item = (vsize, key, value_off, inline)
                if len(heap) < limit:
                    heapq.heappush(heap, item)
                else:
                    heapq.heappushpop(heap, item)

        walk(data)
        selected = sorted(heap, reverse=True)
        summaries.append({"table": name, "rows": rows, "pages": pages, "total_value_bytes": total, "largest_bytes": [x[0] for x in selected]})
        if capture:
            def emit(path, key, value, kind):
                print(json.dumps({"path": [b64(x) for x in path], "key": b64(key), "value": b64(value), "kind": kind}, separators=(",", ":")))
            for key, value, flag in entries:
                if flag == 0:
                    emit([encoded], key, value, "metadata")
            for vsize, key, off, inline in selected:
                assert vsize <= max_value_mb << 20, "largest row exceeds value budget"
                value = db.read(off, vsize) if off >= 0 else inline
                emit([encoded, b"data"], key, value, "row")
        db.cache.clear()
    print(json.dumps({"txid": db.txid, "read_bytes": db.read_bytes, "seconds": round(time.monotonic()-started, 3), "tables": summaries}), file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--tables", required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--capture", action="store_true")
    ap.add_argument("--max-read-mb", type=int, default=64)
    ap.add_argument("--max-value-mb", type=int, default=512)
    args = ap.parse_args()
    assert "/fire-edb-regressions-" in str(args.output.resolve()) and args.limit > 0
    sampler = Path(__file__).with_name("sample-bolt.py").read_text().split('if __name__ == "__main__":')[0]
    helpers = Path(__file__).read_text().split("def main():")[0]
    script = sampler + "\n" + helpers + "\nlargest(" + ",".join(map(repr, [args.snapshot, args.tables.split(","), args.limit, args.capture, args.max_read_mb, args.max_value_mb])) + ")\n"
    command = [sys.executable, "-"] if args.host == "local" else ["ssh", "-o", "BatchMode=yes", args.host, "sudo -n python3 -"]
    with args.output.open("x") as out, args.output.with_suffix(".manifest.json").open("x") as err:
        subprocess.run(command, input=script, text=True, stdout=out, stderr=err, check=True)


if __name__ == "__main__":
    main()
