#!/usr/bin/env python3
"""Read bounded samples from an immutable Bolt snapshot without opening a writer.

Stdout contains private bytes: redirect to a mode-0600 file. No remote files,
locks, writes, app startup or service calls. Layout follows bbolt internal/common.
"""
import argparse
import base64
import json
import hashlib
import os
import random
import struct
import sys
import time


def b64(v):
    return base64.b64encode(v).decode("ascii")


class Bolt:
    def __init__(self, path, max_bytes, max_value_bytes=8*1024*1024):
        self.fd = os.open(path, os.O_RDONLY)
        self.size = os.fstat(self.fd).st_size
        self.read_bytes = 0
        self.max_bytes = max_bytes
        self.max_value_bytes = max_value_bytes
        self.cache = {}
        self.skipped_large_values = 0
        self.skipped_details = {}
        self.current_bucket = ""
        first = self.read(0, 4096)
        self.page_size = struct.unpack_from("<I", first, 24)[0]
        assert 1024 <= self.page_size <= 65536 and not self.page_size & (self.page_size-1)
        metas = []
        for pg in (0, 1):
            raw = self.read(pg*self.page_size, 80)
            magic, version, pagesize, flags, root, seq, free, high, txid, checksum = struct.unpack_from("<IIIIQQQQQQ", raw, 16)
            h = 14695981039346656037
            for v in raw[16:72]:
                h = ((h ^ v) * 1099511628211) & ((1 << 64)-1)
            if magic == 0xed0cdaed and version == 2 and pagesize == self.page_size and h == checksum:
                assert 1 < root < high and high*self.page_size <= self.size
                metas.append((txid, root, seq, high))
        assert metas, "no valid checksummed meta"
        self.txid, self.root, self.sequence, self.high = max(metas)

    def read(self, off, size):
        assert off >= 0 and size >= 0 and off+size <= self.size
        self.read_bytes += size
        assert self.read_bytes <= self.max_bytes, "read budget exceeded"
        result = os.pread(self.fd, size, off)
        assert len(result) == size
        return result

    def page(self, pgid):
        assert 1 < pgid < self.high
        if pgid not in self.cache:
            raw = self.read(pgid*self.page_size, 16)
            page_id, flags, count, overflow = struct.unpack_from("<QHHI", raw)
            assert page_id == pgid and flags in (1, 2)
            size = (overflow+1)*self.page_size
            assert 16+count*16 <= size
            raw += self.read(pgid*self.page_size+16, count*16)
            self.cache[pgid] = (raw, pgid*self.page_size, size)
        return self.cache[pgid]

    def entries(self, page, limit=None, rng=None):
        raw, base, size = page
        _, flags, count, _ = struct.unpack_from("<QHHI", raw)
        assert flags in (1, 2) and 16+16*count <= len(raw)
        indices = range(count)
        if flags == 2 and limit is not None and count > limit:
            keep = {0, count-1, count//2}
            keep.update(rng.sample(range(count), min(limit-3, count)))
            indices = sorted(keep)
        def span(off, n):
            assert 0 <= off <= size and off+n <= size
            return raw[off:off+n] if base is None else self.read(base+off,n)
        out = []
        for i in indices:
            off = 16+16*i
            if flags == 1:
                pos, ksize, pgid = struct.unpack_from("<IIQ", raw, off)
                out.append((span(off+pos,ksize), pgid))
            else:
                eflags, pos, ksize, vsize = struct.unpack_from("<IIII", raw, off)
                if vsize > self.max_value_bytes:
                    self.skipped_large_values += 1
                    digest = hashlib.sha256(span(off+pos,ksize)).hexdigest()
                    self.skipped_details[(self.current_bucket,digest)] = vsize
                    continue
                out.append((span(off+pos,ksize), span(off+pos+ksize,vsize), eflags))
        assert all(out[i][0] < out[i+1][0] for i in range(len(out)-1))
        return flags, out

    def bucket_page(self, descriptor):
        root, seq = struct.unpack_from("<QQ", descriptor)
        return self.page(root) if root else (descriptor[16:], None, len(descriptor)-16), seq

    def sample(self, raw, leaves, per_leaf, rng, depth=0):
        assert depth < 20
        flags, entries = self.entries(raw, per_leaf, rng)
        if flags == 2:
            return entries
        if not entries:
            return []
        n = min(leaves, len(entries))
        indices = sorted({round(i*(len(entries)-1)/max(1,n-1)) for i in range(n)})
        out = []
        for i in indices:
            out.extend(self.sample(self.page(entries[i][1]), max(1, leaves//n), per_leaf, rng, depth+1))
        return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--leaves", type=int, default=64)
    ap.add_argument("--kv-leaves", type=int, default=16)
    ap.add_argument("--rows-per-leaf", type=int, default=24)
    ap.add_argument("--max-read-mb", type=int, default=512)
    ap.add_argument("--max-value-mb", type=int, default=8)
    ap.add_argument("--manifest-only", action="store_true")
    ap.add_argument("--tables", default="")
    args = ap.parse_args()
    assert "/.zfs/snapshot/" in args.path or os.environ.get("BOLT_SAMPLE_LOCAL_FIXTURE") == "1", "immutable snapshot required"
    started = time.monotonic()
    db = Bolt(args.path, args.max_read_mb*1024*1024, args.max_value_mb*1024*1024)
    root = db.sample(db.page(db.root), 1024, 65536, random.Random(1))
    summary = {"txid": db.txid, "page_size": db.page_size, "file_size": db.size, "buckets": []}
    def emit(path, key, value, kind):
        if not args.manifest_only:
            print(json.dumps({"path": [b64(v) for v in path], "key": b64(key), "value": b64(value), "kind": kind}, separators=(",", ":")))
    for name, desc, flag in root:
        assert flag == 1
        if args.tables and name.decode() not in args.tables.split(","):
            continue
        db.current_bucket = name.decode()
        raw, seq = db.bucket_page(desc)
        first = db.sample(raw, 1, 65536, random.Random(2))
        children = {key: val for key,val,f in first if f == 1}
        if b"data" in children:
            # Normal table root contains only state and nested buckets.
            direct = db.sample(raw, 1024, 65536, random.Random(2))
            children = {key: val for key,val,f in direct if f == 1}
            for key, value, f in direct:
                if f == 0:
                    emit([name], key, value, "metadata")
            data, data_seq = db.bucket_page(children[b"data"])
            count = 0
            total = 0
            for key, value, f in db.sample(data, args.leaves, args.rows_per_leaf, random.Random(name)):
                assert f == 0
                emit([name, b"data"], key, value, "row")
                count += 1
                total += len(key)+len(value)
            summary["buckets"].append({"name": name.decode(), "kind": "table", "rows": count, "bytes": total, "indexes": len(children)-1})
        else:
            count = 0
            total = 0
            for key, value, f in db.sample(raw, args.kv_leaves, args.rows_per_leaf, random.Random(name)):
                if f == 0:
                    emit([name], key, value, "kv")
                    count += 1
                    total += len(key)+len(value)
            summary["buckets"].append({"name": name.decode(), "kind": "kv", "rows": count, "bytes": total})
        db.cache.clear()
    summary.update(read_bytes=db.read_bytes, skipped_large_values=db.skipped_large_values, elapsed_seconds=round(time.monotonic()-started,3))
    summary["skipped"] = [{"bucket":k[0],"key_digest":k[1],"size":v} for k,v in db.skipped_details.items()]
    print(json.dumps(summary, separators=(",", ":")), file=sys.stderr)


if __name__ == "__main__":
    main()
