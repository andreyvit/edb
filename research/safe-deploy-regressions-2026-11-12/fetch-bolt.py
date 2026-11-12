#!/usr/bin/env python3
"""Fetch connected rows through immutable snapshot indexes, without a writer.

Local wrapper sends sampler + fetch code through SSH stdin. Query/output files
contain private identifiers and values. No files are created on the server.
"""
import argparse
import base64
from collections import Counter, deque
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time


def prefix_end(prefix):
    result = bytearray(prefix)
    for i in range(len(result)-1,-1,-1):
        if result[i] < 255:
            result[i] += 1
            return bytes(result[:i+1])
    return None


def selected_entries(db, page, lower, upper, depth=0):
    assert depth < 20
    flags, entries = db.entries(page)
    if flags == 2:
        for entry in entries:
            if entry[0] >= lower and (upper is None or entry[0] < upper):
                yield entry
    else:
        for i,(key,child) in enumerate(entries):
            next_key = entries[i+1][0] if i+1<len(entries) else None
            if (next_key is None or next_key > lower) and (upper is None or key < upper):
                yield from selected_entries(db,db.page(child),lower,upper,depth+1)


def exact(db, page, key):
    # Appending NUL is the immediate lexicographic successor for exact lookup.
    found = list(selected_entries(db,page,key,key+b"\0"))
    assert len(found) <= 1
    return found[0] if found else None


def tuple_parts(raw):
    def pop(buf):
        n=0;shift=0;i=len(buf)
        while i:
            i-=1;b=buf[i];n|=(b&127)<<shift
            if not b&128:return n,buf[:i]
            shift+=7;assert shift<=35
        raise AssertionError("invalid reverse varint")
    n,raw=pop(raw);assert 0<n<100
    lengths=[]
    for _ in range(n-1):
        size,raw=pop(raw);lengths.append(size)
    out=[]
    for size in reversed(lengths):
        assert size<=len(raw);out.append(raw[:size]);raw=raw[size:]
    return out+[raw]


def fetch(snapshot, requests, max_read_mb=1024):
    assert "/.zfs/snapshot/" in snapshot or os.environ.get("BOLT_SAMPLE_LOCAL_FIXTURE")=="1"
    started=time.monotonic()
    db=Bolt(snapshot,max_read_mb*1024*1024)
    roots={k:v for k,v,f in db.sample(db.page(db.root),1024,65536,random.Random(1)) if f==1}
    tables={};emitted=set();counts=Counter();missing=Counter();queries=Counter()
    def emit(path,key,value,kind):
        ident=tuple(path)+(key,)
        if ident in emitted:return
        emitted.add(ident);counts[(path[0].decode(),kind)]+=1
        print(json.dumps({"path":[b64(x) for x in path],"key":b64(key),"value":b64(value),"kind":kind},separators=(",",":")))
    def table(name):
        if name not in roots:return None
        if name not in tables:
            root,_=db.bucket_page(roots[name])
            entries=db.sample(root,1024,65536,random.Random(2))
            children={k:v for k,v,f in entries if f==1}
            if b"data" not in children:return None
            for k,v,f in entries:
                if f==0:emit([name],k,v,"metadata")
            tables[name]={k:db.bucket_page(v)[0] for k,v in children.items()}
        return tables[name]
    pending=deque(requests);seen_queries=set()
    while pending:
        request=pending.popleft();identity=json.dumps(request,sort_keys=True)
        if identity in seen_queries:continue
        seen_queries.add(identity)
        name=request["table"].encode();db.current_bucket=name.decode()
        buckets=table(name)
        if buckets is None:missing[(name.decode(),"table")]+=1;continue
        if "key" in request:
            key=base64.b64decode(request["key"]);row=exact(db,buckets[b"data"],key)
            queries[(name.decode(),"key")]+=1
            if row:emit([name,b"data"],row[0],row[1],"row")
            else:missing[(name.decode(),"row")]+=1
            continue
        idx=b"i_"+request["index"].encode()
        if idx not in buckets:missing[(name.decode(),idx.decode())]+=1;continue
        prefix=base64.b64decode(request["prefix"]);n=0
        queries[(name.decode(),idx.decode())]+=1
        for ik,iv,flag in selected_entries(db,buckets[idx],prefix,prefix_end(prefix)):
            assert flag==0
            n+=1;assert n<=request.get("limit",10000),"query row limit exceeded"
            parts=tuple_parts(ik)
            # These requests use nonunique indexes with one fixed-width ID key.
            assert len(parts)==2 and len(parts[0])==8 and parts[0]==prefix
            key=parts[-1];row=exact(db,buckets[b"data"],key)
            if not row:missing[(name.decode(),"dangling index")]+=1;continue
            emit([name,idx],ik,iv,"index")
            emit([name,b"data"],row[0],row[1],"row")
            if name==b"Orders":
                assert len(key)==9 and key[-1]==1
                pending.append({"table":"changes_points","index":"by_order","prefix":b64(key[:-1]),"limit":10000})
    assert db.skipped_large_values==0,"connected cohort skipped an oversized value"
    summary={"txid":db.txid,"read_bytes":db.read_bytes,"seconds":round(time.monotonic()-started,3),"counts":{f"{k[0]}:{k[1]}":v for k,v in counts.items()},"missing":{f"{k[0]}:{k[1]}":v for k,v in missing.items()},"queries":{f"{k[0]}:{k[1]}":v for k,v in queries.items()}}
    print(json.dumps(summary),file=sys.stderr)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--host",required=True)
    ap.add_argument("--snapshot",required=True)
    ap.add_argument("--queries",type=Path,required=True)
    ap.add_argument("--output",type=Path,required=True)
    args=ap.parse_args()
    assert "/fire-edb-regressions-" in str(args.output.resolve())
    requests=json.loads(args.queries.read_text())
    sampler=Path(__file__).with_name("sample-bolt.py").read_text().split('if __name__ == "__main__":')[0]
    helpers=Path(__file__).read_text().split("def main():")[0]
    script=sampler+"\n"+helpers+"\nfetch("+repr(args.snapshot)+","+repr(requests)+")\n"
    with args.output.open("x") as out,args.output.with_suffix(".manifest.json").open("x") as err:
        subprocess.run(["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10",args.host,"sudo -n python3 -"],input=script,text=True,stdout=out,stderr=err,check=True)


if __name__ == "__main__":
    main()
