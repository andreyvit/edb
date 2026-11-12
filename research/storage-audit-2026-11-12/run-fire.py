#!/usr/bin/env python3
"""Repeat synthetic Fire storage probes using a Go overlay; never open a live DB."""
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

here = Path(__file__).resolve().parent
fire = Path(sys.argv[1]).resolve()
base = sys.argv[2] if len(sys.argv) > 2 else "ac5e2c4807f112f73961bbceb9375c0778e0816e"
work = Path(tempfile.mkdtemp(prefix="fire-deep-storage-audit-"))
env = dict(os.environ, GOFLAGS=os.environ.get("GOFLAGS", "-p=4"))
print(f"Artifacts: {work}", flush=True)


def run(args, cwd=fire):
    return subprocess.check_output([str(a) for a in args], cwd=cwd, env=env, text=True)


module = json.loads(run(["go", "list", "-m", "-json", "github.com/andreyvit/edb"]))
if Path(module["Dir"]).resolve() != here.parents[1]:
    raise SystemExit("Fire must select this EDB checkout; inspect its local go.mod replacements")
imports = shutil.which("goimports") or str(Path(run(["go", "env", "GOPATH"]).strip()) / "bin/goimports")
if not Path(imports).is_file():
    raise SystemExit("This source-extraction harness requires the existing goimports developer tool")
for name in ["extract", "extract-migrations", "schema-manifest", "fields-manifest", "props"]:
    subprocess.run(["go", "build", "-o", str(work / name), str(here / "_tools" / f"{name}.go")], cwd=fire, env=env, check=True)
for name in ["reg-schema.go", "reg-mcp.go"]:
    exists = run(["git", "ls-tree", "--name-only", base, f"fdb/{name}"]).strip()
    (work / f"old-{name}").write_text(run(["git", "show", f"{base}:fdb/{name}"]) if exists else "package fdb\n")

index_source = run([work / "extract", work / "old-reg-schema.go"])
# Moved enum types only; callback expressions are extracted unchanged.
index_source = index_source.replace("bm.SendingState", "mevents.SendingState").replace("bm.CustomerSigninAttemptStatus", "mauth.CustomerSigninAttemptStatus")
names = re.findall(r'^"(\w+)": func', index_source, re.M)
index_source += "\nvar auditTables=map[string]*edb.TableDef{\n" + "".join(f'"{n}":{n}.TableDef(),\n' for n in names) + "}\n"
index_source += '''
type auditOldBuilder struct{keys map[string]map[string]bool}
func(b *auditOldBuilder)Add(h edb.AnyIndex,key any){i:=h.IndexDef();n:=i.FullName();if b.keys[n]==nil{b.keys[n]=map[string]bool{}};b.keys[n][string(i.EncodeKey(key))]=true}
'''
migration_source = run([work / "extract-migrations", work / "old-reg-schema.go"])
migration_source = migration_source.replace("orderPointChangesForUpgradeToV4", "auditOrderChanges")
migration_source = migration_source.replace("import(", 'import("iter"\n')
migration_source += '''
func auditOrderChanges(tx *edb.Tx,id bm.OrderID)iter.Seq[*bm.PointBalanceChange]{return func(yield func(*bm.PointBalanceChange)bool){for c:=edb.IndexScan[bm.PointBalanceChange](tx,PointBalanceChangesByOrder,edb.ExactScan(id));c.Next();{if !yield(c.Row()){return}}}}
'''
sources = {"oracle": index_source, "migration_oracle": migration_source, "properties": run([work / "props", fire / "nm"])}
overlay = {}
for suffix, source in sources.items():
    target = work / f"zz_storage_audit_{suffix}_test.go"
    target.write_text(source)
    subprocess.run([imports, "-w", str(target)], cwd=fire, env=env, check=True)
for source in (here / "_fire").glob("*.go"):
    shutil.copyfile(source, work / source.name)
for source in work.glob("zz_storage_audit_*_test.go"):
    dest = fire / "fdb" / source.name
    if dest.exists():
        raise SystemExit(f"Refusing to shadow existing file: {dest}")
    overlay[str(dest)] = str(source)
(work / "overlay.json").write_text(json.dumps({"Replace": overlay}))

# Declaration and field manifests are comparisons, not binary-format proofs.
for name, paths in [("old", [work / "old-reg-schema.go", work / "old-reg-mcp.go"]), ("new", [fire / "fdb/reg-schema.go", fire / "fdb/reg-mcp.go"])]:
    (work / f"schema-{name}.json").write_text(run([work / "schema-manifest", *paths]))
a, b = [json.loads((work / f"schema-{name}.json").read_text()) for name in ["old", "new"]]
for kind in a:
    changes = [k for k in a[kind] if a[kind][k] != b[kind].get(k)]
    print(f"{kind}: {len(a[kind])} baseline declarations, changed={changes}, added={sorted(b[kind].keys()-a[kind].keys())}", flush=True)

old = work / "old-fields"
old.mkdir()
paths = ["bm", "bpub", "internal", "firejobs", "bubbleflake", "tristate"]
archive = subprocess.check_output(["git", "archive", base, *paths], cwd=fire)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    for member in tar:
        if member.isfile() and member.name.endswith(".go"):
            p = old / member.name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(tar.extractfile(member).read())
for name, path in [("old", old), ("new", fire)]:
    (work / f"field-tags-{name}.json").write_text(run([work / "fields-manifest", path]))
a, b = [json.loads((work / f"field-tags-{name}.json").read_text()) for name in ["old", "new"]]
print(f"Fields: {len(a)} baseline, missing={sorted(a.keys()-b.keys())}, tag changes={[k for k in a.keys()&b.keys() if a[k]['Tag'] != b[k]['Tag']]}", flush=True)

for name, command in [
    ("matrix", ["go", "test", "-overlay", work / "overlay.json", "./fdb", "-run", "^TestDeep(Storage|Migration|RefundJoin|Collection)Audit$", "-count=1", "-v"]),
    ("fields", ["go", "run", here / "fields-boundary.go"]),
    ("clear", ["go", "run", here / "clear.go"]),
]:
    with (work / f"{name}.log").open("w") as out:
        result = subprocess.run([str(x) for x in command], cwd=fire, env=env, stdout=out, stderr=subprocess.STDOUT)
    print(f"{name}: exit={result.returncode}; {work / (name + '.log')}", flush=True)
print("Inspect FAIL/rejected diagnostics: this is an adversarial probe, not a green release gate.")
