#!/usr/bin/env python3
"""Run a disposable old/new/old Bolt compatibility rehearsal (never a live DB)."""
import io
import os
import re
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

repo = Path(__file__).resolve().parents[2]
baseline = sys.argv[1] if len(sys.argv) > 1 else "fdd1715930dd6b6815c90738b782fe5fc881c32d"
work = Path(tempfile.mkdtemp(prefix="edb-storage-compat-"))
print(f"Artifacts: {work}", flush=True)
old = work / "baseline"
old.mkdir()
archive = subprocess.check_output(["git", "archive", baseline], cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    tar.extractall(old, filter="data")
source = (Path(__file__).parent / "main.go").read_text()
env = dict(os.environ, GOWORK="off")

for name, edb in [("old", old), ("new", repo)]:
    module = work / name
    module.mkdir()
    code = source
    if name == "old":
        code = re.sub(r"(edb\.DefineTable\([^\n]+), (?:rowModel|emptyModel\[\w+\]\(\)), func", r"\1, func", code)
        code = code[:code.index("func emptyModel[")]
        code = code.replace("TableBuilder[uint64]", "TableBuilder[row, uint64]")
        code = code.replace("TableBuilder[key]", "TableBuilder[compositeRow, key]")
        code = code.replace("TableBuilder[string]", "TableBuilder[stringRow, string]")
        code = re.sub(r'(edb.AddIndex\[[^]]+\]\([^\n]+), nil, edb.IndexOptDeprecatedUnique\)', r'\1).Unique()', code)
        code = re.sub(r'(edb.AddIndex\[[^]]+\]\([^\n]+), nil\)', r'\1)', code)
        # Historical API lives only in the baseline build, never in current EDB.
        code = re.sub(r"// BEGIN ROW INDEXER.*?// END ROW INDEXER", """func configureRowIndexes(b *edb.TableBuilder[row, uint64]) {
            b.Indexer(func(r *row, ib *edb.IndexBuilder) { ib.Add(email, r.Email); ib.Add(group, r.Group) })
        }""", code, flags=re.S)
    if not (edb / "kvo/codec/fast1").is_dir():
        code = code.replace("b.Prop(s.MustPropByCode(1))", "b.Prop(1)").replace("b.Prop(s.MustPropByCode(2))", "b.Prop(2)")
        code = code.replace('\t"github.com/andreyvit/edb/kvo/codec/fast1"\n', "").replace("fast1.", "kvo.")
    (module / "main.go").write_text(code)
    (module / "go.mod").write_text(
        f"module compatibility\n\ngo 1.27.0\n\nrequire github.com/andreyvit/edb v0.0.0\nreplace github.com/andreyvit/edb => {edb}\n"
    )
    subprocess.run(["go", "mod", "tidy"], cwd=module, env=env, check=True)
    subprocess.run(["go", "build", "-o", str(work / f"{name}-binary"), "."], cwd=module, env=env, check=True)

database = work / "fixture.bolt"
for binary, mode in [("old", "seed"), ("new", "modify"), ("old", "verify"), ("new", "verify")]:
    print(f"{binary}: {mode}", flush=True)
    subprocess.run([str(work / f"{binary}-binary"), str(database), mode], check=True)
print(f"PASS baseline={baseline} candidate=" + subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip())
