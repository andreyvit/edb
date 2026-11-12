"""Count dispatches and transfers in a separate, untimed build of current EVM."""
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[4]
tmp = Path(sys.argv[1]).resolve()
tmp.mkdir(parents=True, exist_ok=True)
overlay = {}


def put(name, data):
    dest = tmp / Path(name).name
    dest.write_text(data)
    overlay[str(root / name)] = str(dest)


vm = (root / "evm/evm.go").read_text()
before = "ins := f.Code[cur.pc]"
assert vm.count(before) == 1
put("evm/evm.go", vm.replace(before, "ScratchMeter.Outer++\n\t\t" + before) + """
var ScratchMeter struct {Outer, Inner, Folded, Loads, Stores, Moves, Spills uint64}
""")
name = "evm/floatloop_run.go"
body = (root / name).read_text()
for mask, field in (("loadMask", "Loads"), ("writeMask", "Stores")):
    for bit in (1, 2):
        old = "if l." + mask + "&" + str(bit) + " != 0 {"
        assert body.count(old) == 1
        body = body.replace(old, old + "\nScratchMeter." + field + "++")
old = "switch s.form {"
assert body.count(old) == 1
body = body.replace(old, """ScratchMeter.Inner++
            if s.form>=floatKinds[floatSpill].base {ScratchMeter.Spills++}
            if s.form>=floatKinds[floatMove].base && s.form<floatKinds[floatFromInt].base {ScratchMeter.Moves++}
            switch s.form {""")
old = "spent += l.cost\n"
assert body.count(old) == 1
put(name, body.replace(old, """ScratchMeter.Folded++
        if l.rangeLoop {ScratchMeter.Folded += 3}
        """ + old))
put("evm/bench/scratch_counts_test.go", """package bench_test
import ("fmt"; "testing"; "github.com/andreyvit/edb/evm")
func TestScratchCount(t *testing.T) {
 for _,name:=range []string{"Price","Mandelbrot","Sieve","Fib","Scan"} {
  run:=newEVM(t,name)
  for seed:=range 4 {
   start:=evm.ScratchMeter
   if _,err:=run(seed);err!=nil {t.Fatal(err)}
   end:=evm.ScratchMeter
   fmt.Printf("%s\\t%d\\t%d\\t%d\\t%d\\t%d\\t%d\\t%d\\t%d\\n",name,seed,
    end.Outer-start.Outer,end.Inner-start.Inner,end.Folded-start.Folded,end.Loads-start.Loads,
    end.Stores-start.Stores,end.Moves-start.Moves,end.Spills-start.Spills)
  }
 }
}
""")
path = tmp / "overlay.json"
path.write_text(json.dumps({"Replace": overlay}, indent=2))
subprocess.run(["go", "test", "-overlay", path, "-run", "^TestScratchCount$", "-v", "./evm/bench"], cwd=root, check=True)
