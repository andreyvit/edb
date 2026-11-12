"""Build scratch experiments with go -overlay. The checkout stays buildable as usual."""
import argparse
import itertools
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = Path(os.environ.get("SCRATCH_EDB", HERE.parents[3])).resolve()
EVM = ROOT / "evm"


class Variant:
    def __init__(self, bank, count, form):
        self.bank = bank
        self.names = [bank + str(i) for i in range(count)]
        self.form = form
        self.ops = {}
        self.plans = {}

    def emit(self, name, body):
        self.ops[name] = body

    def generate(self):
        if not self.names:
            return
        fp = self.bank == "f"
        get = "math.Float64frombits(r[a])" if fp else "r[a]"
        def assign(d, expr):
            if d == "r" and fp:
                return "r[a] = math.Float64bits(" + expr + ")"
            return ("r[a]" if d == "r" else d) + " = " + expr

        for x in self.names:
            self.emit("load." + x + ".r", assign(x, get))
            self.emit("store.r." + x, assign("r", x))
            for y in self.names:
                if x != y:
                    self.emit("move." + x + "." + y, assign(x, y))
        ops = {"add": "{} + {}", "sub": "{} - {}", "mul": "{} * {}"}
        if fp:
            ops["div"] = "{} / {}"
        else:
            ops.update({"div.s": "uint64(int64({}) / int64({}))", "div.u": "{} / {}",
                        "lt.s": "boolWord(int64({}) < int64({}))", "lt.u": "boolWord({} < {})",
                        "le.s": "boolWord(int64({}) <= int64({}))", "le.u": "boolWord({} <= {})",
                        "shr.s": "uint64(int64({}) >> {})", "shr.u": "{} >> {}"})
        for op, expr in ops.items():
            choices = self.names + (["r"] if self.form == "mixed" else [])
            for d, x, y in itertools.product(choices, repeat=3):
                if (d, x, y).count("r") > 1 or self.form == "dest" and d != x:
                    continue
                args = [d, y] if self.form == "dest" else [d, x, y]
                left, right = [get if operand == "r" else operand for operand in (x, y)]
                guard = ""
                if op in ("div.s", "div.u"):
                    guard = f"if {right} == 0 {{ failure = ErrDivideByZero; goto fault }}\n"
                self.emit(".".join([op, *args]), guard + assign(d, expr.format(left, right)))

    def alu(self, op, d, x, y, reg=0):
        if self.form == "dest":
            assert "r" not in (d, x, y)
            if d == y and d != x:
                assert op in ("mul", "add")
                x, y = y, x
            move = [] if d == x else ["move." + d + "." + x]
            return move + [f"{op}.{d}.{y}"]
        return [f"{op}.{d}.{x}.{y} {reg}"]

    def schedules(self):
        # Each list replaces one original PC. @ re-emits the original word.
        # The preheader initializes locals on every entry. Old branch targets
        # refer to the first replacement word at that PC, including exit spills.
        a = self.alu
        if self.bank == "f":
            p = {31: ["load.f0.r 29", "load.f1.r 30", "@"]}
            if self.form == "mixed":
                # f0 balance, f1 factor, f2 temp, f3 total
                p[31].insert(0, "load.f3.r 28")
                p[38] = a("div", "f1", "f1", "r", 5)
                p[39] = a("mul", "f2", "f0", "r", 24) + a("add", "f2", "r", "f2", 18)
                p[40] = a("mul", "f2", "f2", "f1") + a("add", "f3", "f3", "f2")
                p[41] = a("sub", "f0", "f0", "r", 18)
                p[43] = ["store.r.f3 28", "@"]
            else:
                p[38] = ["load.f3.r 5"] + a("div", "f1", "f1", "f3")
                p[39] = ["load.f3.r 24"] + a("mul", "f2", "f0", "f3") + ["load.f3.r 18"] + a("add", "f2", "f2", "f3")
                p[40] = a("mul", "f2", "f2", "f1") + a("sub", "f0", "f0", "f3") + ["load.f3.r 28"] + a("add", "f3", "f3", "f2") + ["store.r.f3 28"]
                p[41] = []
            m = {32: ["load.f0.r 20"], 33: ["load.f1.r 13"], 34: ["load.f2.r 22"], 35: ["load.f3.r 23"]}
            m[43] = a("mul", "f1", "f0", "f1")
            m[44] = a("sub", "f0", "f2", "f3")
            if self.form == "mixed":
                m[45] = a("mul", "f1", "f1", "r", 6) + a("add", "f1", "f1", "r", 13) + a("add", "f0", "f0", "r", 20)
            else:
                m[45] = ["load.f2.r 6"] + a("mul", "f1", "f1", "f2") + ["load.f2.r 13"] + a("add", "f1", "f1", "f2") + ["load.f2.r 20"] + a("add", "f0", "f0", "f2")
            m[46] = a("mul", "f3", "f1", "f1")
            m[47] = a("mul", "f2", "f0", "f0")
            m[48] = a("add", "r", "f2", "f3", 41) if self.form == "mixed" else ["store.r.f2 28", "store.r.f3 29", "@"]
            self.plans = {"Price": p, "Mandelbrot": m}
        elif self.bank == "u":
            # Sieve's first loop already has just a checked store and fused latch.
            # In the strike loop k is dead at exit. Keep the flags store unchanged.
            p = {26: ["load.u0.r 20", "load.u1.r 8", "@"], 31: a("sub", "r", "u0", "u1", 22)}
            if len(self.names) == 4:
                p[26] = ["load.u0.r 20", "load.u1.r 8", "load.u2.r 11", "load.u3.r 3", "@"]
                p[33] = a("add", "u0", "u0", "u2") + a("le.s", "r", "u0", "u3", 21) + ["jnz 21 31"]
            else:
                p[33] = a("add", "u0", "u0", "r", 11) + ["store.r.u0 20", "le.i64 21 20 3", "jnz 21 31"]
            self.plans = {"Sieve": p}

    def build(self, tmp, slim, counts, island, fused, plain):
        tmp.mkdir(parents=True, exist_ok=True)
        overlay = {}

        def put(src, content):
            dest = tmp / src.name
            dest.write_text(content)
            overlay[str(src)] = str(dest)

        if fused:
            # Preserve the existing rounded mul-add. The Mandelbrot update has
            # two distinct frame constants, so this extra control also tries a
            # second frame operand in B on that already fused instruction.
            p, m = self.plans["Price"], self.plans["Mandelbrot"]
            p[31] = ["load.f0.r 29", "load.f1.r 30", "load.f3.r 18", "@"]
            p[39] = ["addmul.f2.f3.f0.r 24"]
            p[40] = ["macc.r.f2.f1 28"]
            p[41] = ["sub.f0.f0.f3"]
            p[43] = ["@"]
            m[44] = ["muladd.f1.f1.r.r 6 13"]
            m[45] = ["subadd.f0.f2.f3.r 20"]
            self.emit("addmul.f2.f3.f0.r", "f2 = f3 + float64(f0*math.Float64frombits(r[a]))")
            self.emit("macc.r.f2.f1", "r[a] = math.Float64bits(math.Float64frombits(r[a]) + float64(f2*f1))")
            self.emit("muladd.f1.f1.r.r", "f1 = float64(f1*math.Float64frombits(r[a])) + math.Float64frombits(r[b])")
            self.emit("subadd.f0.f2.f3.r", "f0 = float64(f2-f3) + math.Float64frombits(r[a])")

        if plain:
            self.plans = {}
            self.ops = {}
        used = {word.split()[0] for plan in self.plans.values() for lines in plan.values() for word in lines}
        forms = {name: body for name, body in self.ops.items() if not slim or name in used}
        names = list(forms)
        base = 160  # experiment-only range above all production opcodes
        op = {name: base + i for i, name in enumerate(names)}
        switch = "".join(f"\t\tcase {op[name]}: // {name}\n\t\t\t{body}\n" for name, body in forms.items())

        def replace(s, old, new):
            if old not in s:
                raise ValueError("overlay anchor missing: " + old)
            return s.replace(old, new, 1)

        vm = (EVM / "evm.go").read_text()
        if island:
            helper = (HERE / "island.go.in").read_text().replace("/*HANDLERS*/", switch)
            if plain:
                lo = vm.index("\t\tcase OpMulAddF64:")
                hi = vm.index("\t\tcase OpAnd:", lo)
                arithmetic = vm[lo:hi]
                helper = helper.replace("case OpMoveR:", arithmetic + "case OpMoveR:")
                helper = helper.replace("f0, f1, f2, f3 := bank.f0, bank.f1, bank.f2, bank.f3", "").replace("bank.f0, bank.f1, bank.f2, bank.f3 = f0, f1, f2, f3", "")
                vm = vm[:lo] + vm[hi:]
            if counts:
                helper = replace(helper, "switch ins.Op()", "ScratchCounts[ins.Op()]++; switch ins.Op()")
                helper = replace(helper, "return pc-1, spent, true", "ScratchCounts[ins.Op()]--; return pc-1, spent, true")
            put(EVM / "scratch_island.go", helper)
            entry = "case " + ",".join(str(op[n]) for n in names if n.startswith("load.")) + ":"
            if plain:
                entry = "case OpMulAddF64, OpAddMulF64, OpSubAddF64, OpAddF64, OpSubF64, OpMulF64, OpDivF64:"
            vm = replace(vm, "\t\tcase OpDeferStack:", entry + """
            remaining, failure = runScratch(f.Code, r, &cur, remaining+1, budget)
            if failure != nil {return vm.trap(cur,failure)}
        case OpDeferStack:""")
        elif self.names:
            locals_ = "var " + ", ".join(self.names) + (" float64" if self.bank == "f" else " uint64") + "\n"
            vm = replace(vm, "resumeFrame:", locals_ + "resumeFrame:")
            vm = replace(vm, "\t\tcase OpDeferStack:", switch + "\t\tcase OpDeferStack:")
        if counts:
            vm = replace(vm, "\t\tcur.pc++\n\t\ta, b, c :=", "\t\tcur.pc++\n\t\tScratchCounts[ins.Op()]++\n\t\ta, b, c :=")
            if island:
                vm = replace(vm, "remaining, failure = runScratch", "ScratchCounts[ins.Op()]--; remaining, failure = runScratch")
            vm += "\nvar ScratchCounts [65536]uint64\n"
        put(EVM / "evm.go", vm)
        if forms:
            common = f"package evm\nconst scratchBase = {base}\nvar scratchNames = [...]string{{\n" + "".join(json.dumps(n) + ",\n" for n in names) + "}\n"
            common += "func (op Opcode) isScratch() bool {return op >= scratchBase && int(op)-scratchBase < len(scratchNames)}\n"
            mixed = [str(op[n]) for n in names if "r" in n.split(".")]
            common += "func (op Opcode) scratchR() bool { switch op {case " + ",".join(mixed) + ":return true};return false}\n"
            two = op.get("muladd.f1.f1.r.r", 0)
            common += f"func (op Opcode) scratchRR() bool {{return op == {two} && op.isScratch()}}\n"
            put(EVM / "scratch_forms.go", common)
            ins = (EVM / "instruction.go").read_text()
            put(EVM / "instruction.go", replace(ins, "if form := op.hostForm();", "if op.isScratch() { return scratchNames[op-scratchBase] }; if form := op.hostForm();"))
            dasm = (EVM / "disassemble.go").read_text()
            put(EVM / "disassemble.go", replace(dasm, "if pa, pb, ok := i.Op().HostRegisters();", 'if i.Op().isScratch() { if i.Op().scratchRR() {return fmt.Sprintf("r%d r%d", a,b)}; if i.Op().scratchR() {return fmt.Sprintf("r%d", a)}; return "" }; if pa, pb, ok := i.Op().HostRegisters();'))
            link = (EVM / "program.go").read_text()
            put(EVM / "program.go", replace(link, "valid := false\n\tswitch i.Op() {", 'if i.Op().isScratch() {if (i.Op().scratchRR() && r(b) || b == 0) && c == 0 && (i.Op().scratchR() && r(a) || !i.Op().scratchR() && a==0) {return nil};return fmt.Errorf("bad scratch operand")}; valid := false\n\tswitch i.Op() {'))

        tpl = (HERE / "schedule.go.in").read_text()
        plans = []
        for name, pcs in self.plans.items():
            entries = []
            for pc, words in pcs.items():
                ins = []
                for word in words:
                    parts = word.split()
                    if parts[0] == "@":
                        ins.append("old")
                    elif parts[0] == "jnz":
                        ins.append("jump(%s,%s)" % tuple(parts[1:]))
                    elif parts[0] == "le.i64":
                        ins.append("evm.ABC(evm.OpLeI64,%s,%s,%s)" % tuple(parts[1:]))
                    else:
                        ins.append(f"evm.ABC({op[parts[0]]},{parts[1] if len(parts)>1 else 0},{parts[2] if len(parts)>2 else 0},0)")
                entries.append(f"case {pc}: return []evm.Instruction{{" + ",".join(ins) + "}")
            plans.append(f'case "{name}": switch pc {{' + "\n".join(entries) + "}")
        tpl = tpl.replace("/*PLANS*/", "\n".join(plans)).replace("/*NAMES*/", ",".join(json.dumps(n) for n in self.plans))
        if counts:
            tpl += (HERE / "counts.go.in").read_text()
        put(EVM / "bench/scratch_schedule_test.go", tpl)
        bench = (EVM / "bench/bench_test.go").read_text()
        put(EVM / "bench/bench_test.go", replace(bench, "p, err := evm.Link(m, pkg)", "scheduleScratch(m)\n\tp, err := evm.Link(m, pkg)"))
        path = tmp / "overlay.json"
        path.write_text(json.dumps({"Replace": overlay}, indent=2))
        subprocess.run(["go", "test", "-overlay", path, "-c", "-o", tmp / "bench", "./evm/bench"], cwd=ROOT, check=True)
        subprocess.run([tmp / "bench", "-test.run=^TestEVM$"], cwd=EVM / "bench", check=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("out", type=Path)
    p.add_argument("bank", choices=["none", "f4", "u2", "u4"])
    p.add_argument("form", choices=["dest", "three", "mixed"])
    p.add_argument("--slim", action="store_true", help="generate only handlers used by the hand schedule")
    p.add_argument("--counts", action="store_true", help="instrument separate, untimed binary")
    p.add_argument("--island", action="store_true", help="dispatch call-free float runs in bounded chunks")
    p.add_argument("--fused", action="store_true", help="keep the rounded float superinstructions")
    p.add_argument("--plain", action="store_true", help="isolated float dispatch using the original R operands")
    args = p.parse_args()
    if args.plain and not args.island:
        p.error("plain requires island")
    if (args.island or args.fused) and (args.bank, args.form) != ("f4", "mixed"):
        p.error("island and fused trials use the four-float mixed schedule")
    v = Variant(args.bank[0], 0 if args.bank == "none" else int(args.bank[1]), args.form)
    if v.bank == "u" and args.form != "mixed":
        p.error("the integer strike-loop schedule uses mixed operands")
    v.generate()
    v.schedules()
    v.build(args.out.resolve(), args.slim, args.counts, args.island, args.fused, args.plain)


if __name__ == "__main__":
    main()
