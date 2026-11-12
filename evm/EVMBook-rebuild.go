//go:build ignore

// Run from the EDB repository root with go run ./evm/EVMBook-rebuild.go.
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

const bookPath = "evm/_docs/EVMBook.md"

func must[T any](v T, e error) T {
	if e != nil {
		panic(e)
	}
	return v
}

// Check both directions: a new instruction needs a row, and a retired or
// renumbered instruction must not survive unnoticed in the reader's manual.
func checkManual(book string) {
	_, section, ok := strings.Cut(book, "\n## 4. Instruction set\n")
	if !ok {
		panic("missing instruction set chapter")
	}
	section, _, _ = strings.Cut(section, "\n## ")
	rows := regexp.MustCompile("(?m)^\\| ([0-9]+) \\| `([^`]+)` \\|").FindAllStringSubmatch(section, -1)
	// The register-call matrix lists three numbered opcodes per row.
	rows = append(rows, regexp.MustCompile("\\| ([0-9]+) `(call\\.host\\.[^`]+)`").FindAllStringSubmatch(section, -1)...)
	documented := map[int]string{}
	for _, row := range rows {
		n := must(strconv.Atoi(row[1]))
		if n > math.MaxUint16 || evm.Opcode(n).String() != row[2] || documented[n] != "" {
			panic("incorrect or duplicate instruction row: " + row[0])
		}
		documented[n] = row[2]
	}
	for n := 0; n <= math.MaxUint16; n++ {
		if name := evm.Opcode(n).String(); name != documented[n] {
			panic(fmt.Sprintf("missing instruction row for %d %s", n, name))
		}
	}
}

func checkedModule(name, check, src string, pkg evm.Package, options evmcompiler.Options) *evm.Module {
	isSpin := check == "ErrSteps after 8 steps"
	if !isSpin {
		src += "\nfunc BookCheck() bool {\n    return " + check + "\n}"
	}
	m := must(options.Compile(name+".go", src, pkg))
	vm := evm.New(must(evm.Link(m, pkg)))
	if isSpin {
		err := vm.RunWithBudget(context.Background(), name, nil, nil, evm.Budget{Steps: 8})
		if !errors.Is(err, evm.ErrSteps) {
			panic(fmt.Sprintf("expected fuel exhaustion, got %v", err))
		}
	} else {
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "BookCheck", nil, out); err != nil || out[0].R != 1 {
			panic(fmt.Sprintf("%s: %s => %v, %v", name, check, out, err))
		}
	}
	return m
}

func main() {
	book := string(must(os.ReadFile(bookPath)))
	checkManual(book)
	// The filter chapter annotates both register banks. If allocation changes,
	// print the new plan so its listing and the surrounding prose stay together.
	source := must(os.ReadFile("evm/bench/internal/mathwork/kernels.go"))
	filterModule := must(evmcompiler.Compile("math.go", string(source)))
	plans := must(evm.Link(filterModule)).DisassembleFloatLoops()
	filter := regexp.MustCompile("(?m)^Filter pc .+\n(?:  .+\n)+").FindString(plans)
	if filter == "" || !strings.Contains(book, "```text\n"+filter+"```\n") {
		panic("update the filter listing and operands in the optimization chapter:\n" + filter)
	}
	var manuscript strings.Builder
	pkg := evm.Package{Path: "meter", Functions: []evm.ExternalFunction{{Name: "Take", Signature: evm.Signature{Params: []*values.Type{values.StringType, values.IntType}, Results: []*values.Type{values.IntType}}, Call: func(c *evm.Call) error {
		n := len(c.Argument(0).AsString()) + int(c.Argument(1).AsInt())
		c.SetResult(0, values.Int(int64(n)))
		return nil
	}}}}

	sensor := values.NewStructShape("meter.Sensor", values.Field{Name: "Label", Shape: &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}})
	sensorPtr := values.NewPointerShape(sensor)
	pkg.Types = []evm.TypeDecl{{Name: "Sensor", Shape: sensor}, {Shape: sensorPtr}}
	pkg.Methods = []evm.ExternalMethod{{Name: "HasLabel", Receiver: sensorPtr.Type,
		Signature: evm.Signature{Params: []*values.Type{values.StringType}, Results: []*values.Type{values.Boolean}},
		Direct: evm.DirectBinding{Op: evm.OpCallHostMethodSU, Call: func(_ *evm.Call, recv, label values.Value) (values.Value, error) {
			return values.Bool(recv.P != nil && sensor.GetField(recv.P, 0).AsString() == label.AsString()), nil
		}},
	}}

	// Match only a block attached to the preceding marked Go fence. Count the
	// markers independently so a mistyped marker cannot silently drop a demo.
	const start = "<!-- bytecode -->"
	const end = "<!-- /bytecode -->"
	pattern := regexp.MustCompile("(?ms)^<!-- listing: (\\w+) \\| ([^\\n]+?) -->\n```go\n(.*?)\n```\n(?:\n)?" + start + "\n(.*?)^" + end + "$")
	matches := pattern.FindAllStringSubmatchIndex(book, -1)
	n := len(matches)
	count := func(marker string) int {
		return len(regexp.MustCompile("(?m)^"+regexp.QuoteMeta(marker)).FindAllStringIndex(book, -1))
	}
	if n == 0 || n != count("<!-- listing:") || n != count(start) || n != count(end) {
		panic("each marked Go listing needs exactly one adjacent, paired bytecode block")
	}
	seen := map[string]bool{}
	offset := 0
	for _, x := range matches {
		name, check, src := book[x[2]:x[3]], book[x[4]:x[5]], book[x[6]:x[7]]
		if seen[name] {
			panic("duplicate listing: " + name)
		}
		seen[name] = true
		check, raw := strings.CutPrefix(check, "raw | ")
		m := checkedModule(name, check, src, pkg, evmcompiler.Options{NoOptimization: raw})

		manuscript.WriteString(book[offset:x[8]])
		offset = x[9]
		fmt.Fprintln(&manuscript, "```text")
		entry := -1
		for j, f := range m.Functions {
			if f.Name == name {
				entry = j
			}
		}
		if entry == -1 {
			panic("no compiled function for listing: " + name)
		}
		manuscript.WriteString(m.DisassembleFunction(entry))
		for j, f := range m.Functions {
			if j != entry && (f.Name != "BookCheck" || name == "Counter") {
				fmt.Fprintln(&manuscript)
				manuscript.WriteString(m.DisassembleFunction(j))
			}
		}
		fmt.Fprintln(&manuscript, "```")
	}
	manuscript.WriteString(book[offset:])
	if err := os.WriteFile(bookPath, []byte(manuscript.String()), 0644); err != nil {
		panic(err)
	}
	fmt.Printf("Compiled, linked and executed %d examples. Verified the instruction set table and linked filter plan.\n", n)
}
