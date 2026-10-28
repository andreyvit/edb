package kvolab

// Replay data contains only scalar/key-delta bit widths, lengths, container
// structure and ordinal field numbers. No application strings or IDs are stored.
import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sort"
	"sync"
	"testing"
)

// README.md keeps this pattern valid without the ignored replay file. A local
// build with the file present embeds it so portable binaries stay self-contained.
//
//go:embed testdata/*
var workloadShapeFiles embed.FS

type workloadShape struct {
	K string `json:"k"`
	B int    `json:"b"`
	N int    `json:"n"`
	F []struct {
		K int           `json:"k"`
		G int           `json:"g"`
		V workloadShape `json:"v"`
	} `json:"f"`
	E []struct {
		N int           `json:"n"`
		V workloadShape `json:"v"`
	} `json:"e"`
	L []workloadShape `json:"l"`
}

func (s workloadShape) value() Value {
	switch s.K {
	case "w":
		if s.B == 0 {
			return U(0)
		}
		return U(uint64(1) << uint(s.B-1))
	case "s":
		return S(string(bytes.Repeat([]byte{'x'}, s.N)))
	case "b":
		return Value{Kind: Blob, B: bytes.Repeat([]byte{0xa5}, s.N)}
	case "o":
		out := Obj()
		numeric := false
		for _, f := range s.F {
			numeric = numeric || f.G != 0
		}
		var key uint64
		for _, f := range s.F {
			if numeric {
				delta := uint64(0)
				if f.G > 0 {
					delta = uint64(1) << uint(f.G-1)
				}
				if delta > math.MaxUint64-key {
					panic("shape key overflow")
				}
				key += delta
			} else {
				key = uint64(f.K)
			}
			out.Fields = append(out.Fields, Field{key, f.V.value()})
		}
		return out
	case "d":
		out := Value{Kind: Dict}
		counts := map[int]uint64{}
		for _, e := range s.E {
			n := counts[e.N]
			counts[e.N]++
			key := make([]byte, e.N)
			for i := range key {
				key[i] = byte(n)
				n >>= 8
			}
			if n != 0 {
				panic("shape key capacity")
			}
			out.Entries = append(out.Entries, Entry{string(key), e.V.value()})
		}
		sort.Slice(out.Entries, func(i, j int) bool { return out.Entries[i].Key < out.Entries[j].Key })
		return out
	case "l":
		out := Value{Kind: List}
		for _, x := range s.L {
			out.Items = append(out.Items, x.value())
		}
		return out
	default:
		panic("shape kind")
	}
}

var workloadOnce sync.Once
var workloadValues map[string]Value
var workloadMissing bool

func workloadFixtures(tb testing.TB) map[string]Value {
	tb.Helper()
	workloadOnce.Do(func() {
		workloadShapeData, err := workloadShapeFiles.ReadFile("testdata/workload-shapes.json.gz")
		if errors.Is(err, fs.ErrNotExist) {
			workloadMissing = true
			return
		}
		if err != nil {
			panic(err)
		}
		z, err := gzip.NewReader(bytes.NewReader(workloadShapeData))
		if err != nil {
			panic(err)
		}
		raw, err := io.ReadAll(z)
		if err != nil {
			panic(err)
		}
		if err = z.Close(); err != nil {
			panic(err)
		}
		var groups []struct {
			ID     string          `json:"id"`
			Shapes []workloadShape `json:"shapes"`
		}
		if err = json.Unmarshal(raw, &groups); err != nil {
			panic(err)
		}
		workloadValues = map[string]Value{}
		for _, g := range groups {
			for i, s := range g.Shapes {
				workloadValues[fmt.Sprintf("%s-%d", g.ID, i)] = s.value()
			}
		}
	})
	if workloadMissing {
		tb.Skip("local replay fixture absent: restore testdata/workload-shapes.json.gz and rebuild to run sample-dependent experiments")
	}
	return workloadValues
}
func TestWorkloadShapes(t *testing.T) {
	all := workloadFixtures(t)
	if len(all) < 100 {
		t.Fatal("missing shapes")
	}
	for name, v := range all {
		t.Run(name, func(t *testing.T) {
			if v.Kind != Record {
				t.Fatal("row root")
			}
			var w lengthWriter
			raw := lengthEncode(&w, nil, v, nil)
			checkLengthTree(t, v, raw)
		})
	}
}
