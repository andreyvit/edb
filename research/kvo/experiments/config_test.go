package kvolab

import (
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

type Rule struct {
	Name    string `msgpack:"a"`
	Weight  int64  `msgpack:"b"`
	Enabled bool   `msgpack:"c"`
}
type Configuration struct {
	Name      string            `msgpack:"!"`
	Enabled   bool              `msgpack:"\""`
	Threshold int64             `msgpack:"#"`
	Labels    map[string]string `msgpack:"$"`
	Rules     []Rule            `msgpack:"%"`
}

func configurationFixture() (Value, Configuration) {
	cfg := Configuration{Name: "Example configuration", Enabled: true, Threshold: 400, Labels: make(map[string]string)}
	labels := Map()
	rules := Array()
	for i := 0; i < 32; i++ {
		k := fmt.Sprintf("option-%03d", i)
		value := fmt.Sprintf("text for option %d", i)
		cfg.Labels[k] = value
		labels.Entries = append(labels.Entries, Entry{k, S(value)})
		r := Rule{Name: fmt.Sprintf("rule-%03d", i), Weight: int64(i * 10), Enabled: true}
		cfg.Rules = append(cfg.Rules, r)
		rules.Items = append(rules.Items, Obj(Field{1, S(r.Name)}, Field{2, I(r.Weight)}, Field{3, Value{Kind: True}}))
	}
	return Obj(Field{1, S(cfg.Name)}, Field{2, Value{Kind: True}}, Field{3, I(cfg.Threshold)}, Field{4, labels}, Field{5, rules}), cfg
}

// This handwritten decoder represents generated typed decoding, not reflection.
func decodeConfiguration(v View) Configuration {
	var out Configuration
	v.Walk(func(k uint64, c Cell) bool {
		switch k {
		case 1:
			out.Name = string(c.Bytes())
		case 2:
			out.Enabled = c.Kind == True
		case 3:
			out.Threshold = int64(c.U)
		case 4:
			_, body := payload(c.Raw)
			n, body := takeU(body)
			out.Labels = make(map[string]string, int(n))
			for i := uint64(0); i < n; i++ {
				l, rest := takeU(body)
				key := string(rest[:int(l)])
				x, tail := readValue(rest[int(l):])
				out.Labels[key] = string(x.Bytes())
				body = tail
			}
		case 5:
			_, body := payload(c.Raw)
			n, body := takeU(body)
			out.Rules = make([]Rule, int(n))
			for i := range out.Rules {
				x, rest := readValue(body)
				x.Raw.Walk(func(k uint64, c Cell) bool {
					switch k {
					case 1:
						out.Rules[i].Name = string(c.Bytes())
					case 2:
						out.Rules[i].Weight = int64(c.U)
					case 3:
						out.Rules[i].Enabled = c.Kind == True
					}
					return true
				})
				body = rest
			}
		}
		return true
	})
	return out
}
func sumConfiguration(c Configuration) uint64 {
	sum := uint64(len(c.Name)) + uint64(c.Threshold)
	for k, v := range c.Labels {
		sum += uint64(len(k) + len(v))
	}
	for _, r := range c.Rules {
		sum += uint64(len(r.Name)) + uint64(r.Weight)
	}
	return sum
}
func sumConfigView(v View) uint64 {
	var sum uint64
	v.Walk(func(k uint64, c Cell) bool {
		switch k {
		case 1:
			sum += uint64(len(c.Bytes()))
		case 3:
			sum += c.U
		case 4:
			_, body := payload(c.Raw)
			n, body := takeU(body)
			for i := uint64(0); i < n; i++ {
				l, rest := takeU(body)
				x, tail := readValue(rest[int(l):])
				sum += l + uint64(len(x.Bytes()))
				body = tail
			}
		case 5:
			_, body := payload(c.Raw)
			n, body := takeU(body)
			for i := uint64(0); i < n; i++ {
				x, rest := readValue(body)
				x.Raw.Walk(func(k uint64, c Cell) bool {
					switch k {
					case 1:
						sum += uint64(len(c.Bytes()))
					case 2:
						sum += c.U
					}
					return true
				})
				body = rest
			}
		}
		return true
	})
	return sum
}
func TestConfiguration(t *testing.T) {
	v, want := configurationFixture()
	data := View(Encode(v, Compact))
	got := decodeConfiguration(data)
	if sumConfiguration(got) != sumConfiguration(want) || sumConfigView(data) != sumConfiguration(want) {
		t.Fatal("configuration mismatch")
	}
	encoded, err := msgpack.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Configuration
	if err := msgpack.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if sumConfiguration(decoded) != sumConfiguration(want) {
		t.Fatal("messagepack")
	}
	t.Logf("compact=%d msgpack-struct-default=%d msgpack-best-short=%d", len(data), len(encoded), len(mpEncode(v, true)))
}
func BenchmarkConfiguration(b *testing.B) {
	v, cfg := configurationFixture()
	compact := View(Encode(v, Compact))
	encoded, err := msgpack.Marshal(cfg)
	if err != nil {
		b.Fatal(err)
	}
	for _, repeats := range []int{1, 8, 64} {
		for _, name := range []string{"view-full-walk", "decode-generated", "decode-msgpack", "cached-struct"} {
			b.Run(fmt.Sprintf("reads%d/%s", repeats, name), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					switch name {
					case "view-full-walk":
						for j := 0; j < repeats; j++ {
							sum += sumConfigView(compact)
						}
					case "decode-generated":
						c := decodeConfiguration(compact)
						for j := 0; j < repeats; j++ {
							sum += sumConfiguration(c)
						}
					case "decode-msgpack":
						var c Configuration
						if err := msgpack.Unmarshal(encoded, &c); err != nil {
							b.Fatal(err)
						}
						for j := 0; j < repeats; j++ {
							sum += sumConfiguration(c)
						}
					case "cached-struct":
						for j := 0; j < repeats; j++ {
							sum += sumConfiguration(cfg)
						}
					}
				}
				sinkU = sum
			})
		}
	}
}

func decodeMPConfiguration(v MPView) Configuration {
	var out Configuration
	v.Walk(func(k uint64, c MPView) bool {
		switch k {
		case 1:
			out.Name = string(c.Bytes())
		case 2:
			out.Enabled = c[0] == 0xc3
		case 3:
			out.Threshold = c.Int()
		case 4:
			_, n, body := mpHead(c)
			out.Labels = make(map[string]string, n)
			for i := 0; i < n; i++ {
				key := string(MPView(body).Bytes())
				body = mpSkip(body)
				value := string(MPView(body).Bytes())
				body = mpSkip(body)
				out.Labels[key] = value
			}
		case 5:
			_, n, body := mpHead(c)
			out.Rules = make([]Rule, n)
			for i := range out.Rules {
				end := mpSkip(body)
				MPView(body[:len(body)-len(end)]).Walk(func(k uint64, c MPView) bool {
					switch k {
					case 1:
						out.Rules[i].Name = string(c.Bytes())
					case 2:
						out.Rules[i].Weight = c.Int()
					case 3:
						out.Rules[i].Enabled = c[0] == 0xc3
					}
					return true
				})
				body = end
			}
		}
		return true
	})
	return out
}
func TestMPGeneratedDecode(t *testing.T) {
	v, want := configurationFixture()
	data := mpEncode(v, false)
	if sumConfiguration(decodeMPConfiguration(data)) != sumConfiguration(want) {
		t.Fatal("decode")
	}
	t.Logf("compact integer-field MessagePack: %d bytes", len(data))
}
func BenchmarkMPGeneratedDecode(b *testing.B) {
	v, _ := configurationFixture()
	data := mpEncode(v, false)
	b.ReportAllocs()
	var sum uint64
	for i := 0; i < b.N; i++ {
		sum += sumConfiguration(decodeMPConfiguration(data))
	}
	sinkU = sum
}
