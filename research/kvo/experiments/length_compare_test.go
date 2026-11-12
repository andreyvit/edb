package kvolab

import (
	"fmt"
	"reflect"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func BenchmarkLengthPaired(b *testing.B) {
	sources := map[string]Value{"numeric": numeric(128, 0), "dictionary": dictionary(128, false), "list": Array()}
	list := Array()
	for i := 0; i < 128; i++ {
		list.Items = append(list.Items, U(uint64(256+i)))
	}
	sources["list"] = list
	for _, name := range []string{"numeric", "dictionary", "list"} {
		source := sources[name]
		for _, indexed := range []bool{false, true} {
			layout := draft.Compact
			if indexed {
				layout = draft.Indexed
			}
			oldRaw := draftObject(source, layout, layout)
			old := draft.Trusted(oldRaw)
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
			v, err := lengthOpen(raw)
			if err != nil {
				b.Fatal(err)
			}
			oldGet := func(i int) uint64 {
				switch name {
				case "numeric":
					return old.Get(query(i, 128)).Uint()
				case "dictionary":
					return uint64(len(old.GetString(source.Entries[query(i, 128)-1].Key).Bytes()))
				default:
					return old.At(int(query(i, 128) - 1)).Uint()
				}
			}
			newGet := func(i int) uint64 {
				switch name {
				case "numeric":
					return v.Get(query(i, 128)).Uint()
				case "dictionary":
					return uint64(len(v.GetString(source.Entries[query(i, 128)-1].Key).Bytes()))
				default:
					return v.At(int(query(i, 128) - 1)).Uint()
				}
			}
			for _, op := range []string{"get", "scan", "open"} {
				for _, next := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/index%v/%s/new%v", name, indexed, op, next), func(b *testing.B) {
						b.ReportAllocs()
						var sum uint64
						for i := 0; i < b.N; i++ {
							switch op {
							case "get":
								if next {
									sum += newGet(i)
								} else {
									sum += oldGet(i)
								}
							case "open":
								if next {
									_, err = lengthOpen(raw)
								} else {
									_, err = draft.Open(oldRaw)
								}
								if err != nil {
									b.Fatal(err)
								}
							case "scan":
								if next {
									it := v.Iter()
									for {
										_, _, x, ok := it.Next()
										if !ok {
											break
										}
										if x.ptr == nil {
											sum += x.Uint()
										} else {
											sum += uint64(len(x.Bytes()))
										}
									}
								} else {
									it := old.Iter()
									for it.Next() {
										x := it.Entry().Value
										if x.Wire() == draft.Bytes {
											sum += uint64(len(x.Bytes()))
										} else {
											sum += x.Uint()
										}
									}
								}
							}
						}
						sinkU = sum
					})
				}
			}
		}
	}
	for _, indexed := range []bool{false, true} {
		layout := draft.Compact
		if indexed {
			layout = draft.Indexed
		}
		source := numeric(128, 0)
		old := draft.Trusted(draftObject(source, layout, layout))
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
		v, _ := lengthOpen(raw)
		for _, n := range []int{1, 8, 64} {
			for _, next := range []bool{false, true} {
				b.Run(fmt.Sprintf("edit/index%v/n%d/new%v", indexed, n, next), func(b *testing.B) {
					// Warm builder metadata and output; the per-operation overlay
					// arrays themselves are intentionally local to the timed loop.
					var warm [64]classMeta
					e := newLengthEditor(v, warm[:0], nil)
					for j := 0; j < n; j++ {
						e.Set(uint64(j+1), lengthWord(uint64(j+999)))
					}
					dst := e.Append(&w, make([]byte, 0, 4096), indexed)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if next {
							var scratch [64]classMeta
							e := newLengthEditor(v, scratch[:0], nil)
							for j := 0; j < n; j++ {
								e.Set(uint64(j+1), lengthWord(uint64(j+999)))
							}
							dst = e.Append(&w, dst[:0], indexed)
						} else {
							var scratch [64]draft.Edit
							e := draft.NewEditor(old, scratch[:], nil)
							for j := 0; j < n; j++ {
								e.Set(uint64(j+1), draft.Uint(uint64(j+999)))
							}
							dst = e.Append(dst[:0], layout)
						}
					}
					sinkBytes = dst
				})
			}
		}
	}
}

func decodePreviousConfiguration(v draft.View) Configuration {
	var out Configuration
	it := v.Iter()
	for it.Next() {
		entry := it.Entry()
		k, x := entry.Key, entry.Value
		switch k {
		case 1:
			out.Name = string(x.Bytes())
		case 2:
			out.Enabled = x.Uint() != 0
		case 3:
			u := x.Uint()
			out.Threshold = int64(u>>1) ^ -int64(u&1)
		case 4:
			child := draft.Trusted(x.Bytes())
			out.Labels = make(map[string]string, child.Len())
			ci := child.Iter()
			for ci.Next() {
				entry := ci.Entry()
				out.Labels[string(entry.KeyBytes)] = string(entry.Value.Bytes())
			}
		case 5:
			child := draft.Trusted(x.Bytes())
			out.Rules = make([]Rule, child.Len())
			i := 0
			ci := child.Iter()
			for ci.Next() {
				rule := draft.Trusted(ci.Entry().Value.Bytes())
				ri := rule.Iter()
				for ri.Next() {
					entry := ri.Entry()
					v := entry.Value
					switch entry.Key {
					case 1:
						out.Rules[i].Name = string(v.Bytes())
					case 2:
						out.Rules[i].Weight = v.Int()
					case 3:
						out.Rules[i].Enabled = v.Uint() != 0
					}
				}
				i++
			}
		}
	}
	return out
}

func decodeLengthConfiguration(v lengthView) Configuration {
	var out Configuration
	it := v.Iter()
	for {
		k, _, x, ok := it.Next()
		if !ok {
			break
		}
		switch k {
		case 1:
			out.Name = string(x.Bytes())
		case 2:
			out.Enabled = x.Uint() != 0
		case 3:
			u := x.Uint()
			out.Threshold = int64(u>>1) ^ -int64(u&1)
		case 4:
			child := lengthTrusted(x.Bytes())
			out.Labels = make(map[string]string, child.Len())
			ci := child.Iter()
			for {
				_, key, value, ok := ci.Next()
				if !ok {
					break
				}
				out.Labels[string(key)] = string(value.Bytes())
			}
		case 5:
			child := lengthTrusted(x.Bytes())
			out.Rules = make([]Rule, child.Len())
			i := 0
			ci := child.Iter()
			for {
				_, _, item, ok := ci.Next()
				if !ok {
					break
				}
				rule := lengthTrusted(item.Bytes())
				ri := rule.Iter()
				for {
					k, _, v, ok := ri.Next()
					if !ok {
						break
					}
					switch k {
					case 1:
						out.Rules[i].Name = string(v.Bytes())
					case 2:
						u := v.Uint()
						out.Rules[i].Weight = int64(u>>1) ^ -int64(u&1)
					case 3:
						out.Rules[i].Enabled = v.Uint() != 0
					}
				}
				i++
			}
		}
	}
	return out
}

func TestLengthConfiguration(t *testing.T) {
	source, want := configurationFixture()
	var w lengthWriter
	old := draft.Trusted(draftObject(source, draft.Compact, draft.Compact))
	next := lengthTrusted(lengthEncode(&w, nil, source, nil))
	if got := decodePreviousConfiguration(old); !reflect.DeepEqual(got, want) {
		t.Fatal("old decode")
	}
	if got := decodeLengthConfiguration(next); !reflect.DeepEqual(got, want) {
		t.Fatal("new decode")
	}
}
func BenchmarkLengthConfiguration(b *testing.B) {
	source, want := configurationFixture()
	var w lengthWriter
	old := draft.Trusted(draftObject(source, draft.Compact, draft.Compact))
	next := lengthTrusted(lengthEncode(&w, nil, source, nil))
	mp := MPView(mpEncode(source, false))
	for _, reads := range []int{1, 8, 64} {
		for _, mode := range []string{"previous", "next", "msgpack-generated", "cached-struct"} {
			b.Run(fmt.Sprintf("reads%d/%s", reads, mode), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					var cfg Configuration
					switch mode {
					case "previous":
						cfg = decodePreviousConfiguration(old)
					case "next":
						cfg = decodeLengthConfiguration(next)
					case "msgpack-generated":
						cfg = decodeMPConfiguration(mp)
					case "cached-struct":
						cfg = want
					}
					for j := 0; j < reads; j++ {
						sum += sumConfiguration(cfg)
					}
				}
				sinkU = sum
			})
		}
	}
}

func BenchmarkLengthPreviousEncode(b *testing.B) {
	all := fixtures()
	all["deep16"] = tagDeep(16, 32768)
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k", "deep16"} {
		source := all[name]
		for _, cold := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/cold%v", name, cold), func(b *testing.B) {
				var e tagDraftEncoder
				dst := e.object(nil, source, 0)
				checkDraftTree(b, source, dst)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if cold {
						e = tagDraftEncoder{}
						dst = nil
					}
					dst = e.object(dst[:0], source, 0)
				}
				sinkBytes = dst
			})
		}
	}
}
