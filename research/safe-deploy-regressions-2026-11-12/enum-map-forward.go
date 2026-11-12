//go:build ignore

// Historical enum dictionary keys must remain addressable during partial edits.
package main

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/msocmon"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/nm"
	"github.com/vmihailenco/msgpack/v5"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func fixture(mask, mode int) []byte {
	var b bytes.Buffer
	e := msgpack.NewEncoder(&b)
	must(e.EncodeMapLen(3))
	must(e.EncodeString("name"))
	must(e.EncodeString("retained"))
	must(e.EncodeString("extras"))
	must(e.Encode(map[string]any{"unmodeled": "retained"}))
	must(e.EncodeString("soc"))
	must(e.EncodeMapLen(1))
	must(e.EncodeString("hs"))
	must(e.EncodeMapLen(9))
	for i := 0; i < 9; i++ {
		p := msocmon.Platform(i)
		if mask&(1<<i) != 0 {
			must(e.EncodeInt(int64(i)))
		} else {
			name := p.String()
			if mode == 2 && i == 3 {
				name = "X"
			}
			if mode == 0 {
				must(e.EncodeBytes([]byte(name)))
			} else {
				must(e.EncodeString(name))
			}
		}
		must(e.Encode(map[string]any{"a": fmt.Sprintf("alias-%d", i), "#": fmt.Sprintf("id-%d", i)}))
	}
	return b.Bytes()
}
func checkRawMap(raw []byte, want int) {
	d := msgpack.NewDecoder(bytes.NewReader(raw))
	n, e := d.DecodeMapLen()
	must(e)
	for j := 0; j < n; j++ {
		key, e := d.DecodeString()
		must(e)
		if key != "soc" {
			must(d.Skip())
			continue
		}
		n, e := d.DecodeMapLen()
		must(e)
		for j := 0; j < n; j++ {
			key, e := d.DecodeString()
			must(e)
			if key != "hs" {
				must(d.Skip())
				continue
			}
			n, e := d.DecodeMapLen()
			must(e)
			if n != want {
				panic("encoded dictionary entry count changed")
			}
			seen := map[msocmon.Platform]bool{}
			for j := 0; j < n; j++ {
				var p msocmon.Platform
				must(d.Decode(&p))
				if seen[p] {
					panic("duplicate logical enum key")
				}
				seen[p] = true
				must(d.Skip())
			}
			return
		}
	}
	panic("missing handles dictionary")
}
func main() {
	projection := kvo.Project(reflect.TypeFor[bm.Customer](), fdb.CustomersTable.Model(), nil)
	checks := 0
	for mode := 0; mode < 3; mode++ {
		for mask := 0; mask < 512; mask++ {
			raw := fixture(mask, mode)
			r := projection.MessagePack(raw)
			base := r.Get(nm.Customer_SocialMedia).Get(nm.CustomerSocialMedia_Handles)
			if base.Len() != 9 {
				panic("historical map size")
			}
			for p := msocmon.Platform(0); p <= 8; p++ {
				original := fmt.Sprintf("alias-%d", p)
				if base.Get(p).Get(nm.CustomerSocialMediaHandle_Alias) != original {
					panic("historical key lookup failed")
				}
				for _, remove := range []bool{false, true} {
					e := r.Edit()
					m := e.Edit(nm.Customer_SocialMedia).Map(nm.CustomerSocialMedia_Handles)
					want := 9
					if remove {
						if !m.Delete(p) {
							panic("delete did not find historical key")
						}
						want--
					} else {
						m.Edit(p).Set(nm.CustomerSocialMediaHandle_Alias, "changed\x00é")
					}
					encoded := e.MessagePack()
					checkRawMap(encoded, want)
					var c bm.Customer
					must(msgpack.Unmarshal(encoded, &c))
					if len(c.SocialMedia.Handles) != want || c.Name != "retained" || c.Extras["unmodeled"] != "retained" {
						panic("partial edit changed unrelated fields")
					}
					for q := msocmon.Platform(0); q <= 8; q++ {
						h, ok := c.SocialMedia.Handles[q]
						if q == p && remove {
							if ok {
								panic("deleted key reappeared")
							}
							continue
						}
						expected := fmt.Sprintf("alias-%d", q)
						if q == p {
							expected = "changed\x00é"
						}
						if !ok || h.Alias != expected || h.ID != fmt.Sprintf("id-%d", q) {
							panic("edited dictionary content mismatch")
						}
					}
					if base.Len() != 9 || base.Get(p).Get(nm.CustomerSocialMediaHandle_Alias) != original {
						panic("retained historical map changed")
					}
					checks++
				}
			}
		}
	}
	fmt.Printf("PASS %d enum map edit/delete cases across 1536 mixed historical encodings\n", checks)
}
