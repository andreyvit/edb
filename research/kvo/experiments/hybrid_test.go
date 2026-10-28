package kvolab

import "testing"

// This bridge experiments with standard MessagePack compact leaves under a
// fixed-cell indexed root. Blob framing is a prototype adapter, not a proposed
// extra logical type or finalized extension tag.
func mpHybrid(v Value) View {
	copyValue := v
	copyValue.Fields = append([]Field(nil), v.Fields...)
	for i := range copyValue.Fields {
		c := &copyValue.Fields[i].Value
		if !scalar(c.Kind) {
			*c = Value{Kind: Blob, B: mpEncode(*c, false)}
		}
	}
	return Encode(copyValue, Indexed)
}
func TestMPHybrid(t *testing.T) {
	for _, name := range []string{"profile", "state", "settings", "transaction", "sparse-event"} {
		v := fixtures()[name]
		raw := mpHybrid(v)
		t.Logf("%s indexed-root-mp-leaves=%d mp-numeric=%d", name, len(raw), len(mpEncode(v, false)))
	}
	v := mpHybrid(fixtures()["state"])
	c, _ := v.Get(57)
	profile := MPView(c.Bytes())
	props, ok := profile.Get(5)
	if !ok {
		t.Fatal("properties")
	}
	x, ok := props.GetString("key-0003")
	if !ok || string(x.Bytes()) != "value-0003" {
		t.Fatal("lookup")
	}
}
func BenchmarkMPHybrid(b *testing.B) {
	v := mpHybrid(fixtures()["state"])
	b.ReportAllocs()
	var sum uint64
	for i := 0; i < b.N; i++ {
		c, _ := v.Get(57)
		profile := MPView(c.Bytes())
		props, _ := profile.Get(5)
		x, _ := props.GetString("key-0003")
		sum += uint64(len(x.Bytes()))
	}
	sinkU = sum
}
