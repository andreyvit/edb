package kvolab

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func changeFixture(n int) ChangeSet {
	cs := ChangeSet{Schema: 1, Base: 42, Next: 43}
	for i := 0; i < n; i++ {
		cs.Changes = append(cs.Changes, Change{Path: []Step{{Kind: FieldStep, U: 58}, {Kind: StringKeyStep, S: fmt.Sprintf("key-%04d", i)}}, Op: Set, Value: Encode(S("changed"), Compact)})
	}
	return cs
}
func TestChangesRoundTrip(t *testing.T) {
	cs := changeFixture(6)
	cs.Changes = append(cs.Changes, Change{Path: []Step{{Kind: FieldStep, U: 57}, {Kind: FieldStep, U: 5}, {Kind: StringKeyStep, S: "key-0001"}}, Op: Delete}, Change{Path: []Step{{Kind: FieldStep, U: 3}}, Op: Set, Value: Encode(Value{Kind: Null}, Compact)})
	for _, prefix := range []bool{false, true} {
		encoded := AppendChanges(nil, cs, prefix)
		var scratch [16]Step
		var got []Change
		head, err := VisitChanges(encoded, scratch[:], func(path []Step, op Op, v View) error {
			got = append(got, Change{append([]Step(nil), path...), op, bytes.Clone(v)})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if head.Schema != cs.Schema || head.Base != cs.Base || head.Next != cs.Next || !reflect.DeepEqual(got, cs.Changes) {
			t.Fatal("round trip")
		}
	}
}
func TestChangeReplay(t *testing.T) {
	v := fixtures()["state"]
	cs := changeFixture(6)
	revision := cs.Base
	apply := func(cs ChangeSet) error {
		if revision == cs.Next {
			return nil
		}
		if revision != cs.Base {
			return fmt.Errorf("revision conflict")
		}
		for _, ch := range cs.Changes {
			c, _ := readValue(ch.Value)
			if err := ApplyToFixture(&v, ch.Path, ch.Op, Value{Kind: c.Kind, S: string(c.Bytes())}); err != nil {
				return err
			}
		}
		revision = cs.Next
		return nil
	}
	if err := apply(cs); err != nil {
		t.Fatal(err)
	}
	first := Encode(v, Compact)
	if err := apply(cs); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, Encode(v, Compact)) {
		t.Fatal("duplicate replay changed value")
	}
	bad := cs
	bad.Base = 1
	bad.Next = 44
	if apply(bad) == nil {
		t.Fatal("stale base accepted")
	}
	c, _ := View(first).Get(58)
	x, _ := c.Object().GetString("key-0004")
	if string(x.Bytes()) != "changed" {
		t.Fatal("not applied")
	}
}
func TestChangeOrder(t *testing.T) {
	v := Obj(Field{1, Map(Entry{"a", U(1)})})
	if err := ApplyToFixture(&v, []Step{{Kind: FieldStep, U: 1}}, Set, Map()); err != nil {
		t.Fatal(err)
	}
	if err := ApplyToFixture(&v, []Step{{Kind: FieldStep, U: 1}, {Kind: StringKeyStep, S: "b"}}, Set, U(2)); err != nil {
		t.Fatal(err)
	}
	c, _ := View(Encode(v, Compact)).Get(1)
	if _, ok := c.Object().GetString("a"); ok {
		t.Fatal("old child survived")
	}
	if _, ok := c.Object().GetString("b"); !ok {
		t.Fatal("child lost")
	}
}
func TestChangeTruncation(t *testing.T) {
	data := AppendChanges(nil, changeFixture(8), true)
	for i := 0; i < len(data); i++ {
		var scratch [8]Step
		_, err := VisitChanges(data[:i], scratch[:], func([]Step, Op, View) error { return nil })
		if err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
}
func TestChangeSizes(t *testing.T) {
	for _, n := range []int{1, 4, 16, 64} {
		cs := changeFixture(n)
		t.Logf("string-edits=%d plain=%d prefix=%d", n, len(AppendChanges(nil, cs, false)), len(AppendChanges(nil, cs, true)))
	}
	for _, n := range []int{1, 4, 16, 64} {
		cs := ChangeSet{Schema: 1, Base: 42, Next: 43}
		for i := 0; i < n; i++ {
			cs.Changes = append(cs.Changes, Change{Path: []Step{{Kind: FieldStep, U: 53}, {Kind: UintKeyStep, U: 1<<54 + uint64(i)}, {Kind: FieldStep, U: 3}}, Value: Encode(U(9), Compact)})
		}
		t.Logf("numeric-edits=%d plain=%d prefix=%d", n, len(AppendChanges(nil, cs, false)), len(AppendChanges(nil, cs, true)))
	}
}
func BenchmarkChangeEncode(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		cs := changeFixture(n)
		dst := make([]byte, 0, 8192)
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst = AppendChanges(dst[:0], cs, true)
			}
			sinkBytes = dst
		})
	}
}
func BenchmarkChangeVisit(b *testing.B) {
	data := AppendChanges(nil, changeFixture(16), true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var scratch [8]Step
		_, err := VisitChanges(data, scratch[:], func(path []Step, op Op, v View) error { sinkU += uint64(len(path) + len(v)); return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestBorrowedChanges(t *testing.T) {
	data := AppendChanges(nil, changeFixture(6), true)
	var scratch [8]PathRef
	r, err := OpenChanges(data, scratch[:])
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		c, ok, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if string(r.Key(c.Path[1])) != fmt.Sprintf("key-%04d", n) {
			t.Fatal("path")
		}
		n++
	}
	if n != 6 {
		t.Fatal("count")
	}
}
func BenchmarkChangeReader(b *testing.B) {
	data := AppendChanges(nil, changeFixture(16), true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var scratch [8]PathRef
		r, err := OpenChanges(data, scratch[:])
		if err != nil {
			b.Fatal(err)
		}
		for {
			c, ok, err := r.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			sinkU += uint64(len(c.Path) + len(c.Value))
		}
	}
}

func TestChangesV2(t *testing.T) {
	for _, n := range []int{1, 4, 16, 64} {
		cs := changeFixture(n)
		data := AppendChanges2(nil, cs)
		t.Logf("string-edits=%d v1=%d v2=%d", n, len(AppendChanges(nil, cs, true)), len(data))
		var scratch [8]PathRef
		r, err := OpenChanges(data, scratch[:])
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			c, ok, err := r.Next()
			if err != nil || !ok {
				t.Fatalf("read %v %v", ok, err)
			}
			if string(r.Key(c.Path[1])) != cs.Changes[i].Path[1].S || !bytes.Equal(c.Value, cs.Changes[i].Value) {
				t.Fatal("mismatch")
			}
		}
		if _, ok, err := r.Next(); ok || err != nil {
			t.Fatal("end")
		}
	}
}

func TestChangeMessagePackValues(t *testing.T) {
	cs := changeFixture(4)
	for i := range cs.Changes {
		cs.Changes[i].Value = mpEncode(S("changed"), false)
	}
	data := AppendChanges2(nil, cs)
	var scratch [8]PathRef
	r, err := OpenChanges(data, scratch[:])
	if err != nil {
		t.Fatal(err)
	}
	for range cs.Changes {
		c, ok, err := r.Next()
		if err != nil || !ok {
			t.Fatal("read")
		}
		if string(MPView(c.Value).Bytes()) != "changed" {
			t.Fatal("value")
		}
	}
	t.Logf("four string assignments, v2 with MessagePack values: %d bytes", len(data))
}

func BenchmarkChangeV2(b *testing.B) {
	cs := changeFixture(16)
	for i := range cs.Changes {
		cs.Changes[i].Value = mpEncode(S("changed"), false)
	}
	data := AppendChanges2(nil, cs)
	b.Run("encode-reused", func(b *testing.B) {
		dst := make([]byte, 0, len(data))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = AppendChanges2(dst[:0], cs)
		}
		sinkBytes = dst
	})
	b.Run("read-borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var scratch [8]PathRef
			r, err := OpenChanges(data, scratch[:])
			if err != nil {
				b.Fatal(err)
			}
			for {
				c, ok, err := r.Next()
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					break
				}
				sinkU += uint64(len(r.Key(c.Path[1])) + len(c.Value))
			}
		}
	})
}
