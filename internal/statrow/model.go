// Package statrow defines synthetic account state for normal-table benchmarks.
package statrow

import (
	"github.com/andreyvit/edb/kvo"
	"reflect"
	"time"
)

//go:generate go run ./gen
type Profile struct {
	Balance uint64 `edb:"balance"`
	Rate    uint64 `edb:"rate"`
}
type State struct {
	Key          uint64             `edb:"-" msgpack:"-"`
	ID           uint64             `edb:"id"`
	Active       bool               `edb:"active"`
	Revision     uint64             `edb:"revision"`
	Profile      Profile            `edb:"profile"`
	Group        uint64             `edb:"group"`
	Created      time.Time          `edb:"created"`
	LastActivity time.Time          `edb:"last_activity"`
	Tags         []uint64           `edb:"tags,compact"`
	History      map[uint64]int64   `edb:"history,compact"`
	Progress     map[uint64]Profile `edb:"progress"`
	Segments     []uint64           `edb:"segments"`
	Earned       int64              `edb:"earned"`
	Spent        int64              `edb:"spent"`
	Visits       uint64             `edb:"visits"`
}

func Model() (*kvo.Schema, *kvo.Model) {
	s := kvo.NewSchema()
	root := kvo.NewEntityType(s, "customer", reflect.TypeFor[State]())
	p := kvo.NewEntityType(s, "profile", reflect.TypeFor[Profile]())
	for _, x := range []struct {
		k uint64
		n string
		t kvo.AnyType
	}{
		{1, "id", kvo.TUint64}, {3, "active", kvo.TBool}, {5, "balance", kvo.TUint64}, {7, "rate", kvo.TUint64}, {9, "profile", p}, {11, "revision", kvo.TUint64}, {12, "group", kvo.TUint64}, {13, "created", kvo.TTime}, {14, "last_activity", kvo.TTime}, {15, "tags", kvo.ListOf(kvo.TUint64)}, {16, "history", kvo.MapOf(kvo.TUint64, kvo.TInt64)}, {17, "progress", kvo.MapOf(kvo.TUint64, p)}, {18, "segments", kvo.ListOf(kvo.TUint64)}, {19, "earned", kvo.TInt64}, {20, "spent", kvo.TInt64}, {21, "visits", kvo.TUint64}} {
		kvo.NewProp(s, x.k, x.n, x.t, nil)
	}
	kvo.NewModel(s, p, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(5)); b.Prop(s.MustPropByCode(7)) })
	m := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) {
		for _, k := range []uint64{1, 3, 9, 11, 12, 13, 14, 15, 18, 19, 20, 21} {
			b.Prop(s.MustPropByCode(k))
		}
		b.Prop(s.MustPropByCode(16), kvo.KeepDictionaryZeroValues)
		b.Prop(s.MustPropByCode(17), kvo.KeepDictionaryZeroValues)
	})
	return s, m
}
func Sample(id uint64) *State {
	r := &State{Key: id, ID: id, Group: 1 + id%23, Active: id%5 != 0, Profile: Profile{100 + id%37, 1 + id%9}, Visits: id % 92, Earned: int64(id * 14), Spent: int64(id * 9), Created: time.Unix(1600000000+int64(id)*3600, 0).UTC()}
	if id%5 != 0 {
		r.LastActivity = r.Created.Add(time.Hour * 24 * 47)
		for i := uint64(0); i < id%8; i++ {
			r.Tags = append(r.Tags, i+1)
		}
		r.Segments = []uint64{1, id%31 + 2}
		r.History = map[uint64]int64{}
		for i := uint64(0); i < id%9; i++ {
			r.History[19000+i] = int64(i * id)
		}
		if id%3 == 0 {
			r.Progress = map[uint64]Profile{1: r.Profile, 7: {id % 81, 2}}
		}
	}
	return r
}
