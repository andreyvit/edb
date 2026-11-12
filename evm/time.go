package evm

import "github.com/andreyvit/edb/values"

// TimePackage exposes the database's timestamp cell as time.Time. It is linked
// automatically, so record fields and host methods share the same nominal type.
func TimePackage() Package {
	p := Package{Path: "time", Types: []TypeDecl{{Name: "Time", Shape: &values.Shape{Type: values.TimeType, Kind: values.TimestampKind, Cells: 1}}}}
	add := func(name string, params []*values.Type, result *values.Type, f func(*Call) values.Value) {
		p.Methods = append(p.Methods, ExternalMethod{Ownership: &OwnershipEffects{NoRetain: true, NoInvalidate: true}, Name: name, Receiver: values.TimeType, Signature: Signature{Params: params, Results: []*values.Type{result}}, Call: func(c *Call) error { c.SetResult(0, f(c)); return nil }})
	}
	add("IsZero", nil, values.Boolean, func(c *Call) values.Value { return values.Bool(c.Argument(0).AsTime().IsZero()) })
	for _, name := range []string{"Before", "After", "Equal"} {
		add(name, []*values.Type{values.TimeType}, values.Boolean, func(c *Call) values.Value {
			a, b := c.Argument(0).AsTime(), c.Argument(1).AsTime()
			switch name {
			case "Before":
				return values.Bool(a.Before(b))
			case "After":
				return values.Bool(a.After(b))
			default:
				return values.Bool(a.Equal(b))
			}
		})
	}
	for _, name := range []string{"Unix", "UnixMilli", "UnixMicro", "UnixNano"} {
		add(name, nil, values.Int64, func(c *Call) values.Value {
			t := c.Argument(0).AsTime()
			switch name {
			case "Unix":
				return values.Int(t.Unix())
			case "UnixMilli":
				return values.Int(t.UnixMilli())
			case "UnixMicro":
				return values.Int(t.UnixMicro())
			default:
				return values.Int(t.UnixNano())
			}
		})
	}
	return p
}
