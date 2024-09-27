package kvo

type ValueKind int

const (
	ValueKindUnknown ValueKind = iota
	ValueKindInteger
	ValueKindSubobject
	ValueKindPIIData
)
