package kvo

type ValueKind int

const (
	ValueKindUnknown ValueKind = iota
	ValueKindWord
	ValueKindMap
	ValueKindScalarData
	ValueKindPIIData
)
