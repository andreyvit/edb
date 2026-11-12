package values

var (
	IntType     = NewType("int", Raw)
	UintType    = NewType("uint", Raw)
	UintptrType = NewType("uintptr", Raw)
	Int8        = NewType("int8", Raw)
	Int16       = NewType("int16", Raw)
	Int32       = NewType("int32", Raw)
	Uint8       = NewType("uint8", Raw)
	Uint16      = NewType("uint16", Raw)
	Uint32      = NewType("uint32", Raw)
	Float32Type = NewType("float32", Raw)
)
