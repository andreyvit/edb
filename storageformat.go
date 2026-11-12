package edb

// StorageFormat names payload encodings, not row-envelope versions. Fast1 and
// MessagePack remain supported; new storage should use Fast2 or Compact2.
type StorageFormat uint8

const (
	MessagePack StorageFormat = iota
	Fast1
	Fast2
	Compact2
)

func (f StorageFormat) String() string {
	switch f {
	case MessagePack:
		return "MessagePack"
	case Fast1:
		return "Fast1"
	case Fast2:
		return "Fast2"
	case Compact2:
		return "Compact2"
	default:
		return "unknown"
	}
}
