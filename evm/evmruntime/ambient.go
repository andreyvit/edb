package evmruntime

// Ambient is an immutable stack of inherited values. Values themselves are not
// copied. A nil pointer is empty. CloneWith snapshots the supplied delta map.
type Ambient struct {
	parent *Ambient
	values map[string]any
}

func (a *Ambient) Get(key string) any {
	for node := a; node != nil; node = node.parent {
		if value, ok := node.values[key]; ok {
			return value
		}
	}
	return nil
}

func (a *Ambient) CloneWith(values map[string]any) *Ambient {
	if len(values) == 0 {
		return a
	}
	delta := make(map[string]any, len(values))
	for key, value := range values {
		delta[key] = value
	}
	return &Ambient{parent: a, values: delta}
}

// Values returns an independent map, for bridges to APIs that accept extras.
func (a *Ambient) Values() map[string]any {
	result := make(map[string]any)
	for node := a; node != nil; node = node.parent {
		for key, value := range node.values {
			if _, ok := result[key]; !ok {
				result[key] = value
			}
		}
	}
	return result
}
