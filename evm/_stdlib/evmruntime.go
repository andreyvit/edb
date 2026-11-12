package evmruntime

// IncreaseCPUBudget adds milliseconds to the current scope, up to its hard limit.
func IncreaseCPUBudget(ms uint64)

// IncreaseMemBudget adds bytes and allocations to the invocation's soft limits.
func IncreaseMemBudget(bytes, allocs uint64)
