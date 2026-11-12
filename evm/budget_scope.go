package evm

import (
	"context"
	"errors"
	"time"
)

// BudgetScope temporarily restricts the running invocation. A scope may span
// several external calls. Only the innermost scope can be closed.
type BudgetScope struct {
	owner                  *executionBudget
	parent                 *BudgetScope
	previous               context.Context
	cancel                 context.CancelFunc
	previousRecords        *RecordScope
	records                *RecordScope
	floor                  uint64
	started                time.Time
	duration, hardDuration time.Duration
}

// NarrowBudget starts a scope at the current external call. Zero leaves that
// limit alone. All work still counts against the enclosing budgets.
func (c *Call) NarrowBudget(steps uint64, duration time.Duration) *BudgetScope {
	b := c.budget
	s := &BudgetScope{owner: b, parent: b.scope, previous: b.ctx}
	// Specialized external calls can run between meter checkpoints.
	// Allow that prefix so a short section never exhausts its quota early.
	start := b.remaining - min(b.remaining, 255)
	if steps > 0 && steps < start {
		s.floor = start - steps
	}
	if s.parent != nil {
		s.floor = max(s.floor, s.parent.floor)
	}
	if duration > 0 {
		b.ctx, s.cancel = context.WithTimeout(b.ctx, duration)
		b.canCancel = true
	}
	b.scope = s
	c.Context = b.ctx
	return s
}

// BorrowRecords opens a transaction-local record scope inside this budget.
func (s *BudgetScope) BorrowRecords(c *Call) *RecordScope {
	b := c.budget
	if s.owner != b || b.scope != s || s.records != nil {
		panic("inactive or already bound record scope")
	}
	s.previousRecords = b.records
	b.ctx, s.records = WithRecords(b.ctx)
	b.records, c.Context = s.records, b.ctx
	return s.records
}

// End checks the section before restoring the parent limits. Call it before
// committing external effects. An exhausted scope also fails the invocation.
func (s *BudgetScope) End(c *Call) error {
	b := c.budget
	if s == nil || s.owner != b || b.scope != s {
		return errors.New("budget scopes must end in reverse order on the same invocation")
	}
	err := b.check(b.remaining)
	if err == nil {
		err = deadlineErr(b.ctx)
	}
	s.release()
	c.Context = b.ctx
	if err != nil {
		b.err = err
	}
	return err
}

func (s *BudgetScope) release() {
	b := s.owner
	if s.records != nil {
		s.records.Close()
		b.records, s.records, s.previousRecords = s.previousRecords, nil, nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	b.ctx, b.scope = s.previous, s.parent
	b.canCancel = b.ctx.Done() != nil
	s.owner, s.parent, s.previous, s.cancel = nil, nil, nil, nil
}

func (b *executionBudget) check(remaining uint64) error {
	if b.scope != nil && b.scope.floor != 0 && remaining < b.scope.floor {
		return ErrSteps
	}
	return b.contextErr()
}

// Also check the clock at completion: the deadline timer's goroutine may not
// have run yet when a short external call returns.
func deadlineErr(ctx context.Context) error {
	if d, ok := ctx.Deadline(); ok && !time.Now().Before(d) {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

// NarrowCPUBudget starts an adjustable time scope. Enclosing limits still apply.
// Memory is charged to the invocation, including across transaction scopes.
func (c *Call) NarrowCPUBudget(soft, hard time.Duration) *BudgetScope {
	if soft <= 0 || hard < soft {
		panic("invalid CPU budgets")
	}
	s := c.NarrowBudget(0, hard)
	s.started, s.duration, s.hardDuration = time.Now(), soft, hard
	return s
}

// IncreaseCPUBudget adds wall time to the current scope, capped by the host.
func (c *Call) IncreaseCPUBudget(ms uint64) error {
	b := c.budget
	if err := b.contextErr(); err != nil {
		return err
	}
	soft, hard := &b.duration, b.hardDuration
	if b.scope != nil {
		soft, hard = &b.scope.duration, b.scope.hardDuration
	}
	if hard == 0 {
		return nil
	}
	remaining := max(0, hard-*soft)
	if ms > uint64(remaining/time.Millisecond) {
		*soft = hard
	} else {
		*soft += time.Duration(ms) * time.Millisecond
	}
	return nil
}

// IncreaseMemBudget raises the invocation allowance without resetting usage.
func (c *Call) IncreaseMemBudget(bytes, allocs uint64) error {
	b := c.budget
	if err := b.contextErr(); err != nil {
		return err
	}
	b.limits.AllocationBytes += min(bytes, b.hardBytes-b.limits.AllocationBytes)
	b.limits.Allocations += min(allocs, b.hardAllocs-b.limits.Allocations)
	return nil
}
