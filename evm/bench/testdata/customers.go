package guest

import "m"

func ReadCustomers(tx m.Context, _ uint64) uint64 {
	var sum uint64
	rows := m.Customer.Scan(tx)
	for c := rows.Next(); c != nil; c = rows.Next() {
		if c.active {
			sum += c.profile.balance*c.profile.rate + c.id
		}
	}
	return sum
}

func update(ctx m.Context, apply func(m.Context) uint64) uint64 {
	ctx.BeginWrite()
	sum := apply(ctx)
	ctx.Commit()
	return sum
}

func WriteCustomers(ctx m.Context, revision uint64) uint64 {
	return update(ctx, func(tx m.Context) uint64 {
		var sum uint64
		rows := m.Customer.Scan(tx)
		for c := rows.Next(); c != nil; c = rows.Next() {
			if !c.active {
				continue
			}
			edit := m.Customer.Edit(tx, c.id)
			edit.revision = revision
			edit.profile.balance = c.id*edit.profile.rate + revision
			tx.Put(c.id, edit)
			sum += edit.profile.balance
		}
		return sum
	})
}
