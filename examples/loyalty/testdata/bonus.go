package rules

import "m"

// BonusPoints proposes a bonus; the host decides whether to award it.
func BonusPoints(customer *m.Customer) int64 {
	if customer == nil {
		return 0
	}
	balance := customer.points_earned - customer.points_redeemed
	if balance <= 0 {
		return 0
	}
	for _, tag := range customer.tags {
		if tag == "vip" {
			return balance / 10
		}
	}
	return 0
}
