package lease

func WithLimit(limit int) *LeaseArg {
	return &LeaseArg{
		Name:  "LIMIT",
		Value: limit,
	}
}
