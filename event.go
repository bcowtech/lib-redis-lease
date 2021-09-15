package lease

type Event struct {
	Action    string
	Sink      string
	Workspace string
	LeaseID   string
	ExpireAt  Timestamp
	Timestamp Timestamp
}
