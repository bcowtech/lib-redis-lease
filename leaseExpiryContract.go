package lease

import "github.com/bcowtech/lib-redis-lease/internal"

type LeaseExpiryContract struct {
	Workspace   string
	EventSink   string
	MaxInFlight int
}

func (c *LeaseExpiryContract) createExpireExecutor(provider *internal.LeaseProvider) *LeaseExpireExecutor {
	if len(c.Workspace) == 0 {
		logger.Panic("'Workspace' cannot be an empty string")
	}
	if len(c.EventSink) == 0 {
		logger.Panic("'EventSink' cannot be an empty string")
	}
	if provider == nil {
		logger.Panic("specified argument 'provider' cannot be nil")
	}

	var (
		options []*internal.LeaseArg
	)
	if c.MaxInFlight > 0 {
		options = append(options, WithLimit(c.MaxInFlight))
	}

	return &LeaseExpireExecutor{
		workspace: c.Workspace,
		eventSink: c.EventSink,
		options:   options,
		provider:  provider,
	}
}
