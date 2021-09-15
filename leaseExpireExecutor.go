package lease

import (
	"time"

	"github.com/bcowtech/lib-redis-lease/internal"
)

type LeaseExpireExecutor struct {
	workspace string
	eventSink string
	options   []*LeaseArg

	provider *internal.LeaseProvider
}

func (e *LeaseExpireExecutor) Execute(timestamp time.Time) (count int64, err error) {
	var (
		workspace = e.workspace
		sink      = e.eventSink
		options   = e.options
	)

	expired, err := e.provider.Expire(workspace, sink, timestamp, options...)
	if err != nil {
		return 0, err
	}
	return expired, nil
}
