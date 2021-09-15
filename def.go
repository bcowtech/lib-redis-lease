package lease

import (
	"log"
	"os"

	"github.com/bcowtech/lib-redis-lease/internal"
	stream "github.com/bcowtech/lib-redis-stream"
	redis "github.com/go-redis/redis/v7"
)

const (
	LOGGER_PREFIX string = "[bcowtech/lib-redis-lease] "
)

var (
	logger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

// stuct & interface
type (
	Lease     = internal.Lease
	Timestamp = internal.Timestamp
	LeaseArg  = internal.LeaseArg

	RedisOption          = redis.Options
	RedisErrorHandleProc = stream.RedisErrorHandleProc
	StreamOffset         = stream.StreamOffset
)

// func
type (
	EventHandleProc func(ev *Event) error
)
