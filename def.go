package lease

import (
	"log"
	"os"
	"time"

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

	RedisClient  = redis.Client
	RedisOption  = redis.Options
	StreamOffset = stream.StreamOffset

	LeaseReaperHook interface {
		OnPause(workspace, eventSink string)
		OnProcess(workspace, eventSink string, timestamp time.Time)
		OnResume(workspace, eventSink string)
		OnStart()
		OnStop()
	}
)

// func
type (
	ErrorHandleProc = stream.RedisErrorHandleProc

	EventHandleProc func(ev *Event) error
)
