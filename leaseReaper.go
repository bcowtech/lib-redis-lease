package lease

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bcowtech/lib-redis-lease/internal"
	"github.com/bcowtech/lib-redis-lease/internal/helper"
)

type LeaseReaper struct {
	PollingTimeout time.Duration
	IdlingTimeout  time.Duration
	RedisOption    *RedisOption
	ErrorHandler   ErrorHandleProc

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	maxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	minRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	maxRetryBackoff time.Duration

	provider  *internal.LeaseProvider
	executors []*LeaseExpireExecutor
	hooks     []LeaseReaperHook

	existedWorkspaces []string

	ctx      context.Context
	stopChan chan bool
	wg       sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (r *LeaseReaper) Init() {
	if r.initialized {
		return
	}

	if r.stopChan == nil {
		r.stopChan = make(chan bool, 1)
	}

	r.provider = new(internal.LeaseProvider)

	r.ctx = context.TODO()

	{
		r.maxRetries = r.RedisOption.MaxRetries
		r.maxRetryBackoff = r.RedisOption.MaxRetryBackoff
		r.minRetryBackoff = r.RedisOption.MinRetryBackoff

		if r.maxRetries == -1 {
			r.maxRetries = 0
		}
		switch r.minRetryBackoff {
		case -1:
			r.minRetryBackoff = 0
		case 0:
			r.minRetryBackoff = 8 * time.Millisecond
		}
		switch r.maxRetryBackoff {
		case -1:
			r.maxRetryBackoff = 0
		case 0:
			r.maxRetryBackoff = 512 * time.Millisecond
		}

		r.RedisOption.MaxRetries = 0
		r.RedisOption.MaxRetryBackoff = -1
		r.RedisOption.MinRetryBackoff = -1
	}

	r.initialized = true
	return
}

func (r *LeaseReaper) AddExpiryContracts(contracts ...*LeaseExpiryContract) error {
	if !r.initialized {
		logger.Panic("the LeaseReaper haven't be initialized yet")
	}

	for _, contract := range contracts {
		if found := r.isDuplicatedWorkspace(contract.Workspace); found {
			return fmt.Errorf("specified workspace '%s' is duplicated", contract.Workspace)
		}
		r.executors = append(r.executors, contract.createExpireExecutor(r.provider))
	}

	return nil
}

func (r *LeaseReaper) AddHook(hook LeaseReaperHook) {
	r.hooks = append(r.hooks, hook)
}

func (r *LeaseReaper) Start() error {
	if r.disposed {
		logger.Panic("the LeaseReaper has been disposed")
	}
	if !r.initialized {
		logger.Panic("the LeaseReaper haven't be initialized yet")
	}
	if r.running {
		logger.Panic("the LeaseReaper is running")
	}

	var err error
	r.mutex.Lock()
	defer func() {
		if err != nil {
			r.running = false
			r.disposed = true
		}
		r.mutex.Unlock()
	}()
	r.running = true

	var (
		pollingTimeout time.Duration = r.PollingTimeout
		idlingTimeout  time.Duration = r.IdlingTimeout
		redisClient    *RedisClient
	)

	// redisClient
	{
		client, err := CreateRedisClient(r.RedisOption)
		if err != nil {
			return err
		}
		r.provider.Init(client)
		redisClient = client
	}

	timer := time.NewTimer(pollingTimeout)

	go func() {
		r.triggerOnStart()
		r.wg.Add(1)
		defer func() {
			r.wg.Done()
			r.triggerOnStop()
		}()
		defer redisClient.Close()
		defer func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}()

		for {
			select {
			case <-r.stopChan:
				return

			case next := <-timer.C:
				count, err := r.removeExpiredLeases(next)
				if err != nil {
					if !r.processRedisError(err) {
						logger.Fatalf("%% Error: %v\n", err)
						return
					}
				}

				if count > 0 {
					timer.Reset(pollingTimeout)
				} else {
					timer.Reset(idlingTimeout)
				}
			}
		}
	}()

	return nil
}

func (r *LeaseReaper) Stop() {
	if r.disposed {
		return
	}

	r.mutex.Lock()
	defer func() {
		r.running = false
		r.disposed = true

		r.mutex.Unlock()
	}()

	if r.stopChan != nil {
		r.stopChan <- true
		close(r.stopChan)
	}

	r.wg.Wait()
}

func (r *LeaseReaper) isDuplicatedWorkspace(workspace string) bool {
	var (
		existed = r.existedWorkspaces
	)

	found := sort.SearchStrings(existed, workspace)
	if found < len(existed) && existed[found] == workspace {
		return true
	}

	existed = append(existed, "")
	copy(existed[found+1:], existed[found:])
	existed[found] = workspace

	return false
}

func (r *LeaseReaper) processRedisError(err error) (disposed bool) {
	if r.ErrorHandler != nil {
		return r.ErrorHandler(err)
	}
	return false
}

func (r *LeaseReaper) removeExpiredLeases(timestamp time.Time) (count int64, err error) {
	var (
		total           int64         = 0
		attempts        int           = r.maxRetries
		maxRetryBackoff time.Duration = r.maxRetryBackoff
		minRetryBackoff time.Duration = r.minRetryBackoff
		paused          bool          = false
		lastErr         error
	)
	for _, v := range r.executors {
		// reset the paused flag
		paused = false
		r.triggerOnProcess(v.workspace, v.eventSink, timestamp)
		for attempt := 0; attempt <= attempts; attempt++ {
			expired, err := v.Execute(timestamp)
			total = total + expired
			if err == nil {
				if paused {
					paused = false
					r.triggerOnResume(v.workspace, v.eventSink)
				}
				break
			}

			if helper.IsRetriableError(err, true) {
				if !paused {
					paused = true
					r.triggerOnPause(v.workspace, v.eventSink)
				}

				if err := helper.Sleep(r.ctx, helper.RetryBackoff(attempts, minRetryBackoff, maxRetryBackoff)); err != nil {
					return total, err
				}
				continue
			}
			lastErr = err
			break
		}
	}
	return total, lastErr
}

func (r *LeaseReaper) triggerOnProcess(workspace, eventSink string, timestamp time.Time) {
	for _, h := range r.hooks {
		h.OnProcess(workspace, eventSink, timestamp)
	}
}

func (r *LeaseReaper) triggerOnPause(workspace, eventSink string) {
	for _, h := range r.hooks {
		h.OnPause(workspace, eventSink)
	}
}

func (r *LeaseReaper) triggerOnResume(workspace, eventSink string) {
	for _, h := range r.hooks {
		h.OnResume(workspace, eventSink)
	}
}

func (r *LeaseReaper) triggerOnStart() {
	for _, h := range r.hooks {
		h.OnStart()
	}
}

func (r *LeaseReaper) triggerOnStop() {
	for _, h := range r.hooks {
		h.OnStop()
	}
}
