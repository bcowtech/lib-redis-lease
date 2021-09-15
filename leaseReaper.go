package lease

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bcowtech/lib-redis-lease/internal"
	"github.com/go-redis/redis/v7"
)

type LeaseReaper struct {
	PollingTimeout time.Duration
	IdlingTimeout  time.Duration
	RedisOption    *RedisOption
	ErrorHandler   RedisErrorHandleProc

	provider  *internal.LeaseProvider
	executors []*LeaseExpireExecutor

	existedWorkspaces []string

	stopChan chan bool
	wg       sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
	abandoned   bool
}

func (r *LeaseReaper) Init() {
	if r.initialized {
		return
	}

	if r.stopChan == nil {
		r.stopChan = make(chan bool, 1)
	}

	r.provider = new(internal.LeaseProvider)

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

func (r *LeaseReaper) Start() error {
	if r.disposed {
		logger.Panic("the LeaseReaper has been disposed")
	}
	if r.abandoned {
		logger.Panic("the LeaseReaper has been termimated")
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
		redisClient    *redis.Client
	)

	// redisClient
	{
		client, err := internal.CreateRedisClient(r.RedisOption)
		if err != nil {
			return err
		}
		r.provider.Init(client)
		redisClient = client
	}

	timer := time.NewTimer(pollingTimeout)

	go func() {
		r.wg.Add(1)
		defer r.wg.Done()
		defer redisClient.Close()
		defer func() {
			if !timer.Stop() {
				<-timer.C
			}
		}()

		for {
			select {
			case <-r.stopChan:
				return

			case next := <-timer.C:
				count, err := r.removeExpiredLeases(next)
				fmt.Printf("%+v\n", next)
				if err != nil {
					if !r.processRedisError(err) {
						logger.Fatalf("%% Error: %v\n", err)
						return
					}
				}
				fmt.Printf("count: %d\n", count)

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
		total int64 = 0
	)
	for _, v := range r.executors {
		expired, err := v.Execute(timestamp)
		total = total + expired
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
