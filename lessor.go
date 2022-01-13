package lease

import (
	"time"

	"github.com/bcowtech/lib-redis-lease/internal"
)

type Lessor struct {
	RedisOption *RedisOption

	provider *internal.LeaseProvider
}

func (l *Lessor) Init() error {
	provider := new(internal.LeaseProvider)
	{
		client, err := CreateRedisUniversalClient(l.RedisOption)
		if err != nil {
			return err
		}
		provider.Init(client)
	}

	l.provider = provider

	return nil
}

func (l *Lessor) Grant(workspace string, lease Lease, timestamp time.Time) (ok bool, err error) {
	return l.provider.Put(workspace, lease.ID, lease.TTL, timestamp)
}

func (l *Lessor) KeepAlive(workspace, leaseKey string, timestamp time.Time) (Timestamp, error) {
	return l.provider.Renew(workspace, leaseKey, timestamp)
}

func (l *Lessor) Revoke(workspace, leaseKey string) (ok bool, err error) {
	return l.provider.Delete(workspace, leaseKey)
}

func (l *Lessor) Lease(workspace, leaseKey string) (*Lease, error) {
	return l.provider.Get(workspace, leaseKey)
}

func (l *Lessor) TimeToLive(workspace, leaseKey string) (*time.Duration, error) {
	lease, err := l.provider.Get(workspace, leaseKey)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		return lease.TimeToLive(), nil
	}
	return nil, nil
}
