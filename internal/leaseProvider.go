package internal

import (
	"bytes"
	"time"

	redis "github.com/go-redis/redis/v7"
	"github.com/tinylib/msgp/msgp"
)

type LeaseProvider struct {
	handle redis.UniversalClient
	script *LeaseScript
}

func (p *LeaseProvider) Init(client redis.UniversalClient) {
	if client == nil {
		panic("specified argument 'client' cannot be nil")
	}

	p.handle = client
	p.script = LeaseScriptInstance
}

func (p *LeaseProvider) Put(workspace, lease string, ttl time.Duration, timestamp time.Time) (ok bool, err error) {
	var (
		ttl_ms       int64 = ttl.Milliseconds()
		timestamp_ms int64 = timestamp.UnixNano() / int64(time.Millisecond)
	)

	if ttl_ms > 0 {
		reply, err := p.script.Exec(p.handle, LEASE_LUA_PUT, []string{workspace, lease}, ttl_ms, timestamp_ms)
		if err != nil {
			if err != redis.Nil {
				return false, err
			}
		}

		if v, ok := reply.(string); ok {
			return (v == "OK"), nil
		}
	}
	return false, nil
}

func (p *LeaseProvider) Get(workspace, lease string) (*Lease, error) {
	reply, err := p.script.Exec(p.handle, LEASE_LUA_GET, []string{workspace, lease})
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	}

	if reply != nil {
		result := &Lease{
			ID: lease,
		}
		reader := bytes.NewBuffer([]byte(reply.(string)))
		err := msgp.Decode(reader, result)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, nil
}

func (p *LeaseProvider) Delete(workspace, lease string) (ok bool, err error) {
	reply, err := p.script.Exec(p.handle, LEASE_LUA_DELETE, []string{workspace, lease})
	if err != nil {
		if err != redis.Nil {
			return false, err
		}
	}

	if v, ok := reply.(string); ok {
		return (v == "OK"), nil
	}
	return false, nil
}

func (p *LeaseProvider) Renew(workspace, lease string, timestamp time.Time) (Timestamp, error) {
	var (
		timestamp_ms int64 = timestamp.UnixNano() / int64(time.Millisecond)
	)

	reply, err := p.script.Exec(p.handle, LEASE_LUA_RENEW, []string{workspace, lease}, timestamp_ms)
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
	}

	if v, ok := reply.(int64); ok {
		return Timestamp(v), nil
	}
	return 0, nil
}

func (p *LeaseProvider) Expire(workspace, sink string, timestamp time.Time, options ...*LeaseArg) (count int64, err error) {
	var (
		timestamp_ms int64 = timestamp.UnixNano() / int64(time.Millisecond)
	)

	reply, err := p.script.Exec(p.handle, LEASE_LUA_EXPIRE, []string{workspace, sink}, redisArgs(timestamp_ms).NamedArguments(options...)...)
	if err != nil {
		if err != redis.Nil {
			return 0, err
		}
	}

	if v, ok := reply.(int64); ok {
		return v, nil
	}
	return 0, nil
}
