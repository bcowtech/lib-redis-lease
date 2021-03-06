package internal

import (
	"os"
	"testing"
	"time"

	"github.com/bcowtech/lib-redis-lease/internal/helper"
	"github.com/go-redis/redis/v7"
)

func TestLeaseProvider_Put(t *testing.T) {
	client, err := helper.CreateRedisUniversalClient(&redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
	})
	if err != nil {
		t.Fatal(err)
	}

	p := new(LeaseProvider)
	p.Init(client)
	{
		ok, err := p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
		if err != nil {
			t.Fatal(err)
		}
		var expectedOK bool = true
		if ok != expectedOK {
			t.Errorf("expect %v, but got %v", expectedOK, ok)
		}
	}

	// duplicated operation
	{
		ok, err := p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
		if err != nil {
			t.Fatal(err)
		}
		var expectedOK bool = false
		if ok != expectedOK {
			t.Errorf("expect %v, but got %v", expectedOK, ok)
		}
	}

	client.Del("op/lease", "lease-1")
}

func TestLeaseProvider_Get(t *testing.T) {
	client, err := helper.CreateRedisUniversalClient(&redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
	})
	if err != nil {
		t.Fatal(err)
	}

	p := new(LeaseProvider)
	p.Init(client)
	_, err = p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	lease, err := p.Get("op/lease", "lease-1")
	if err != nil {
		t.Fatal(err)
	}

	if lease == nil {
		t.Errorf("Lease should not be nil")
	}

	if lease != nil {
		var expectedKey string = "lease-1"
		if lease.ID != expectedKey {
			t.Errorf("Lease.Key: expect %v, but got %v", expectedKey, lease.ID)
		}
		var expectedTTL time.Duration = 300 * time.Millisecond
		if lease.TTL != expectedTTL {
			t.Errorf("Lease.TTL: expect %v, but got %v", expectedTTL, lease.TTL)
		}
		var expectedTimestamp Timestamp = 1631116984000
		if lease.Timestamp != expectedTimestamp {
			t.Errorf("Lease.Timestamp: expect %v, but got %v", expectedTimestamp, lease.Timestamp)
		}
		if lease.ExpireAt == nil {
			t.Errorf("Lease.ExpireAt: should not be nil")
		} else {
			var expectedExpireAt Timestamp = 1631116984300
			if *lease.ExpireAt != expectedExpireAt {
				t.Errorf("Lease.ExpireAt: expect %v, but got %v", expectedExpireAt, *lease.ExpireAt)
			}
		}
	}

	client.Del("op/lease", "lease-1")
}

func TestLeaseProvider_Delete(t *testing.T) {
	client, err := helper.CreateRedisUniversalClient(&redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
	})
	if err != nil {
		t.Fatal(err)
	}

	p := new(LeaseProvider)
	p.Init(client)
	_, err = p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	{
		ok, err := p.Delete("op/lease", "lease-1")
		if err != nil {
			t.Fatal(err)
		}

		var expectedOK bool = true
		if ok != expectedOK {
			t.Errorf("expect %v, but got %v", expectedOK, ok)
		}
	}

	// duplicated operation
	{
		ok, err := p.Delete("op/lease", "lease-1")
		if err != nil {
			t.Fatal(err)
		}

		var expectedOK bool = false
		if ok != expectedOK {
			t.Errorf("expect %v, but got %v", expectedOK, ok)
		}
	}

	client.Del("op/lease", "lease-1")
}

func TestLeaseProvider_Renew(t *testing.T) {
	client, err := helper.CreateRedisUniversalClient(&redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
	})
	if err != nil {
		t.Fatal(err)
	}

	p := new(LeaseProvider)
	p.Init(client)
	_, err = p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	{
		expireAt, err := p.Renew("op/lease", "lease-1", time.Date(2021, 9, 8, 16, 3, 4, int(150*time.Millisecond), time.UTC))
		if err != nil {
			t.Fatal(err)
		}

		var expectedExpireAt Timestamp = 1631116984450
		if expireAt != expectedExpireAt {
			t.Errorf("expect %v, but got %v", expectedExpireAt, expireAt)
		}
	}
	{
		expireAt, err := p.Renew("op/lease", "lease-1", time.Date(2021, 9, 8, 16, 3, 4, int(150*time.Millisecond), time.UTC))
		if err != nil {
			t.Fatal(err)
		}

		var expectedExpireAt Timestamp = 1631116984450
		if expireAt != expectedExpireAt {
			t.Errorf("expect %v, but got %v", expectedExpireAt, expireAt)
		}
	}

	client.Del("op/lease", "lease-1")
}

func TestLeaseProvider_Expire(t *testing.T) {
	client, err := helper.CreateRedisUniversalClient(&redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
	})
	if err != nil {
		t.Fatal(err)
	}

	p := new(LeaseProvider)
	p.Init(client)
	_, err = p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	expired, err := p.Expire("op/lease", "op/lease/events",
		time.Date(2021, 9, 8, 16, 3, 4, int(301*time.Millisecond), time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	var expectedExpired int64 = 1
	if expired != expectedExpired {
		t.Errorf("expect %v, but got %v", expectedExpired, expired)
	}

	client.Del("op/lease/events", "op/lease", "lease-1")
}

func TestLeaseProvider_Expire_WithLimit(t *testing.T) {
	client, err := helper.CreateRedisUniversalClient(&redis.UniversalOptions{
		Addrs: []string{os.Getenv("REDIS_SERVER")},
	})
	if err != nil {
		t.Fatal(err)
	}

	p := new(LeaseProvider)
	p.Init(client)
	_, err = p.Put("op/lease", "lease-1", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Put("op/lease", "lease-2", 300*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Put("op/lease", "lease-3", 400*time.Millisecond, time.Date(2021, 9, 8, 16, 3, 4, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	var optArgs = []*LeaseArg{
		{
			Name:  "LIMIT",
			Value: 1,
		},
	}
	expired, err := p.Expire("op/lease", "op/lease/events",
		time.Date(2021, 9, 8, 16, 3, 4, int(300*time.Millisecond), time.UTC),
		optArgs...)
	if err != nil {
		t.Fatal(err)
	}

	var expectedExpired int64 = 1
	if expired != expectedExpired {
		t.Errorf("expect %v, but got %v", expectedExpired, expired)
	}

	client.Del("op/lease/events", "op/lease",
		"lease-1",
		"lease-2",
		"lease-3")
}
