package lease

import (
	"github.com/bcowtech/lib-redis-lease/internal/helper"
)

func WithLimit(limit int) *LeaseArg {
	return &LeaseArg{
		Name:  "LIMIT",
		Value: limit,
	}
}

func CreateRedisUniversalClient(opt *RedisOption) (RedisClient, error) {
	return helper.CreateRedisUniversalClient(opt)
}
