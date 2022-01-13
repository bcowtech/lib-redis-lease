package helper

import (
	"fmt"

	redis "github.com/go-redis/redis/v7"
)

func CreateRedisUniversalClient(opt *redis.UniversalOptions) (redis.UniversalClient, error) {
	client := redis.NewUniversalClient(opt)
	if client == nil {
		return nil, fmt.Errorf("fail to create redis.Client")
	}

	_, err := client.Ping().Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	}
	return client, nil
}
