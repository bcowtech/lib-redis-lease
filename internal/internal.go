package internal

import (
	"fmt"

	redis "github.com/go-redis/redis/v7"
)

func CreateRedisClient(opt *redis.Options) (*redis.Client, error) {
	client := redis.NewClient(opt)
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

func RedisArgs(args ...interface{}) RedisArgsBuilder {
	var arg RedisArgsBuilder
	return arg.Pack(args...)
}
