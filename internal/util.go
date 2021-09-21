package internal

func redisArgs(args ...interface{}) RedisArgsBuilder {
	var arg RedisArgsBuilder
	return arg.Pack(args...)
}
