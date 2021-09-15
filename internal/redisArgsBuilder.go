package internal

type RedisArgsBuilder []interface{}

func (list RedisArgsBuilder) Pack(args ...interface{}) RedisArgsBuilder {
	for _, elem := range args {
		if v, ok := elem.(Unpacker); ok {
			list = append(list, v.Unpack()...)
		} else {
			list = append(list, elem)
		}

	}
	return list
}

func (list RedisArgsBuilder) NamedArguments(args ...*LeaseArg) RedisArgsBuilder {
	for _, elem := range args {
		list = append(list, elem.Unpack()...)
	}
	return list
}

func (list RedisArgsBuilder) Unpack() []interface{} {
	return list
}
