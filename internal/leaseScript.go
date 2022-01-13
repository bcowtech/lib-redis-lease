package internal

import (
	"fmt"

	redis "github.com/go-redis/redis/v7"
)

const (
	LEASE_LUA_PUT  = "put"
	LUA_SCRIPT_PUT = `
if #KEYS < 2 then
	return redis.error_reply("ILLEGAL_ARGUMENTS")
end

local WORKSPACE = KEYS[1]
local LEASE_ID  = KEYS[2]
local TTL       = tonumber(ARGV[1])
local TIMESTAMP = tonumber(ARGV[2])

if TIMESTAMP and TTL and LEASE_ID and WORKSPACE then
	if WORKSPACE == "" then  return redis.error_reply("INVALID_ARGUMENT")  end
	if LEASE_ID  == "" then  return redis.error_reply("INVALID_ARGUMENT")  end

	local LAST_UPDATE_AT
	local EXPIRE_AT = TIMESTAMP + TTL

	do
		local reply = redis.call('HGET', LEASE_ID, "timestamp")
		if type(reply)=='table' and reply.err then
			return reply
		end
		LAST_UPDATE_AT = tonumber(reply)
	end

	if not LAST_UPDATE_AT  or  TIMESTAMP > LAST_UPDATE_AT then
		do
			local reply = redis.call('ZADD', WORKSPACE, EXPIRE_AT, LEASE_ID)
			if type(reply)=='table' and reply.err then
				return reply
			end
		end

		do
			local reply = redis.call('HSET' , LEASE_ID
																			, "ttl"      , TTL
																			, "timestamp", TIMESTAMP)
			if type(reply)=='table' and reply.err then
				return reply
			end
		end

		return redis.status_reply("OK")
	end
end
return redis.status_reply("NOP")`

	LEASE_LUA_GET  = "get"
	LUA_SCRIPT_GET = `
if #KEYS < 2 then
	return redis.error_reply("ILLEGAL_ARGUMENTS")
end

local WORKSPACE = KEYS[1]
local LEASE_ID  = KEYS[2]

local RESULT
if LEASE_ID and WORKSPACE then
	if WORKSPACE == "" then  return redis.error_reply("INVALID_ARGUMENT")  end
	if LEASE_ID  == "" then  return redis.error_reply("INVALID_ARGUMENT")  end

	local TTL, TIMESTAMP, EXPIRE_AT

	do
		local reply = redis.call('HMGET', LEASE_ID
																		, "ttl"
																		, "timestamp")
		if type(reply)=='table' and reply.err then
			return reply
		end
		TTL, TIMESTAMP = unpack(reply)
	end

	do
		local reply = redis.call('ZSCORE', WORKSPACE, LEASE_ID)
		if type(reply)=='table' and reply.err then
			return reply
		end
		EXPIRE_AT = reply
	end

	do
		local result = {
			ttl       = tonumber(TTL),
			timestamp = tonumber(TIMESTAMP),
			expire_at = tonumber(EXPIRE_AT),
		}

		if next(result) then
			RESULT = cmsgpack.pack(result)
		end
	end
end
return RESULT`

	LEASE_LUA_DELETE  = "delete"
	LUA_SCRIPT_DELETE = `
if #KEYS < 2 then
	return redis.error_reply("ILLEGAL_ARGUMENTS")
end

local WORKSPACE = KEYS[1]
local LEASE_ID  = KEYS[2]

if LEASE_ID and WORKSPACE then
	if WORKSPACE == "" then  return redis.error_reply("INVALID_ARGUMENT")  end
	if LEASE_ID  == "" then  return redis.error_reply("INVALID_ARGUMENT")  end

	do
		local reply = redis.call('DEL', LEASE_ID)
		if type(reply)=='table' and reply.err then
			return reply
		end
	end

	do
		local reply = redis.call('ZREM', WORKSPACE, LEASE_ID)
		if type(reply)=='table' and reply.err then
			return reply
		end

		if tonumber(reply)  and  reply > 0 then
			return redis.status_reply("OK")
		end
	end
end
return redis.status_reply("NOP")`

	LEASE_LUA_RENEW  = "renew"
	LUA_SCRIPT_RENEW = `
if #KEYS < 1 then
	return redis.error_reply("ILLEGAL_ARGUMENTS")
end

local WORKSPACE = KEYS[1]
local LEASE_ID  = KEYS[2]
local TIMESTAMP = tonumber(ARGV[1])

local RESULT
if TIMESTAMP and LEASE_ID and WORKSPACE then
	if WORKSPACE == "" then  return redis.error_reply("INVALID_ARGUMENT")  end
	if LEASE_ID  == "" then  return redis.error_reply("INVALID_ARGUMENT")  end

	local TTL, LAST_UPDATE_AT

	do
		local reply = redis.call('HMGET', LEASE_ID
																		, "ttl"
																		, "timestamp")
		if type(reply)=='table' and reply.err then
		return reply
		end
		TTL, LAST_UPDATE_AT = unpack(reply)

		LAST_UPDATE_AT = tonumber(LAST_UPDATE_AT)
	end

	if TTL  and (not LAST_UPDATE_AT  or  TIMESTAMP > LAST_UPDATE_AT) then
		local expire_at = TIMESTAMP + TTL

		do
			local reply  = redis.call('HSET', LEASE_ID
																			, "timestamp", TIMESTAMP)
			if type(reply)=='table' and reply.err then
				return reply
			end
		end

		do
			local reply = redis.call('ZADD', WORKSPACE, expire_at, LEASE_ID)
			if type(reply)=='table' and reply.err then
				return reply
			end
			if reply then
				RESULT =  expire_at
			end
		end
	else
		local reply = redis.call('ZSCORE', WORKSPACE, LEASE_ID)
		if type(reply)=='table' and reply.err then
			return reply
		end
		if reply then
			RESULT = tonumber(reply)
		end
	end
end
return RESULT`

	LEASE_LUA_EXPIRE  = "expire"
	LUA_SCRIPT_EXPIRE = `
if #KEYS < 2 then
	return redis.error_reply("ILLEGAL_ARGUMENTS")
end

local WORKSPACE = KEYS[1]
local SINK      = KEYS[2]
local TIMESTAMP = tonumber(ARGV[1])

local LIMIT

if ARGV then
	if (#ARGV - 1) % 2 ~= 0 then
		return redis.error_reply("ILLEGAL_ARGUMENTS")
	end

	local ARGV_SETTER = {
		LIMIT = function(v) LIMIT  = tonumber(v) end,
	}

	for i = 2, #ARGV, 2 do
		local k = ARGV[i]
		local setter = ARGV_SETTER[k]
		if setter then
			local err = setter(ARGV[i+1])
			if err then
				return err
			end
		end
	end
end

local RESULT
if TIMESTAMP and SINK and WORKSPACE then
	if SINK    == "" then  return redis.error_reply("INVALID_ARGUMENT")  end
	if WORKSPACE == "" then  return redis.error_reply("INVALID_ARGUMENT")  end

	if not LIMIT  or  LIMIT == 0 then
		LIMIT = math.huge
	end

	local LEASE_REPLY, COUNT

	do
		local reply = redis.call('ZRANGEBYSCORE', WORKSPACE, '-inf', TIMESTAMP, 'WITHSCORES')
		if type(reply)=='table' and reply.err then
			return reply
		end

		if type(reply)=='table' then
			LEASE_REPLY = reply
		end
	end

	if LEASE_REPLY and #LEASE_REPLY > 0 then
		COUNT = 0

		for i = 1, #LEASE_REPLY, 2 do
			if COUNT >= LIMIT then
				break
			end

			local lease     = LEASE_REPLY[i]
			local expire_at = LEASE_REPLY[i+1]

			do
				local reply  = redis.call('XADD', SINK, '*'
																				, "action"   , 'EXPIRED'
																				, "workspace", WORKSPACE
																				, "lease"    , lease
																				, "expire_at", expire_at)
				if type(reply)=='table' and reply.err then
					return reply
				end
			end
			if expire_at then
				local reply  = redis.call('DEL', lease)
				if type(reply)=='table' and reply.err then
					return reply
				end
			end
			do
				local reply  = redis.call('ZREM', WORKSPACE, lease)
				if type(reply)=='table' and reply.err then
					return reply
				end
			end

			COUNT = COUNT + 1
		end
	end

	RESULT = COUNT or 0
end
return RESULT`
)

var (
	LeaseScriptInstance = new(LeaseScript)

	LeaseScriptList   map[string]string
	LeaseScriptIDList map[string]string
)

func init() {
	LeaseScriptList = map[string]string{
		LEASE_LUA_PUT:    LUA_SCRIPT_PUT,
		LEASE_LUA_GET:    LUA_SCRIPT_GET,
		LEASE_LUA_DELETE: LUA_SCRIPT_DELETE,
		LEASE_LUA_RENEW:  LUA_SCRIPT_RENEW,
		LEASE_LUA_EXPIRE: LUA_SCRIPT_EXPIRE,
	}

	LeaseScriptIDList = make(map[string]string)
}

type LeaseScript struct{}

func (s *LeaseScript) getScriptID(client redis.UniversalClient, name string) (string, error) {
	id, ok := LeaseScriptIDList[name]
	if !ok {
		return s.registerScript(client, name)
	}
	if len(id) == 0 {
		return s.registerScript(client, name)
	}
	reply, err := client.ScriptExists(id).Result()
	if err != nil {
		return "", err
	}
	if (len(reply) < 1) || (reply[0] == false) {
		return s.registerScript(client, name)
	}
	return id, nil
}

func (s *LeaseScript) registerScript(client redis.UniversalClient, name string) (string, error) {
	if script, ok := LeaseScriptList[name]; ok {
		id, err := client.ScriptLoad(script).Result()
		if err != nil {
			return "", err
		}
		// update script id list
		LeaseScriptIDList[name] = id
		return id, nil
	}
	return "", fmt.Errorf("cannot find script '%s'", name)
}

func (s *LeaseScript) Exec(client redis.UniversalClient, name string, keys []string, args ...interface{}) (interface{}, error) {
	if client == nil {
		panic("specified argument 'client' cannot be nil")
	}
	if len(name) == 0 {
		panic("specified argument 'name' cannot be an empty string")
	}

	id, err := s.getScriptID(client, name)
	if err != nil {
		return nil, err
	}
	return client.EvalSha(id, keys, args...).Result()
}
