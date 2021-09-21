package internal

import (
	"bytes"
	"time"

	redis "github.com/go-redis/redis/v7"
	"github.com/tinylib/msgp/msgp"
)

type LeaseProvider struct {
	handle *redis.Client
}

func (p *LeaseProvider) Init(client *redis.Client) {
	if client == nil {
		panic("specified redis.Client cannot be nil")
	}

	p.handle = client
}

func (p *LeaseProvider) Put(workspace, lease string, ttl time.Duration, timestamp time.Time) (ok bool, err error) {
	var (
		ttl_ms       int64 = ttl.Milliseconds()
		timestamp_ms int64 = timestamp.UnixNano() / int64(time.Millisecond)
	)

	if ttl_ms > 0 {
		cmd := p.handle.Eval(`
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
return redis.status_reply("NOP")`, []string{workspace, lease}, ttl_ms, timestamp_ms)

		reply, err := cmd.Result()
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
	cmd := p.handle.Eval(`
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
return RESULT`, []string{workspace, lease})

	reply, err := cmd.Result()
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
	cmd := p.handle.Eval(`
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
return redis.status_reply("NOP")`, []string{workspace, lease})

	reply, err := cmd.Result()
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

	cmd := p.handle.Eval(`
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
return RESULT`, []string{workspace, lease}, timestamp_ms)

	reply, err := cmd.Result()
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

	cmd := p.handle.Eval(`
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
return RESULT`, []string{workspace, sink}, redisArgs(timestamp_ms).NamedArguments(options...)...)

	reply, err := cmd.Result()
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
