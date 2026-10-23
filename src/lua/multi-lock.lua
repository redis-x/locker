-- KEYS[1]: namespace hash
-- ARGV[1]: lock token, ARGV[2]: TTL in milliseconds, ARGV[3...]: IDs
-- Redis 8+: HSETEX with FNX and PX can replace this script.
local ids = { unpack(ARGV, 3) }
-- Read before writing so Redis versions without field TTLs fail safely.
local ttls = redis.call('HPTTL', KEYS[1], 'FIELDS', #ids, unpack(ids))
for _, ttl in ipairs(ttls) do
	if ttl ~= -2 then
		return 0
	end
end

local fields = {}
for _, id in ipairs(ids) do
	table.insert(fields, id)
	table.insert(fields, ARGV[1])
end
redis.call('HSET', KEYS[1], unpack(fields))

local result = redis.pcall('HPEXPIRE', KEYS[1], ARGV[2], 'FIELDS', #ids, unpack(ids))
if result.err then
	-- Lua errors do not roll back writes. Remove only our new fields.
	redis.call('HDEL', KEYS[1], unpack(ids))
	return result
end

return 1
