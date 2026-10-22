-- KEYS[1]: namespace hash
-- ARGV[1]: lock token, ARGV[2]: extension in milliseconds, ARGV[3...]: IDs
local ids = { unpack(ARGV, 3) }
local tokens = redis.call('HMGET', KEYS[1], unpack(ids))
for _, token in ipairs(tokens) do
	if token ~= ARGV[1] then
		return 0
	end
end

local ttls = redis.call('HPTTL', KEYS[1], 'FIELDS', #ids, unpack(ids))
local remaining_ms = math.huge
for _, ttl in ipairs(ttls) do
	if ttl <= 0 then
		return 0
	end
	remaining_ms = math.min(remaining_ms, ttl)
end
redis.call('HPEXPIRE', KEYS[1], remaining_ms + tonumber(ARGV[2]), 'FIELDS', #ids, unpack(ids))

return 1
