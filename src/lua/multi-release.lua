-- KEYS[1]: namespace hash
-- ARGV[1]: lock token, ARGV[2...]: IDs
local ids = { unpack(ARGV, 2) }
local tokens = redis.call('HMGET', KEYS[1], unpack(ids))
for _, token in ipairs(tokens) do
	if token ~= ARGV[1] then
		return 0
	end
end

redis.call('HDEL', KEYS[1], unpack(ids))

return 1
