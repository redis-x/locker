-- KEYS[1]: lock key
-- ARGV[1]: lock token, ARGV[2]: extension in milliseconds
local pttl = redis.call('PTTL', KEYS[1])
if pttl > 0 and redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('PEXPIRE', KEYS[1], pttl + tonumber(ARGV[2]))
end

return 0
