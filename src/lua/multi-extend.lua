-- KEYS[1]: namespace hash
-- ARGV[1]: lock token, ARGV[2]: extension in milliseconds, ARGV[3...]: IDs

-- unpack() can't spread more than ~8000 values, so commands get IDs in packs.
local id_packs = {}
for index = 3, #ARGV, 1000 do
	table.insert(id_packs, { unpack(ARGV, index, math.min(index + 999, #ARGV)) })
end

local remaining_ms = math.huge
for _, ids in ipairs(id_packs) do
	local tokens = redis.call('HMGET', KEYS[1], unpack(ids))
	for _, token in ipairs(tokens) do
		if token ~= ARGV[1] then
			return 0
		end
	end

	local ttls = redis.call('HPTTL', KEYS[1], 'FIELDS', #ids, unpack(ids))
	for _, ttl in ipairs(ttls) do
		if ttl <= 0 then
			return 0
		end
		remaining_ms = math.min(remaining_ms, ttl)
	end
end

for _, ids in ipairs(id_packs) do
	redis.call('HPEXPIRE', KEYS[1], remaining_ms + tonumber(ARGV[2]), 'FIELDS', #ids, unpack(ids))
end

return 1
