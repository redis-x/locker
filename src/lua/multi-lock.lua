-- KEYS[1]: namespace hash
-- ARGV[1]: lock token, ARGV[2]: TTL in milliseconds, ARGV[3...]: IDs

-- Redis 8+: HSETEX with FNX and PX can replace this script.

-- unpack() can't spread more than ~8000 values, so commands get IDs in packs.
local id_packs = {}
for index = 3, #ARGV, 1000 do
	table.insert(id_packs, { unpack(ARGV, index, math.min(index + 999, #ARGV)) })
end

-- Read before writing so Redis versions without field TTLs fail safely.
for _, ids in ipairs(id_packs) do
	local ttls = redis.call('HPTTL', KEYS[1], 'FIELDS', #ids, unpack(ids))
	for _, ttl in ipairs(ttls) do
		if ttl ~= -2 then
			return 0
		end
	end
end

for _, ids in ipairs(id_packs) do
	local fields = {}
	for _, id in ipairs(ids) do
		table.insert(fields, id)
		table.insert(fields, ARGV[1])
	end
	redis.call('HSET', KEYS[1], unpack(fields))

	local result = redis.pcall('HPEXPIRE', KEYS[1], ARGV[2], 'FIELDS', #ids, unpack(ids))
	if result.err then
		-- Lua errors do not roll back writes. All IDs were checked as free, so this removes only our new fields.
		for _, written_ids in ipairs(id_packs) do
			redis.call('HDEL', KEYS[1], unpack(written_ids))
		end
		return result
	end
end

return 1
