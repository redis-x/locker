-- KEYS[1]: namespace hash
-- ARGV[1]: lock token, ARGV[2...]: IDs

-- unpack() can't spread more than ~8000 values, so commands get IDs in packs.
local id_packs = {}
for index = 2, #ARGV, 1000 do
	table.insert(id_packs, { unpack(ARGV, index, math.min(index + 999, #ARGV)) })
end

for _, ids in ipairs(id_packs) do
	local tokens = redis.call('HMGET', KEYS[1], unpack(ids))
	for _, token in ipairs(tokens) do
		if token ~= ARGV[1] then
			return 0
		end
	end
end

for _, ids in ipairs(id_packs) do
	redis.call('HDEL', KEYS[1], unpack(ids))
end

return 1
