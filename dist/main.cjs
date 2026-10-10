Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
let node_crypto = require("node:crypto");
//#region src/error.ts
var RedisLockerAcquireError = class extends Error {
	constructor() {
		super("Failed to acquire lock");
	}
};
var RedisLockerReleaseError = class extends Error {
	constructor() {
		super("Failed to release lock");
	}
};
//#endregion
//#region src/utils.ts
const REDIS_PREFIX = "@x:locker:";
/**
* Validates a TTL or extension duration before sending it to Redis.
* @param duration Time in milliseconds.
* @throws {RangeError} If the duration is not a positive safe integer.
*/
function validateDuration(duration) {
	if (!Number.isSafeInteger(duration) || duration <= 0) throw new RangeError("Duration must be a positive safe integer in milliseconds");
}
/**
* Resolves after a given time.
* @param ms Time in milliseconds.
* @returns -
*/
function asyncTimeout(ms) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}
//#endregion
//#region src/locker/multi.ts
var RedisMultiLocker = class {
	#ttl;
	#retry_interval;
	#retry_count;
	/** @internal */
	_redisClient;
	/** @internal */
	_redis_key;
	/**
	* Creates a multi-resource locker. Requires Redis server 7.4 or newer.
	* @param redisClient - Redis client from the `redis` package.
	* @param namespace - Namespace to create keys in.
	* @param options -
	* @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	* @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
	* @param options.retry_count - Maximum number of lock aquisition attempts.
	* @throws {RangeError} If the default TTL is not a positive safe integer.
	*/
	constructor(redisClient, namespace, options) {
		this.#ttl = (options === null || options === void 0 ? void 0 : options.ttl) === void 0 ? 5e3 : options.ttl;
		validateDuration(this.#ttl);
		this.#retry_interval = (options === null || options === void 0 ? void 0 : options.retry_interval) ?? 100;
		this.#retry_count = (options === null || options === void 0 ? void 0 : options.retry_count) ?? 10;
		this._redisClient = redisClient;
		this._redis_key = REDIS_PREFIX + namespace;
	}
	/**
	* @param ids - IDs to lock in the namespace; an array or Set with 1–100 elements.
	* Duplicate array elements count toward the limit.
	* @param ttl Time to live in milliseconds; a positive safe integer.
	* @returns Lock object.
	* @throws {RangeError} If the ID count is outside 1–100 or the TTL is not a positive safe integer.
	*/
	async lock(ids, ttl = this.#ttl) {
		validateDuration(ttl);
		const id_count = Array.isArray(ids) ? ids.length : ids.size;
		if (id_count < 1 || id_count > 100) throw new RangeError("Multi-lock requests must contain between 1 and 100 IDs");
		const token = (0, node_crypto.randomBytes)(32).toString("base64").replaceAll(/[/+]/gu, "").slice(0, 16);
		const lock_ids = [...new Set(Array.from(ids, String))];
		const redis_script_arguments = [
			token,
			String(ttl),
			...lock_ids
		];
		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			if (await this._redisClient.EVAL(`
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
				`, {
				keys: [this._redis_key],
				arguments: redis_script_arguments
			}) === 1) return new RedisMultiLock(this, token, lock_ids);
			if (try_id + 1 < this.#retry_count) await asyncTimeout(this.#retry_interval);
		}
		throw new RedisLockerAcquireError();
	}
};
var RedisMultiLock = class {
	#locker;
	#token;
	#ids;
	/**
	* @param locker RedisMultiLocker instance.
	* @param token Lock token.
	* @param ids Normalized resource IDs acquired with this token.
	*/
	constructor(locker, token, ids) {
		this.#locker = locker;
		this.#token = token;
		this.#ids = [...ids];
	}
	/**
	* Extends lock time.
	* @param time - Time to extend in milliseconds; a positive safe integer.
	* @returns -
	* @throws {RangeError} If the extension duration is not a positive safe integer.
	*/
	async extend(time) {
		validateDuration(time);
		if (await this.#locker._redisClient.EVAL(`
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
			`, {
			keys: [this.#locker._redis_key],
			arguments: [
				this.#token,
				String(time),
				...this.#ids
			]
		}) === 0) throw new RedisLockerAcquireError();
	}
	/**
	* Releases lock.
	* @returns -
	*/
	async release() {
		if (await this.#locker._redisClient.EVAL(`
				local ids = { unpack(ARGV, 2) }
				local tokens = redis.call('HMGET', KEYS[1], unpack(ids))
				for _, token in ipairs(tokens) do
					if token ~= ARGV[1] then
						return 0
					end
				end

				redis.call('HDEL', KEYS[1], unpack(ids))

				return 1
			`, {
			keys: [this.#locker._redis_key],
			arguments: [this.#token, ...this.#ids]
		}) === 0) throw new RedisLockerReleaseError();
	}
};
//#endregion
//#region src/locker/single.ts
var RedisLocker = class {
	#ttl;
	#retry_interval;
	#retry_count;
	/** @internal */
	_redisClient;
	/** @internal */
	_redis_key;
	/**
	* @param redisClient Redis client from `redis` package.
	* @param key Key to lock.
	* @param options -
	* @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	* @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
	* @param options.retry_count - Maximum number of lock aquisition attempts.
	* @throws {RangeError} If the default TTL is not a positive safe integer.
	*/
	constructor(redisClient, key, options) {
		this.#ttl = (options === null || options === void 0 ? void 0 : options.ttl) === void 0 ? 5e3 : options.ttl;
		validateDuration(this.#ttl);
		this.#retry_interval = (options === null || options === void 0 ? void 0 : options.retry_interval) ?? 100;
		this.#retry_count = (options === null || options === void 0 ? void 0 : options.retry_count) ?? 10;
		this._redisClient = redisClient;
		this._redis_key = REDIS_PREFIX + key;
	}
	/**
	* Tries to aquire a lock. If not successful, throws an error.
	* @param ttl - Time to live in milliseconds; a positive safe integer.
	* @returns Lock object.
	* @throws {RangeError} If the TTL is not a positive safe integer.
	*/
	async lock(ttl = this.#ttl) {
		validateDuration(ttl);
		const token = (0, node_crypto.randomBytes)(16).toString("base64");
		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			if (await this._redisClient.SET(this._redis_key, token, {
				NX: true,
				PX: ttl
			}) === "OK") return new RedisLock(this, token);
			if (try_id + 1 < this.#retry_count) await asyncTimeout(this.#retry_interval);
		}
		throw new RedisLockerAcquireError();
	}
};
var RedisLock = class {
	#locker;
	#token;
	/**
	* @param locker RedisLocker instance.
	* @param token Lock token.
	*/
	constructor(locker, token) {
		this.#locker = locker;
		this.#token = token;
	}
	/**
	* Extends lock time.
	* @param time Time to extend in milliseconds; a positive safe integer.
	* @returns -
	* @throws {RangeError} If the extension duration is not a positive safe integer.
	*/
	async extend(time) {
		validateDuration(time);
		if (await this.#locker._redisClient.EVAL("local pttl = redis.call(\"PTTL\", KEYS[1]) if pttl > 0 and redis.call(\"GET\", KEYS[1]) == ARGV[1] then return redis.call(\"PEXPIRE\", KEYS[1], pttl + tonumber(ARGV[2])) else return 0 end", {
			keys: [this.#locker._redis_key],
			arguments: [this.#token, String(time)]
		}) === 0) throw new RedisLockerAcquireError();
	}
	/**
	* Releases lock.
	* @returns -
	*/
	async release() {
		if (await this.#locker._redisClient.EVAL("if redis.call(\"GET\", KEYS[1]) == ARGV[1] then return redis.call(\"DEL\", KEYS[1]) else return 0 end", {
			keys: [this.#locker._redis_key],
			arguments: [this.#token]
		}) === 0) throw new RedisLockerReleaseError();
	}
};
//#endregion
exports.RedisLock = RedisLock;
exports.RedisLocker = RedisLocker;
exports.RedisLockerAcquireError = RedisLockerAcquireError;
exports.RedisLockerReleaseError = RedisLockerReleaseError;
exports.RedisMultiLock = RedisMultiLock;
exports.RedisMultiLocker = RedisMultiLocker;
