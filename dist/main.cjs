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
		const redis_script_keys = [this._redis_key, `${this._redis_key}:${token}`];
		const redis_script_arguments = [String(Date.now()), String(ttl)];
		if (Array.isArray(ids)) redis_script_arguments.push(...ids.map(String));
		else redis_script_arguments.push(...[...ids].map(String));
		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			if (await this._redisClient.EVAL(`
					local ts_ms_now = tonumber(ARGV[1])

					redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])

					local ttl_ms = ARGV[2]
					local ids = { unpack(ARGV, 3) }

					local args_zadd = {}
					for _, id in ipairs(ids) do
						if redis.call('ZSCORE', KEYS[1], id) ~= false then
							return 0
						end

						table.insert(args_zadd, ts_ms_now + ttl_ms)
						table.insert(args_zadd, id)
					end

					-- set its to general table
					redis.call('ZADD', KEYS[1], unpack(args_zadd))

					-- set ids to lock token
					redis.call('SADD', KEYS[2], unpack(ids))
					redis.call('PEXPIRE', KEYS[2], ttl_ms)

					return 1
				`, {
				keys: redis_script_keys,
				arguments: redis_script_arguments
			}) === 1) return new RedisMultiLock(this, token);
			if (try_id + 1 < this.#retry_count) await asyncTimeout(this.#retry_interval);
		}
		throw new RedisLockerAcquireError();
	}
};
var RedisMultiLock = class {
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
	* @param time - Time to extend in milliseconds; a positive safe integer.
	* @returns -
	* @throws {RangeError} If the extension duration is not a positive safe integer.
	*/
	async extend(time) {
		validateDuration(time);
		if (await this.#locker._redisClient.EVAL(`
				local time_ms = tonumber(ARGV[1])
				local ts_ms_expire_old = redis.call('PEXPIRETIME', KEYS[2])
				if ts_ms_expire_old < 0 then
					return 0
				end
				redis.call('PEXPIREAT', KEYS[2], ts_ms_expire_old + time_ms)

				local ids = redis.call('SMEMBERS', KEYS[2])
				for _, id in ipairs(ids) do
					redis.call('ZINCRBY', KEYS[1], time_ms, id)
				end

				return 1
			`, {
			keys: [this.#locker._redis_key, `${this.#locker._redis_key}:${this.#token}`],
			arguments: [String(time)]
		}) === 0) throw new RedisLockerAcquireError();
	}
	/**
	* Releases lock.
	* @returns -
	*/
	async release() {
		if (await this.#locker._redisClient.EVAL(`
				if redis.call('EXISTS', KEYS[2]) == 0 then
					return 0
				end

				local ids = redis.call('SMEMBERS', KEYS[2])
				redis.call('DEL', KEYS[2])
				redis.call('ZREM', KEYS[1], unpack(ids))

				return 1
			`, { keys: [this.#locker._redis_key, `${this.#locker._redis_key}:${this.#token}`] }) === 0) throw new RedisLockerReleaseError();
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
