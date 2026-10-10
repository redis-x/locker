import { randomBytes } from "node:crypto";
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
	* @param redisClient - Redis client (from `@kirick/redis-client` package)
	* @param namespace - Namespace to create keys in.
	* @param options -
	* @param options.ttl - Default time to live in milliseconds.
	* @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
	* @param options.retry_count - Maximum number of lock aquisition attempts.
	*/
	constructor(redisClient, namespace, options) {
		this.#ttl = (options === null || options === void 0 ? void 0 : options.ttl) ?? 5e3;
		this.#retry_interval = (options === null || options === void 0 ? void 0 : options.retry_interval) ?? 100;
		this.#retry_count = (options === null || options === void 0 ? void 0 : options.retry_count) ?? 10;
		this._redisClient = redisClient;
		this._redis_key = REDIS_PREFIX + namespace;
	}
	/**
	* @param ids - IDs to lock in the namespace.
	* @param ttl Time to live in milliseconds.
	* @returns Lock object.
	*/
	async lock(ids, ttl = this.#ttl) {
		const token = randomBytes(32).toString("base64").replaceAll(/[/+]/gu, "").slice(0, 16);
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
			await asyncTimeout(this.#retry_interval);
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
	* @param time - Time to extend in milliseconds.
	* @returns -
	*/
	async extend(time) {
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
	* @param options.ttl - Default time to live in milliseconds.
	* @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
	* @param options.retry_count - Maximum number of lock aquisition attempts.
	*/
	constructor(redisClient, key, options) {
		this.#ttl = (options === null || options === void 0 ? void 0 : options.ttl) ?? 5e3;
		this.#retry_interval = (options === null || options === void 0 ? void 0 : options.retry_interval) ?? 100;
		this.#retry_count = (options === null || options === void 0 ? void 0 : options.retry_count) ?? 10;
		this._redisClient = redisClient;
		this._redis_key = REDIS_PREFIX + key;
	}
	/**
	* Tries to aquire a lock. If not successful, throws an error.
	* @param ttl - Time to live in milliseconds.
	* @returns Lock object.
	*/
	async lock(ttl = this.#ttl) {
		const token = randomBytes(16).toString("base64");
		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			if (await this._redisClient.SET(this._redis_key, token, {
				NX: true,
				PX: ttl
			}) === "OK") return new RedisLock(this, token);
			await asyncTimeout(this.#retry_interval);
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
	* @param time Time to extend in milliseconds.
	* @returns -
	*/
	async extend(time) {
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
export { RedisLock, RedisLocker, RedisLockerAcquireError, RedisLockerReleaseError, RedisMultiLock, RedisMultiLocker };
