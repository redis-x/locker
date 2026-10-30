Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
let node_crypto = require("node:crypto");
let _redis_x_client = require("@redis-x/client");
//#region src/error.ts
var RedisXLockerAcquireError = class extends Error {
	constructor() {
		super("Failed to acquire lock");
	}
};
var RedisXLockerReleaseError = class extends Error {
	constructor() {
		super("Failed to release lock");
	}
};
//#endregion
//#region src/lua/multi-extend.lua
var multi_extend_default = "-- KEYS[1]: namespace hash\n-- ARGV[1]: lock token, ARGV[2]: extension in milliseconds, ARGV[3...]: IDs\nlocal ids = { unpack(ARGV, 3) }\nlocal tokens = redis.call('HMGET', KEYS[1], unpack(ids))\nfor _, token in ipairs(tokens) do\n	if token ~= ARGV[1] then\n		return 0\n	end\nend\n\nlocal ttls = redis.call('HPTTL', KEYS[1], 'FIELDS', #ids, unpack(ids))\nlocal remaining_ms = math.huge\nfor _, ttl in ipairs(ttls) do\n	if ttl <= 0 then\n		return 0\n	end\n	remaining_ms = math.min(remaining_ms, ttl)\nend\nredis.call('HPEXPIRE', KEYS[1], remaining_ms + tonumber(ARGV[2]), 'FIELDS', #ids, unpack(ids))\n\nreturn 1\n";
//#endregion
//#region src/lua/multi-lock.lua
var multi_lock_default = "-- KEYS[1]: namespace hash\n-- ARGV[1]: lock token, ARGV[2]: TTL in milliseconds, ARGV[3...]: IDs\n-- Redis 8+: HSETEX with FNX and PX can replace this script.\nlocal ids = { unpack(ARGV, 3) }\n-- Read before writing so Redis versions without field TTLs fail safely.\nlocal ttls = redis.call('HPTTL', KEYS[1], 'FIELDS', #ids, unpack(ids))\nfor _, ttl in ipairs(ttls) do\n	if ttl ~= -2 then\n		return 0\n	end\nend\n\nlocal fields = {}\nfor _, id in ipairs(ids) do\n	table.insert(fields, id)\n	table.insert(fields, ARGV[1])\nend\nredis.call('HSET', KEYS[1], unpack(fields))\n\nlocal result = redis.pcall('HPEXPIRE', KEYS[1], ARGV[2], 'FIELDS', #ids, unpack(ids))\nif result.err then\n	-- Lua errors do not roll back writes. Remove only our new fields.\n	redis.call('HDEL', KEYS[1], unpack(ids))\n	return result\nend\n\nreturn 1\n";
//#endregion
//#region src/lua/multi-release.lua
var multi_release_default = "-- KEYS[1]: namespace hash\n-- ARGV[1]: lock token, ARGV[2...]: IDs\nlocal ids = { unpack(ARGV, 2) }\nlocal tokens = redis.call('HMGET', KEYS[1], unpack(ids))\nfor _, token in ipairs(tokens) do\n	if token ~= ARGV[1] then\n		return 0\n	end\nend\n\nredis.call('HDEL', KEYS[1], unpack(ids))\n\nreturn 1\n";
//#endregion
//#region src/lua/single-extend.lua
var single_extend_default = "-- KEYS[1]: lock key\n-- ARGV[1]: lock token, ARGV[2]: extension in milliseconds\nlocal pttl = redis.call('PTTL', KEYS[1])\nif pttl > 0 and redis.call('GET', KEYS[1]) == ARGV[1] then\n	return redis.call('PEXPIRE', KEYS[1], pttl + tonumber(ARGV[2]))\nend\n\nreturn 0\n";
//#endregion
//#region src/lua/single-release.lua
var single_release_default = "-- KEYS[1]: lock key\n-- ARGV[1]: lock token\nif redis.call('GET', KEYS[1]) == ARGV[1] then\n	return redis.call('DEL', KEYS[1])\nend\n\nreturn 0\n";
//#endregion
//#region src/scripts.ts
const SCRIPTS = /* @__PURE__ */ new WeakMap();
/**
* Returns lock scripts for a RedisX client, creating them once per client.
* @param redisXClient RedisX client.
* @returns Lock scripts.
*/
function getScripts(redisXClient) {
	let scripts = SCRIPTS.get(redisXClient);
	if (!scripts) {
		scripts = {
			single_extend: redisXClient.createScript(single_extend_default),
			single_release: redisXClient.createScript(single_release_default),
			multi_lock: redisXClient.createScript(multi_lock_default),
			multi_extend: redisXClient.createScript(multi_extend_default),
			multi_release: redisXClient.createScript(multi_release_default)
		};
		SCRIPTS.set(redisXClient, scripts);
	}
	return scripts;
}
//#endregion
//#region src/utils.ts
const REDIS_PREFIX = "@x:locker:";
const MAX_TIMEOUT = 2147483647;
/**
* Validates a TTL or extension duration before sending it to Redis.
* @param duration Time in milliseconds.
* @throws {RangeError} If the duration is not a positive safe integer.
*/
function validateDuration(duration) {
	if (!Number.isSafeInteger(duration) || duration <= 0) throw new RangeError("Duration must be a positive safe integer in milliseconds");
}
/**
* Validates lock acquisition retry options.
* @param retry_interval Interval between attempts in milliseconds.
* @param retry_count Maximum number of attempts.
* @throws {RangeError} If the interval is not an integer from 0 to 2147483647 or the count is not a positive safe integer.
*/
function validateRetryOptions(retry_interval, retry_count) {
	if (!Number.isSafeInteger(retry_interval) || retry_interval < 0 || retry_interval > MAX_TIMEOUT) throw new RangeError(`Retry interval must be an integer from 0 to ${MAX_TIMEOUT} milliseconds`);
	if (!Number.isSafeInteger(retry_count) || retry_count <= 0) throw new RangeError("Retry count must be a positive safe integer");
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
var RedisXMultiLocker = class {
	#ttl;
	#retry_interval;
	#retry_count;
	/** @internal */
	_scripts;
	/** @internal */
	_redis_key;
	/**
	* Creates a multi-resource locker. Requires Redis server 7.4 or newer.
	* @param redisClient - Client from the `redis` package or a RedisX client.
	* @param namespace - Namespace to create keys in.
	* @param options -
	* @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	* @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
	* @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
	* @throws {RangeError} If the default TTL or a retry option is invalid.
	*/
	constructor(redisClient, namespace, options) {
		this.#ttl = (options === null || options === void 0 ? void 0 : options.ttl) === void 0 ? 5e3 : options.ttl;
		validateDuration(this.#ttl);
		this.#retry_interval = (options === null || options === void 0 ? void 0 : options.retry_interval) === void 0 ? 100 : options.retry_interval;
		this.#retry_count = (options === null || options === void 0 ? void 0 : options.retry_count) === void 0 ? 10 : options.retry_count;
		validateRetryOptions(this.#retry_interval, this.#retry_count);
		this._scripts = getScripts((0, _redis_x_client.getRedisXClient)(redisClient));
		this._redis_key = REDIS_PREFIX + namespace;
	}
	/**
	* @param ids - IDs to lock in the namespace; an array or Set with 1–100 unique IDs after conversion to strings.
	* @param ttl Time to live in milliseconds; a positive safe integer.
	* @returns Lock object.
	* @throws {RangeError} If the unique ID count is outside 1–100 or the TTL is not a positive safe integer.
	*/
	async lock(ids, ttl = this.#ttl) {
		validateDuration(ttl);
		const id_set = new Set(Array.from(ids, String));
		if (id_set.size === 0 || id_set.size > 100) throw new RangeError("Multi-lock requests must contain between 1 and 100 unique IDs");
		const lock_ids = [...id_set];
		const token = (0, node_crypto.randomBytes)(16).toString("base64");
		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			if (await this._scripts.multi_lock.executeWithKeys([this._redis_key], token, String(ttl), ...lock_ids) === 1) return new RedisXMultiLock(this, token, lock_ids);
			if (try_id + 1 < this.#retry_count) await asyncTimeout(this.#retry_interval);
		}
		throw new RedisXLockerAcquireError();
	}
};
var RedisXMultiLock = class {
	#locker;
	#token;
	#ids;
	/**
	* @param locker RedisXMultiLocker instance.
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
		if (await this.#locker._scripts.multi_extend.executeWithKeys([this.#locker._redis_key], this.#token, String(time), ...this.#ids) === 0) throw new RedisXLockerAcquireError();
	}
	/**
	* Releases lock.
	* @param is_silent Do not throw if the lock has expired or belongs to someone else.
	* @returns -
	*/
	async release(is_silent = false) {
		if (await this.#locker._scripts.multi_release.executeWithKeys([this.#locker._redis_key], this.#token, ...this.#ids) === 0 && !is_silent) throw new RedisXLockerReleaseError();
	}
};
//#endregion
//#region src/locker/single.ts
var RedisXLocker = class {
	#ttl;
	#retry_interval;
	#retry_count;
	/** @internal */
	_redisXClient;
	/** @internal */
	_scripts;
	/** @internal */
	_redis_key;
	/**
	* @param redisClient Client from the `redis` package or a RedisX client.
	* @param key Key to lock.
	* @param options -
	* @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	* @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
	* @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
	* @throws {RangeError} If the default TTL or a retry option is invalid.
	*/
	constructor(redisClient, key, options) {
		this.#ttl = (options === null || options === void 0 ? void 0 : options.ttl) === void 0 ? 5e3 : options.ttl;
		validateDuration(this.#ttl);
		this.#retry_interval = (options === null || options === void 0 ? void 0 : options.retry_interval) === void 0 ? 100 : options.retry_interval;
		this.#retry_count = (options === null || options === void 0 ? void 0 : options.retry_count) === void 0 ? 10 : options.retry_count;
		validateRetryOptions(this.#retry_interval, this.#retry_count);
		this._redisXClient = (0, _redis_x_client.getRedisXClient)(redisClient);
		this._scripts = getScripts(this._redisXClient);
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
			if (await this._redisXClient.SET(this._redis_key, token, {
				NX: true,
				PX: ttl
			}) === "OK") return new RedisXLock(this, token);
			if (try_id + 1 < this.#retry_count) await asyncTimeout(this.#retry_interval);
		}
		throw new RedisXLockerAcquireError();
	}
};
var RedisXLock = class {
	#locker;
	#token;
	/**
	* @param locker RedisXLocker instance.
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
		if (await this.#locker._scripts.single_extend.executeWithKeys([this.#locker._redis_key], this.#token, String(time)) === 0) throw new RedisXLockerAcquireError();
	}
	/**
	* Releases lock.
	* @param is_silent Do not throw if the lock has expired or belongs to someone else.
	* @returns -
	*/
	async release(is_silent = false) {
		if (await this.#locker._scripts.single_release.executeWithKeys([this.#locker._redis_key], this.#token) === 0 && !is_silent) throw new RedisXLockerReleaseError();
	}
};
//#endregion
exports.RedisXLock = RedisXLock;
exports.RedisXLocker = RedisXLocker;
exports.RedisXLockerAcquireError = RedisXLockerAcquireError;
exports.RedisXLockerReleaseError = RedisXLockerReleaseError;
exports.RedisXMultiLock = RedisXMultiLock;
exports.RedisXMultiLocker = RedisXMultiLocker;
