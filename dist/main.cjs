"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// dist/esm/main.js
var main_exports = {};
__export(main_exports, {
  RedisLock: () => RedisLock,
  RedisLocker: () => RedisLocker,
  RedisLockerAcquireError: () => RedisLockerAcquireError,
  RedisLockerReleaseError: () => RedisLockerReleaseError,
  RedisMultiLock: () => RedisMultiLock,
  RedisMultiLocker: () => RedisMultiLocker
});
module.exports = __toCommonJS(main_exports);

// dist/esm/locker/single.js
var import_node_crypto = require("node:crypto");

// dist/esm/error.js
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

// dist/esm/utils.js
var REDIS_PREFIX = "@x:locker:";
function asyncTimeout(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// dist/esm/locker/single.js
var RedisLocker = class {
  redisClient;
  redis_key;
  ttl_ms;
  retry_interval_ms;
  retry_count;
  /**
   * @param redisClient Redis client from `redis` package.
   * @param key Key to lock.
   * @param options -
   * @param options.ttl_ms Default time to live in milliseconds.
   * @param options.retry_interval_ms Interval between lock aquisition attempts in milliseconds.
   * @param options.retry_count Maximum number of lock aquisition attempts.
   */
  constructor(redisClient, key, options) {
    this.redisClient = redisClient;
    this.redis_key = REDIS_PREFIX + key;
    this.ttl_ms = options?.ttl_ms ?? 5e3;
    this.retry_interval_ms = options?.retry_interval_ms ?? 100;
    this.retry_count = options?.retry_count ?? 10;
  }
  /**
   * Tries to aquire a lock. If not successful, throws an error.
   * @param ttl_ms Time to live in milliseconds.
   * @returns Lock object.
   */
  async lock(ttl_ms = this.ttl_ms) {
    const token = (0, import_node_crypto.randomBytes)(16).toString("base64");
    for (let try_id = 0; try_id < this.retry_count; try_id++) {
      const result = await this.redisClient.SET(this.redis_key, token, {
        NX: true,
        PX: ttl_ms
      });
      if (result === "OK") {
        return new RedisLock(this, token);
      }
      await asyncTimeout(this.retry_interval_ms);
    }
    throw new RedisLockerAcquireError();
  }
};
var RedisLock = class {
  locker;
  token;
  /**
   * @param locker RedisLocker instance.
   * @param token Lock token.
   */
  constructor(locker, token) {
    this.locker = locker;
    this.token = token;
  }
  /**
   * Extends lock time.
   * @param time_ms Time to extend in milliseconds.
   * @returns -
   */
  async extend(time_ms) {
    const result = await this.locker.redisClient.EVAL('local pttl = redis.call("PTTL", KEYS[1]) if pttl > 0 and redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], pttl + tonumber(ARGV[2])) else return 0 end', {
      keys: [
        this.locker.redis_key
      ],
      arguments: [
        this.token,
        String(time_ms)
      ]
    });
    if (result === 0) {
      throw new RedisLockerAcquireError();
    }
  }
  /**
   * Releases lock.
   * @returns -
   */
  async release() {
    const result = await this.locker.redisClient.EVAL('if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end', {
      keys: [
        this.locker.redis_key
      ],
      arguments: [
        this.token
      ]
    });
    if (result === 0) {
      throw new RedisLockerReleaseError();
    }
  }
};

// dist/esm/locker/multi.js
var import_node_crypto2 = require("node:crypto");
var RedisMultiLocker = class {
  redisClient;
  redis_key;
  ttl_ms;
  retry_interval_ms;
  retry_count;
  /**
   * @param redisClient - Redis client (from `@kirick/redis-client` package)
   * @param namespace - Namespace to create keys in.
   * @param options -
   * @param options.ttl_ms - Default time to live in milliseconds.
   * @param options.retry_interval_ms - Interval between lock aquisition attempts in milliseconds.
   * @param options.retry_count - Maximum number of lock aquisition attempts.
   */
  constructor(redisClient, namespace, options) {
    this.redisClient = redisClient;
    this.redis_key = REDIS_PREFIX + namespace;
    this.ttl_ms = options?.ttl_ms ?? 5e3;
    this.retry_interval_ms = options?.retry_interval_ms ?? 100;
    this.retry_count = options?.retry_count ?? 10;
  }
  /**
   * @param ids - IDs to lock in the namespace.
   * @param ttl_ms Time to live in milliseconds.
   * @returns Lock object.
   */
  async lock(ids, ttl_ms = this.ttl_ms) {
    const token = (0, import_node_crypto2.randomBytes)(32).toString("base64").replaceAll("/", "").replaceAll("+", "").slice(0, 16);
    const redis_script_keys = [
      this.redis_key,
      `${this.redis_key}:${token}`
    ];
    const redis_script_arguments = [
      String(Date.now()),
      String(ttl_ms)
    ];
    if (Array.isArray(ids)) {
      redis_script_arguments.push(...ids.map(String));
    } else {
      redis_script_arguments.push(...[...ids].map(String));
    }
    for (let try_id = 0; try_id < this.retry_count; try_id++) {
      const result = await this.redisClient.EVAL(`
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
      });
      if (result === 1) {
        return new RedisMultiLock(this, token);
      }
      await asyncTimeout(this.retry_interval_ms);
    }
    throw new RedisLockerAcquireError();
  }
};
var RedisMultiLock = class {
  locker;
  token;
  /**
   * @param locker RedisLocker instance.
   * @param token Lock token.
   */
  constructor(locker, token) {
    this.locker = locker;
    this.token = token;
  }
  /**
   * Extends lock time.
   * @param time_ms - Time to extend in milliseconds.
   * @returns -
   */
  async extend(time_ms) {
    const result = await this.locker.redisClient.EVAL(`
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
      keys: [
        this.locker.redis_key,
        `${this.locker.redis_key}:${this.token}`
      ],
      arguments: [
        String(time_ms)
      ]
    });
    if (result === 0) {
      throw new RedisLockerAcquireError();
    }
  }
  /**
   * Releases lock.
   * @returns -
   */
  async release() {
    const result = await this.locker.redisClient.EVAL(`
				if redis.call('EXISTS', KEYS[2]) == 0 then
					return 0
				end

				local ids = redis.call('SMEMBERS', KEYS[2])
				redis.call('DEL', KEYS[2])
				redis.call('ZREM', KEYS[1], unpack(ids))

				return 1
			`, {
      keys: [
        this.locker.redis_key,
        `${this.locker.redis_key}:${this.token}`
      ]
    });
    if (result === 0) {
      throw new RedisLockerReleaseError();
    }
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  RedisLock,
  RedisLocker,
  RedisLockerAcquireError,
  RedisLockerReleaseError,
  RedisMultiLock,
  RedisMultiLocker
});
