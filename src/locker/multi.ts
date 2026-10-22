import { randomBytes } from 'node:crypto';
import { RedisLockerAcquireError, RedisLockerReleaseError } from '../error.js';
import {
	asyncTimeout,
	REDIS_PREFIX,
	type RedisClient,
	validateDuration,
	validateRetryOptions,
} from '../utils.js';

export class RedisMultiLocker {
	#ttl: number;
	#retry_interval: number;
	#retry_count: number;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_redisClient: RedisClient;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_redis_key: string;

	/**
	 * Creates a multi-resource locker. Requires Redis server 7.4 or newer.
	 * @param redisClient - Redis client from the `redis` package.
	 * @param namespace - Namespace to create keys in.
	 * @param options -
	 * @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	 * @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
	 * @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
	 * @throws {RangeError} If the default TTL or a retry option is invalid.
	 */
	constructor(
		redisClient: RedisClient,
		namespace: string,
		options?: {
			ttl?: number;
			retry_interval?: number;
			retry_count?: number;
		},
	) {
		this.#ttl = options?.ttl === undefined ? 5000 : options.ttl;
		validateDuration(this.#ttl);
		this.#retry_interval =
			options?.retry_interval === undefined ? 100 : options.retry_interval;
		this.#retry_count =
			options?.retry_count === undefined ? 10 : options.retry_count;
		validateRetryOptions(this.#retry_interval, this.#retry_count);

		this._redisClient = redisClient;
		this._redis_key = REDIS_PREFIX + namespace;
	}

	/**
	 * @param ids - IDs to lock in the namespace; an array or Set with 1–100 unique IDs after conversion to strings.
	 * @param ttl Time to live in milliseconds; a positive safe integer.
	 * @returns Lock object.
	 * @throws {RangeError} If the unique ID count is outside 1–100 or the TTL is not a positive safe integer.
	 */
	async lock(
		ids: (string | number)[] | Set<string | number>,
		ttl: number = this.#ttl,
	): Promise<RedisMultiLock> {
		validateDuration(ttl);
		const id_set = new Set(Array.from(ids, String));
		if (id_set.size === 0 || id_set.size > 100) {
			throw new RangeError(
				'Multi-lock requests must contain between 1 and 100 unique IDs',
			);
		}

		const lock_ids = [...id_set];
		const token = randomBytes(16).toString('base64');

		const redis_script_arguments = [token, String(ttl), ...lock_ids];

		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			// Redis 8+: HSETEX with FNX and PX can replace this acquisition script.
			// oxlint-disable-next-line no-await-in-loop
			const result = await this._redisClient.EVAL(
				`
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
				`,
				{
					keys: [this._redis_key],
					arguments: redis_script_arguments,
				},
			);

			if (result === 1) {
				return new RedisMultiLock(this, token, lock_ids);
			}

			if (try_id + 1 < this.#retry_count) {
				// oxlint-disable-next-line no-await-in-loop
				await asyncTimeout(this.#retry_interval);
			}
		}

		throw new RedisLockerAcquireError();
	}
}

export class RedisMultiLock {
	#locker: RedisMultiLocker;
	#token: string;
	#ids: string[];

	/**
	 * @param locker RedisMultiLocker instance.
	 * @param token Lock token.
	 * @param ids Normalized resource IDs acquired with this token.
	 */
	constructor(locker: RedisMultiLocker, token: string, ids: readonly string[]) {
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
	async extend(time: number): Promise<void> {
		validateDuration(time);
		const result = await this.#locker._redisClient.EVAL(
			`
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
			`,
			{
				keys: [this.#locker._redis_key],
				arguments: [this.#token, String(time), ...this.#ids],
			},
		);

		if (result === 0) {
			throw new RedisLockerAcquireError();
		}
	}

	/**
	 * Releases lock.
	 * @returns -
	 */
	async release(): Promise<void> {
		const result = await this.#locker._redisClient.EVAL(
			`
				local ids = { unpack(ARGV, 2) }
				local tokens = redis.call('HMGET', KEYS[1], unpack(ids))
				for _, token in ipairs(tokens) do
					if token ~= ARGV[1] then
						return 0
					end
				end

				redis.call('HDEL', KEYS[1], unpack(ids))

				return 1
			`,
			{
				keys: [this.#locker._redis_key],
				arguments: [this.#token, ...this.#ids],
			},
		);

		if (result === 0) {
			throw new RedisLockerReleaseError();
		}
	}
}
