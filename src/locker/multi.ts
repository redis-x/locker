import { randomBytes } from 'node:crypto';
import { RedisLockerAcquireError, RedisLockerReleaseError } from '../error.js';
import { asyncTimeout, REDIS_PREFIX, type RedisClient } from '../utils.js';

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
	 * @param redisClient - Redis client (from `@kirick/redis-client` package)
	 * @param namespace - Namespace to create keys in.
	 * @param options -
	 * @param options.ttl - Default time to live in milliseconds.
	 * @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
	 * @param options.retry_count - Maximum number of lock aquisition attempts.
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
		this.#ttl = options?.ttl ?? 5000;
		this.#retry_interval = options?.retry_interval ?? 100;
		this.#retry_count = options?.retry_count ?? 10;

		this._redisClient = redisClient;
		this._redis_key = REDIS_PREFIX + namespace;
	}

	/**
	 * @param ids - IDs to lock in the namespace.
	 * @param ttl Time to live in milliseconds.
	 * @returns Lock object.
	 */
	async lock(
		ids: (string | number)[] | Set<string | number>,
		ttl: number = this.#ttl,
	): Promise<RedisMultiLock> {
		const token = randomBytes(32)
			.toString('base64')
			.replaceAll(/[/+]/gu, '')
			.slice(0, 16);

		const redis_script_keys = [this._redis_key, `${this._redis_key}:${token}`];

		const redis_script_arguments = [String(Date.now()), String(ttl)];
		if (Array.isArray(ids)) {
			redis_script_arguments.push(...ids.map(String));
		} else {
			redis_script_arguments.push(...[...ids].map(String));
		}

		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			// oxlint-disable-next-line no-await-in-loop
			const result = await this._redisClient.EVAL(
				`
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
				`,
				{
					keys: redis_script_keys,
					arguments: redis_script_arguments,
				},
			);

			if (result === 1) {
				return new RedisMultiLock(this, token);
			}

			// oxlint-disable-next-line no-await-in-loop
			await asyncTimeout(this.#retry_interval);
		}

		throw new RedisLockerAcquireError();
	}
}

export class RedisMultiLock {
	#locker: RedisMultiLocker;
	#token: string;

	/**
	 * @param locker RedisLocker instance.
	 * @param token Lock token.
	 */
	constructor(locker: RedisMultiLocker, token: string) {
		this.#locker = locker;
		this.#token = token;
	}

	/**
	 * Extends lock time.
	 * @param time - Time to extend in milliseconds.
	 * @returns -
	 */
	async extend(time: number): Promise<void> {
		const result = await this.#locker._redisClient.EVAL(
			`
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
			`,
			{
				keys: [
					this.#locker._redis_key,
					`${this.#locker._redis_key}:${this.#token}`,
				],
				arguments: [String(time)],
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
				if redis.call('EXISTS', KEYS[2]) == 0 then
					return 0
				end

				local ids = redis.call('SMEMBERS', KEYS[2])
				redis.call('DEL', KEYS[2])
				redis.call('ZREM', KEYS[1], unpack(ids))

				return 1
			`,
			{
				keys: [
					this.#locker._redis_key,
					`${this.#locker._redis_key}:${this.#token}`,
				],
			},
		);

		if (result === 0) {
			throw new RedisLockerReleaseError();
		}
	}
}
