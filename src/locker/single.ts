import { randomBytes } from 'node:crypto';
import { RedisLockerAcquireError, RedisLockerReleaseError } from '../error.js';
import {
	asyncTimeout,
	REDIS_PREFIX,
	type RedisClient,
	validateDuration,
	validateRetryOptions,
} from '../utils.js';

export class RedisLocker {
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
	 * @param redisClient Redis client from `redis` package.
	 * @param key Key to lock.
	 * @param options -
	 * @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	 * @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
	 * @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
	 * @throws {RangeError} If the default TTL or a retry option is invalid.
	 */
	constructor(
		redisClient: RedisClient,
		key: string,
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
		this._redis_key = REDIS_PREFIX + key;
	}

	/**
	 * Tries to aquire a lock. If not successful, throws an error.
	 * @param ttl - Time to live in milliseconds; a positive safe integer.
	 * @returns Lock object.
	 * @throws {RangeError} If the TTL is not a positive safe integer.
	 */
	async lock(ttl: number = this.#ttl): Promise<RedisLock> {
		validateDuration(ttl);
		const token = randomBytes(16).toString('base64');

		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			// oxlint-disable-next-line no-await-in-loop
			const result = await this._redisClient.SET(this._redis_key, token, {
				NX: true,
				PX: ttl,
			});

			if (result === 'OK') {
				return new RedisLock(this, token);
			}

			if (try_id + 1 < this.#retry_count) {
				// oxlint-disable-next-line no-await-in-loop
				await asyncTimeout(this.#retry_interval);
			}
		}

		throw new RedisLockerAcquireError();
	}
}

export class RedisLock {
	#locker: RedisLocker;
	#token: string;

	/**
	 * @param locker RedisLocker instance.
	 * @param token Lock token.
	 */
	constructor(locker: RedisLocker, token: string) {
		this.#locker = locker;
		this.#token = token;
	}

	/**
	 * Extends lock time.
	 * @param time Time to extend in milliseconds; a positive safe integer.
	 * @returns -
	 * @throws {RangeError} If the extension duration is not a positive safe integer.
	 */
	async extend(time: number): Promise<void> {
		validateDuration(time);
		const result = await this.#locker._redisClient.EVAL(
			'local pttl = redis.call("PTTL", KEYS[1]) if pttl > 0 and redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], pttl + tonumber(ARGV[2])) else return 0 end',
			{
				keys: [this.#locker._redis_key],
				arguments: [this.#token, String(time)],
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
			'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end',
			{
				keys: [this.#locker._redis_key],
				arguments: [this.#token],
			},
		);

		if (result === 0) {
			throw new RedisLockerReleaseError();
		}
	}
}
