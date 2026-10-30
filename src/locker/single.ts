import { randomBytes } from 'node:crypto';
import { getRedisXClient, type RedisXClient } from '@redis-x/client';
import {
	RedisXLockerAcquireError,
	RedisXLockerReleaseError,
} from '../error.js';
import { getScripts, type Scripts } from '../scripts.js';
import {
	asyncTimeout,
	REDIS_PREFIX,
	type RedisClient,
	validateDuration,
	validateRetryOptions,
} from '../utils.js';

export class RedisXLocker {
	#ttl: number;
	#retry_interval: number;
	#retry_count: number;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_redisXClient: RedisXClient;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_scripts: Scripts;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_redis_key: string;

	/**
	 * @param redisClient Client from the `redis` package or a RedisX client.
	 * @param key Key to lock.
	 * @param options -
	 * @param options.ttl - Default time to live in milliseconds; a positive safe integer.
	 * @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
	 * @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
	 * @throws {RangeError} If the default TTL or a retry option is invalid.
	 */
	constructor(
		redisClient: RedisClient | RedisXClient,
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

		this._redisXClient = getRedisXClient(redisClient);
		this._scripts = getScripts(this._redisXClient);
		this._redis_key = REDIS_PREFIX + key;
	}

	/**
	 * Tries to aquire a lock. If not successful, throws an error.
	 * @param ttl - Time to live in milliseconds; a positive safe integer.
	 * @returns Lock object.
	 * @throws {RangeError} If the TTL is not a positive safe integer.
	 */
	async lock(ttl: number = this.#ttl): Promise<RedisXLock> {
		validateDuration(ttl);
		const token = randomBytes(16).toString('base64');

		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			// oxlint-disable-next-line no-await-in-loop
			const result = await this._redisXClient.SET(this._redis_key, token, {
				NX: true,
				PX: ttl,
			});

			if (result === 'OK') {
				return new RedisXLock(this, token);
			}

			if (try_id + 1 < this.#retry_count) {
				// oxlint-disable-next-line no-await-in-loop
				await asyncTimeout(this.#retry_interval);
			}
		}

		throw new RedisXLockerAcquireError();
	}
}

export class RedisXLock {
	#locker: RedisXLocker;
	#token: string;

	/**
	 * @param locker RedisXLocker instance.
	 * @param token Lock token.
	 */
	constructor(locker: RedisXLocker, token: string) {
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
		const result = await this.#locker._scripts.single_extend.executeWithKeys(
			[this.#locker._redis_key],
			this.#token,
			String(time),
		);

		if (result === 0) {
			throw new RedisXLockerAcquireError();
		}
	}

	/**
	 * Releases lock.
	 * @param is_silent Do not throw if the lock has expired or belongs to someone else.
	 * @returns -
	 */
	async release(is_silent = false): Promise<void> {
		const result = await this.#locker._scripts.single_release.executeWithKeys(
			[this.#locker._redis_key],
			this.#token,
		);

		if (result === 0 && !is_silent) {
			throw new RedisXLockerReleaseError();
		}
	}
}
