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

export class RedisXMultiLocker {
	#ttl: number;
	#retry_interval: number;
	#retry_count: number;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_scripts: Scripts;
	/** @internal */
	// eslint-disable-next-line unicorn/prefer-private-class-fields
	_redis_key: string;

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
	constructor(
		redisClient: RedisClient | RedisXClient,
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

		this._scripts = getScripts(getRedisXClient(redisClient));
		this._redis_key = REDIS_PREFIX + namespace;
	}

	/**
	 * @param ids - IDs to lock in the namespace; an array or Set with at least one unique ID after conversion to strings.
	 * @param ttl Time to live in milliseconds; a positive safe integer.
	 * @returns Lock object.
	 * @throws {RangeError} If there are no IDs or the TTL is not a positive safe integer.
	 */
	async lock(
		ids: (string | number)[] | Set<string | number>,
		ttl: number = this.#ttl,
	): Promise<RedisXMultiLock> {
		validateDuration(ttl);
		const id_set = new Set(Array.from(ids, String));
		if (id_set.size === 0) {
			throw new RangeError('Multi-lock requests must contain at least one ID.');
		}

		const token = randomBytes(16).toString('base64');

		for (let try_id = 0; try_id < this.#retry_count; try_id++) {
			// oxlint-disable-next-line no-await-in-loop
			const result = await this._scripts.multi_lock.executeWithKeys(
				[this._redis_key],
				token,
				String(ttl),
				...id_set,
			);

			if (result === 1) {
				return new RedisXMultiLock(this, token, [...id_set]);
			}

			if (try_id + 1 < this.#retry_count) {
				// oxlint-disable-next-line no-await-in-loop
				await asyncTimeout(this.#retry_interval);
			}
		}

		throw new RedisXLockerAcquireError();
	}
}

export class RedisXMultiLock {
	#locker: RedisXMultiLocker;
	#token: string;
	#ids: string[];

	/**
	 * @param locker RedisXMultiLocker instance.
	 * @param token Lock token.
	 * @param ids Normalized resource IDs acquired with this token.
	 */
	constructor(
		locker: RedisXMultiLocker,
		token: string,
		ids: readonly string[],
	) {
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
		const result = await this.#locker._scripts.multi_extend.executeWithKeys(
			[this.#locker._redis_key],
			this.#token,
			String(time),
			...this.#ids,
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
		const result = await this.#locker._scripts.multi_release.executeWithKeys(
			[this.#locker._redis_key],
			this.#token,
			...this.#ids,
		);

		if (result === 0 && !is_silent) {
			throw new RedisXLockerReleaseError();
		}
	}
}
