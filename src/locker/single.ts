import { randomBytes } from 'node:crypto';
import {
	RedisLockerAcquireError,
	RedisLockerReleaseError,
} from '../error.js';
import {
	type RedisClient,
	REDIS_PREFIX,
	asyncTimeout,
} from '../utils.js';

export class RedisLocker {
	readonly redisClient: RedisClient;
	readonly redis_key: string;
	private ttl_ms: number;
	private retry_interval_ms: number;
	private retry_count: number;

	/**
	 * @param redisClient Redis client from `redis` package.
	 * @param key Key to lock.
	 * @param options -
	 * @param options.ttl_ms Default time to live in milliseconds.
	 * @param options.retry_interval_ms Interval between lock aquisition attempts in milliseconds.
	 * @param options.retry_count Maximum number of lock aquisition attempts.
	 */
	constructor(
		redisClient: RedisClient,
		key: string,
		options?: {
			ttl_ms?: number,
			retry_interval_ms?: number,
			retry_count?: number,
		},
	) {
		this.redisClient = redisClient;
		this.redis_key = REDIS_PREFIX + key;

		this.ttl_ms = options?.ttl_ms ?? 5000;
		this.retry_interval_ms = options?.retry_interval_ms ?? 100;
		this.retry_count = options?.retry_count ?? 10;
	}

	/**
	 * Tries to aquire a lock. If not successful, throws an error.
	 * @param ttl_ms Time to live in milliseconds.
	 * @returns Lock object.
	 */
	async lock(ttl_ms: number = this.ttl_ms): Promise<RedisLock> {
		const token = randomBytes(16).toString('base64');

		for (
			let try_id = 0;
			try_id < this.retry_count;
			try_id++
		) {
			// eslint-disable-next-line no-await-in-loop
			const result = await this.redisClient.SET(
				this.redis_key,
				token,
				{
					NX: true,
					PX: ttl_ms,
				},
			);

			if (result === 'OK') {
				return new RedisLock(this, token);
			}

			// eslint-disable-next-line no-await-in-loop
			await asyncTimeout(
				this.retry_interval_ms,
			);
		}

		throw new RedisLockerAcquireError();
	}
}

export class RedisLock {
	private locker: RedisLocker;
	private token: string;

	/**
	 * @param locker RedisLocker instance.
	 * @param token Lock token.
	 */
	constructor(locker: RedisLocker, token: string) {
		this.locker = locker;
		this.token = token;
	}

	/**
	 * Extends lock time.
	 * @param time_ms Time to extend in milliseconds.
	 * @returns -
	 */
	async extend(time_ms: number) {
		const result = await this.locker.redisClient.EVAL(
			'local pttl = redis.call("PTTL", KEYS[1]) if pttl > 0 and redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], pttl + tonumber(ARGV[2])) else return 0 end',
			{
				keys: [
					this.locker.redis_key,
				],
				arguments: [
					this.token,
					String(time_ms),
				],
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
	async release() {
		const result = await this.locker.redisClient.EVAL(
			'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end',
			{
				keys: [
					this.locker.redis_key,
				],
				arguments: [
					this.token,
				],
			},
		);

		if (result === 0) {
			throw new RedisLockerReleaseError();
		}
	}
}
