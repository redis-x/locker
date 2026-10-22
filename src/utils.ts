import { RedisXClient } from '@redis-x/client';
import type {
	RedisClientType,
	RedisFunctions,
	RedisModules,
	RedisScripts,
} from 'redis';
export const REDIS_PREFIX = '@x:locker:';
// setTimeout fires almost immediately when the delay is larger than this.
const MAX_TIMEOUT = 2_147_483_647;

export type RedisClient = RedisClientType<
	RedisModules,
	RedisFunctions,
	RedisScripts
>;

const REDIS_X_CLIENTS = new WeakMap<RedisClient, RedisXClient>();

/**
 * Returns the RedisX client to run commands with, wrapping each `redis` client only once.
 * @param client Client from the `redis` package or a RedisX client.
 * @returns RedisX client.
 */
export function getRedisXClient(
	client: RedisClient | RedisXClient,
): RedisXClient {
	// Checked by shape, so a RedisX client from another package copy is not wrapped again.
	if ('createScript' in client) {
		return client;
	}

	let redisXClient = REDIS_X_CLIENTS.get(client);
	if (!redisXClient) {
		redisXClient = new RedisXClient(client);
		REDIS_X_CLIENTS.set(client, redisXClient);
	}

	return redisXClient;
}

/**
 * Validates a TTL or extension duration before sending it to Redis.
 * @param duration Time in milliseconds.
 * @throws {RangeError} If the duration is not a positive safe integer.
 */
export function validateDuration(duration: number): void {
	if (!Number.isSafeInteger(duration) || duration <= 0) {
		throw new RangeError(
			'Duration must be a positive safe integer in milliseconds',
		);
	}
}

/**
 * Validates lock acquisition retry options.
 * @param retry_interval Interval between attempts in milliseconds.
 * @param retry_count Maximum number of attempts.
 * @throws {RangeError} If the interval is not an integer from 0 to 2147483647 or the count is not a positive safe integer.
 */
export function validateRetryOptions(
	retry_interval: number,
	retry_count: number,
): void {
	if (
		!Number.isSafeInteger(retry_interval)
		|| retry_interval < 0
		|| retry_interval > MAX_TIMEOUT
	) {
		throw new RangeError(
			`Retry interval must be an integer from 0 to ${MAX_TIMEOUT} milliseconds`,
		);
	}

	if (!Number.isSafeInteger(retry_count) || retry_count <= 0) {
		throw new RangeError('Retry count must be a positive safe integer');
	}
}

/**
 * Resolves after a given time.
 * @param ms Time in milliseconds.
 * @returns -
 */
export function asyncTimeout(ms: number): Promise<void> {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}
