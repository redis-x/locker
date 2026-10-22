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
