import type {
	RedisClientType,
	RedisFunctions,
	RedisModules,
	RedisScripts,
} from 'redis';
export const REDIS_PREFIX = '@x:locker:';

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
 * Resolves after a given time.
 * @param ms Time in milliseconds.
 * @returns -
 */
export function asyncTimeout(ms: number): Promise<void> {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}
