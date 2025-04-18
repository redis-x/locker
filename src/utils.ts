import type {
	RedisClientType,
	RedisFunctions,
	RedisModules,
	RedisScripts,
} from 'redis';
export const REDIS_PREFIX = '@x:locker:';

export type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;

/**
 * Resolves after a given time.
 * @param ms Time in milliseconds.
 * @returns -
 */
export function asyncTimeout(ms: number) {
	return new Promise((resolve) => {
		setTimeout(resolve, ms);
	});
}
