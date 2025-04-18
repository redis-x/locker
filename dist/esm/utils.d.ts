import type { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from 'redis';
export declare const REDIS_PREFIX = "@x:locker:";
export type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;
/**
 * Resolves after a given time.
 * @param ms Time in milliseconds.
 * @returns -
 */
export declare function asyncTimeout(ms: number): Promise<unknown>;
