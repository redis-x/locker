import { type RedisClient } from '../utils.js';
export declare class RedisLocker {
    readonly redisClient: RedisClient;
    readonly redis_key: string;
    private ttl;
    private retry_interval;
    private retry_count;
    /**
     * @param redisClient Redis client from `redis` package.
     * @param key Key to lock.
     * @param options -
     * @param options.ttl - Default time to live in milliseconds.
     * @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
     * @param options.retry_count - Maximum number of lock aquisition attempts.
     */
    constructor(redisClient: RedisClient, key: string, options?: {
        ttl?: number;
        retry_interval?: number;
        retry_count?: number;
    });
    /**
     * Tries to aquire a lock. If not successful, throws an error.
     * @param ttl - Time to live in milliseconds.
     * @returns Lock object.
     */
    lock(ttl?: number): Promise<RedisLock>;
}
export declare class RedisLock {
    private locker;
    private token;
    /**
     * @param locker RedisLocker instance.
     * @param token Lock token.
     */
    constructor(locker: RedisLocker, token: string);
    /**
     * Extends lock time.
     * @param time Time to extend in milliseconds.
     * @returns -
     */
    extend(time: number): Promise<void>;
    /**
     * Releases lock.
     * @returns -
     */
    release(): Promise<void>;
}
