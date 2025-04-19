import { type RedisClient } from '../utils.js';
export declare class RedisMultiLocker {
    readonly redisClient: RedisClient;
    readonly redis_key: string;
    private ttl;
    private retry_interval;
    private retry_count;
    /**
     * @param redisClient - Redis client (from `@kirick/redis-client` package)
     * @param namespace - Namespace to create keys in.
     * @param options -
     * @param options.ttl - Default time to live in milliseconds.
     * @param options.retry_interval - Interval between lock aquisition attempts in milliseconds.
     * @param options.retry_count - Maximum number of lock aquisition attempts.
     */
    constructor(redisClient: RedisClient, namespace: string, options?: {
        ttl?: number;
        retry_interval?: number;
        retry_count?: number;
    });
    /**
     * @param ids - IDs to lock in the namespace.
     * @param ttl Time to live in milliseconds.
     * @returns Lock object.
     */
    lock(ids: (string | number)[] | Set<string | number>, ttl?: number): Promise<RedisMultiLock>;
}
export declare class RedisMultiLock {
    private locker;
    private token;
    /**
     * @param locker RedisLocker instance.
     * @param token Lock token.
     */
    constructor(locker: RedisMultiLocker, token: string);
    /**
     * Extends lock time.
     * @param time - Time to extend in milliseconds.
     * @returns -
     */
    extend(time: number): Promise<void>;
    /**
     * Releases lock.
     * @returns -
     */
    release(): Promise<void>;
}
