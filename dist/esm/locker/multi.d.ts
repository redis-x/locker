import { type RedisClient } from '../utils.js';
export declare class RedisMultiLocker {
    readonly redisClient: RedisClient;
    readonly redis_key: string;
    private ttl_ms;
    private retry_interval_ms;
    private retry_count;
    /**
     * @param redisClient - Redis client (from `@kirick/redis-client` package)
     * @param namespace - Namespace to create keys in.
     * @param options -
     * @param options.ttl_ms - Default time to live in milliseconds.
     * @param options.retry_interval_ms - Interval between lock aquisition attempts in milliseconds.
     * @param options.retry_count - Maximum number of lock aquisition attempts.
     */
    constructor(redisClient: RedisClient, namespace: string, options?: {
        ttl_ms?: number;
        retry_interval_ms?: number;
        retry_count?: number;
    });
    /**
     * @param ids - IDs to lock in the namespace.
     * @param ttl_ms Time to live in milliseconds.
     * @returns Lock object.
     */
    lock(ids: (string | number)[] | Set<string | number>, ttl_ms?: number): Promise<RedisMultiLock>;
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
     * @param time_ms - Time to extend in milliseconds.
     * @returns -
     */
    extend(time_ms: number): Promise<void>;
    /**
     * Releases lock.
     * @returns -
     */
    release(): Promise<void>;
}
