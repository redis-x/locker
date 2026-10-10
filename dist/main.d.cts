import { RedisClientType, RedisFunctions, RedisModules, RedisScripts } from "redis";
//#region src/error.d.ts
export declare class RedisLockerAcquireError extends Error {
  constructor();
}
export declare class RedisLockerReleaseError extends Error {
  constructor();
}
//#endregion
//#region src/utils.d.ts
type RedisClient = RedisClientType<RedisModules, RedisFunctions, RedisScripts>;
//#endregion
//#region src/locker/multi.d.ts
export declare class RedisMultiLocker {
  #private;
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
  #private;
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
//#endregion
//#region src/locker/single.d.ts
export declare class RedisLocker {
  #private;
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
  #private;
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
//#endregion
export {};