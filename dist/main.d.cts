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
   * Creates a multi-resource locker. Requires Redis server 7.4 or newer.
   * @param redisClient - Redis client from the `redis` package.
   * @param namespace - Namespace to create keys in.
   * @param options -
   * @param options.ttl - Default time to live in milliseconds; a positive safe integer.
   * @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
   * @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
   * @throws {RangeError} If the default TTL or a retry option is invalid.
   */
  constructor(redisClient: RedisClient, namespace: string, options?: {
    ttl?: number;
    retry_interval?: number;
    retry_count?: number;
  });
  /**
   * @param ids - IDs to lock in the namespace; an array or Set with 1–100 unique IDs after conversion to strings.
   * @param ttl Time to live in milliseconds; a positive safe integer.
   * @returns Lock object.
   * @throws {RangeError} If the unique ID count is outside 1–100 or the TTL is not a positive safe integer.
   */
  lock(ids: (string | number)[] | Set<string | number>, ttl?: number): Promise<RedisMultiLock>;
}
export declare class RedisMultiLock {
  #private;
  /**
   * @param locker RedisMultiLocker instance.
   * @param token Lock token.
   * @param ids Normalized resource IDs acquired with this token.
   */
  constructor(locker: RedisMultiLocker, token: string, ids: readonly string[]);
  /**
   * Extends lock time.
   * @param time - Time to extend in milliseconds; a positive safe integer.
   * @returns -
   * @throws {RangeError} If the extension duration is not a positive safe integer.
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
   * @param options.ttl - Default time to live in milliseconds; a positive safe integer.
   * @param options.retry_interval - Interval between lock acquisition attempts in milliseconds; an integer from 0 to 2147483647.
   * @param options.retry_count - Maximum number of lock acquisition attempts; a positive safe integer.
   * @throws {RangeError} If the default TTL or a retry option is invalid.
   */
  constructor(redisClient: RedisClient, key: string, options?: {
    ttl?: number;
    retry_interval?: number;
    retry_count?: number;
  });
  /**
   * Tries to aquire a lock. If not successful, throws an error.
   * @param ttl - Time to live in milliseconds; a positive safe integer.
   * @returns Lock object.
   * @throws {RangeError} If the TTL is not a positive safe integer.
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
   * @param time Time to extend in milliseconds; a positive safe integer.
   * @returns -
   * @throws {RangeError} If the extension duration is not a positive safe integer.
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