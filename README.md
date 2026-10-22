# @redis-x/locker

[![npm version](https://img.shields.io/npm/v/@redis-x/locker.svg)](https://www.npmjs.com/package/@redis-x/locker)
[![license](https://img.shields.io/npm/l/@redis-x/locker.svg?color=blue)](https://github.com/redis-x/locker/blob/main/LICENSE)

A powerful and flexible distributed locking library for Redis with TypeScript support.

## Features

- 🔒 **Robust Locking**: Reliable distributed locking mechanism built on Redis
- ✨ **Flexible**: Support for both single-resource and multi-resource locks
- ⏱️ **Configurable TTL**: Set custom timeouts for your locks
- 🔄 **Lock Extension**: Extend lock duration without releasing
- 🔁 **Retry Logic**: Configurable retry mechanism for lock acquisition
- 🔍 **Type Safety**: Written in TypeScript with full type definitions

## Installation

```bash
bun i @redis-x/locker redis
# or with pnpm
pnpm add @redis-x/locker redis
# or with npm
npm install @redis-x/locker redis
```

**Requirements**:

- Node.js 16 or newer;
- `redis@^4.6`;
- (optional) [`@redis-x/client`](https://github.com/redis-x/client);
- Redis server **7.4** or newer.

## Basic Usage

First, set up a Redis client:

```typescript
import { createClient } from 'redis';

const redisClient = createClient();
await redisClient.connect();
```

Lockers accept either a plain `redis` client or a `RedisXClient` from [`@redis-x/client`](https://github.com/redis-x/client) package. If you already use `RedisXClient` in your project, pass it to the constructor to avoid duplicating `RedisXClient` instances.

### Single Resource Locking

When you need to lock a single resource (like a user account during update).

```typescript
import { RedisLocker } from '@redis-x/locker';

// Create a locker for a specific resource
const locker = new RedisLocker(redisClient, 'user:123', {
  ttl: 5000,            // Lock expires after 5 seconds
  retry_interval: 100,  // Wait 100ms between retry attempts
  retry_count: 5        // Try 5 times before giving up
});

let lock;
try {
  lock = await locker.lock();

  // Critical section - update user data
  await updateUserData(123);
} catch (error) {
  console.error('Operation failed:', error);
} finally {
  // Clean up the lock regardless of success or failure
  lock?.release().catch(console.error);
}
```

### Options

Both `RedisLocker` and `RedisMultiLocker` accept the same options:

| Option           | Default | Description                                                     |
| ---------------- | ------: | --------------------------------------------------------------- |
| `ttl`            |  `5000` | Lock lifetime in milliseconds. Can be overridden in `lock()`.   |
| `retry_interval` |   `100` | Milliseconds to wait between attempts, from 0 to 2147483647.    |
| `retry_count`    |    `10` | Maximum number of attempts, including the first one.            |

Invalid values throw a `RangeError`.

### Multi-Resource Locking

For operations that require multiple resources to be locked simultaneously (like transferring money between accounts). Either all IDs are locked or none: if any of them is already locked, the attempt fails and is retried.

`lock(ids)` accepts an array or `Set` of **1–100 unique IDs**. Numbers are converted to strings, so `1` and `'1'` are the same ID.

```typescript
import { RedisMultiLocker } from '@redis-x/locker';

// Create a multi-locker with a namespace
const multiLocker = new RedisMultiLocker(redisClient, 'banking');

let lock;
try {
  // Lock multiple accounts for a money transfer
  lock = await multiLocker.lock(['account:456', 'account:789']);

  // Critical section - both accounts are locked
  await transferMoney(456, 789, 100);
} catch (error) {
  console.error('Transfer failed:', error);
} finally {
  // Release all locks when done, even if an error occurred
  lock?.release().catch(console.error);
}
```

Single-lock keys and multi-lock namespaces share the same Redis keys, so don't use one name for both.

### Custom TTL for Specific Operations

You can override the default TTL for each lock operation:

```typescript
// Quick operation with short lock
const quickLock = await locker.lock(1000); // 1 second TTL

// Long operation with extended lock
const longLock = await multiLocker.lock(['account:456'], 30000); // 30 second TTL
```

### Extending a Lock

Sometimes your operation takes longer than expected. Instead of releasing and re-acquiring the lock, extend it. `extend()` adds time to what the lock has left:

```typescript
let lock;
try {
  lock = await locker.lock();

  // Start long-running task
  const isCompleted = await startLongTask();

  // If task isn't done yet, extend the lock
  if (!isCompleted) {
    await lock.extend(5000); // Add 5 more seconds
    await finishLongTask();
  }
} catch (error) {
  console.error('Operation failed:', error);
} finally {
  // Always clean up
  lock?.release().catch(console.error);
}
```

### Handling Lock Errors

- `RedisLockerAcquireError` is thrown by `lock()` when every attempt failed because the resource is locked, and by `extend()` when the lock has expired or belongs to someone else.
- `RedisLockerReleaseError` is thrown by `release()` when the lock has expired or belongs to someone else.
- `RangeError` is thrown for an invalid TTL, retry option or number of IDs, before anything is sent to Redis.

```typescript
import { RedisLockerAcquireError } from '@redis-x/locker';

try {
  const lock = await locker.lock();
  // ...
} catch (error) {
  if (error instanceof RedisLockerAcquireError) {
    console.log('Resource is currently locked by another process');
  }
}
```
