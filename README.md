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
bun i @redis-x/locker
# or with pnpm
pnpm add @redis-x/locker
# or with npm
npm install @redis-x/locker
```

**Note**: This package requires `redis@^4.6` as a peer dependency.

## Basic Usage

First, set up a Redis client:

```typescript
import { createClient } from 'redis';

const redisClient = createClient();
await redisClient.connect();
```

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

### Multi-Resource Locking

For operations that require multiple resources to be locked simultaneously (like transferring money between accounts). If any resource is already locked, the entire operation fails.

```typescript
import { RedisMultiLocker } from '@redis-x/locker';

// Create a multi-locker with a namespace
const multiLocker = new RedisMultiLocker(redisClient, 'banking');

let lock;
try {
  // Lock multiple accounts for a money transfer
  lock = await multiLocker.lock(['account:456', 'account:789']);

  // Perform atomic operation across both resources
  await transferMoney(456, 789, 100);
} catch (error) {
  console.error('Transfer failed:', error);
} finally {
  // Release all locks when done, even if an error occurred
  lock?.release().catch(console.error);
}
```

### Custom TTL for Specific Operations

You can specify custom TTL for each lock operation:

```typescript
// Quick operation with short lock
const quickLock = await locker.lock(1000); // 1 second TTL

// Long operation with extended lock
const longLock = await locker.lock(30000); // 30 second TTL
```

### Extending a Lock

Sometimes your operation takes longer than expected. Instead of releasing and re-acquiring the lock, extend it:

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

The library provides specific error types for different failure scenarios:

```typescript
import { RedisLockerAcquireError, RedisLockerReleaseError } from '@redis-x/locker';

try {
  lock = await locker.lock();
} catch (error) {
  if (error instanceof RedisLockerAcquireError) {
    // Handle acquisition failure (resource is busy)
    console.log('Resource is currently locked by another process');
  } else if (error instanceof RedisLockerReleaseError) {
    // Handle release failure (lock expired or was stolen)
    console.log('Could not release lock, it may have expired');
  }
}
```
