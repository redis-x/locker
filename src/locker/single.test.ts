import { expect, test } from 'vitest';
import { redisClient } from '../../test/redis.js';
import {
	RedisXLock,
	RedisXLocker,
	RedisXLockerAcquireError,
	RedisXLockerReleaseError,
} from '../main.js';
import { asyncTimeout } from '../utils.js';

const TIMEOUT = 200;

const locker = new RedisXLocker(redisClient, 'test', {
	ttl: TIMEOUT,
	retry_interval: 0,
	retry_count: 1,
});

test('successful lock', async () => {
	const lock = await locker.lock();
	expect(lock).toBeInstanceOf(RedisXLock);
});

// try to lock again, should fail
test('failed lock', async () => {
	const lock_promise = locker.lock();
	await expect(lock_promise).rejects.toThrow(RedisXLockerAcquireError);
});

let lock_global: RedisXLock;
test('successful unlock after timeout', async () => {
	await asyncTimeout(TIMEOUT);

	lock_global = await locker.lock();
	expect(lock_global).toBeInstanceOf(RedisXLock);
});

test('lock release', async () => {
	await lock_global.release();
});

test('successful lock after release', async () => {
	const lock = await locker.lock();
	expect(lock).toBeInstanceOf(RedisXLock);
});

test('lock release but with invalid lock token', async () => {
	const promise = lock_global.release();
	await expect(promise).rejects.toThrow(RedisXLockerReleaseError);
});

test('lock extend', async () => {
	await redisClient.FLUSHDB();

	const lock = await locker.lock();
	expect(lock).toBeInstanceOf(RedisXLock);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_2 = locker.lock();
	await expect(promise_lock_2).rejects.toThrow(RedisXLockerAcquireError);

	await lock.extend(TIMEOUT);

	await asyncTimeout(TIMEOUT);

	const promise_lock_3 = locker.lock();
	await expect(promise_lock_3).rejects.toThrow(RedisXLockerAcquireError);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_4 = locker.lock();
	await expect(promise_lock_4).resolves.toBeInstanceOf(RedisXLock);
});

const silent_locker = new RedisXLocker(redisClient, 'test-silent', {
	ttl: TIMEOUT,
	retry_interval: 0,
	retry_count: 1,
});

test('silent release frees a held lock', async () => {
	const lock = await silent_locker.lock();
	await lock.release(true);

	const next = await silent_locker.lock();
	expect(next).toBeInstanceOf(RedisXLock);
	await next.release();
});

test('silent release ignores an already released lock', async () => {
	const lock = await silent_locker.lock();
	await lock.release();

	await expect(lock.release(true)).resolves.toBeUndefined();
});

test("silent release of an expired lock keeps the successor's lock", async () => {
	const stale = await silent_locker.lock(40);
	await asyncTimeout(100);
	const current = await silent_locker.lock();

	await expect(stale.release(true)).resolves.toBeUndefined();
	await expect(silent_locker.lock()).rejects.toThrow(RedisXLockerAcquireError);
	await current.release();
});
