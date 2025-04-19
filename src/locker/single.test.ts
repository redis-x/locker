import {
	test,
	expect,
} from 'vitest';
import { redisClient } from '../../test/redis.js';
import {
	RedisLock,
	RedisLocker,
	RedisLockerAcquireError,
	RedisLockerReleaseError,
} from '../main.js';
import { asyncTimeout } from '../utils.js';

const TIMEOUT = 200;

const locker = new RedisLocker(
	redisClient,
	'test',
	{
		ttl: TIMEOUT,
		retry_interval: 0,
		retry_count: 1,
	},
);

test('successful lock', async () => {
	const lock = await locker.lock();
	expect(lock).toBeInstanceOf(RedisLock);
});

// try to lock again, should fail
test('failed lock', async () => {
	const lock_promise = locker.lock();
	await expect(lock_promise).rejects.toThrow(RedisLockerAcquireError);
});

let lock_global: RedisLock;
test('successful unlock after timeout', async () => {
	await asyncTimeout(TIMEOUT);

	lock_global = await locker.lock();
	expect(lock_global).toBeInstanceOf(RedisLock);
});

test('lock release', async () => {
	await lock_global.release();
});

test('successful lock after release', async () => {
	const lock = await locker.lock();
	expect(lock).toBeInstanceOf(RedisLock);
});

test('lock release but with invalid lock token', async () => {
	const promise = lock_global.release();
	await expect(promise).rejects.toThrow(RedisLockerReleaseError);
});

test('lock extend', async () => {
	await redisClient.FLUSHDB();

	const lock = await locker.lock();
	expect(lock).toBeInstanceOf(RedisLock);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_2 = locker.lock();
	await expect(promise_lock_2).rejects.toThrow(RedisLockerAcquireError);

	await lock.extend(TIMEOUT);

	await asyncTimeout(TIMEOUT);

	const promise_lock_3 = locker.lock();
	await expect(promise_lock_3).rejects.toThrow(RedisLockerAcquireError);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_4 = locker.lock();
	await expect(promise_lock_4).resolves.toBeInstanceOf(RedisLock);
});
