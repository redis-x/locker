import {
	test,
	expect,
} from 'vitest';
import { redisClient } from '../../test/redis.js';
import {
	RedisMultiLock,
	RedisMultiLocker,
	RedisLockerAcquireError,
	RedisLockerReleaseError,
} from '../main.js';
import { asyncTimeout } from '../utils.js';

const TIMEOUT = 200;

const locker = new RedisMultiLocker(
	redisClient,
	'test-multi',
	{
		ttl_ms: TIMEOUT,
		retry_interval_ms: 0,
		retry_count: 1,
	},
);

test('lock', async () => {
	const lock = await locker.lock([ 1, 2 ]);
	expect(lock).toBeInstanceOf(RedisMultiLock);
});

test('lock for same IDs', async () => {
	const lock_promise = locker.lock([ 1, 2 ]);
	await expect(lock_promise).rejects.toThrow(RedisLockerAcquireError);
});

test('lock for one same and one different IDs', async () => {
	const lock_promise = locker.lock([ 2, 3 ]);
	await expect(lock_promise).rejects.toThrow(RedisLockerAcquireError);
});

test('lock totally different IDs', async () => {
	const lock = await locker.lock([ 3, 4 ]);
	expect(lock).toBeInstanceOf(RedisMultiLock);
});

let lock_global: RedisMultiLock;
test('lock after timeout', async () => {
	await asyncTimeout(TIMEOUT);

	lock_global = await locker.lock([ 1, 2 ]);
	expect(lock_global).toBeInstanceOf(RedisMultiLock);
});

test('release lock', async () => {
	await lock_global.release();
});

test('release lock second time', async () => {
	await expect(
		lock_global.release(),
	).rejects.toThrow(RedisLockerReleaseError);
});

test('lock released IDs', async () => {
	await asyncTimeout(TIMEOUT);

	const lock = await locker.lock([ 1, 2 ]);
	expect(lock).toBeInstanceOf(RedisMultiLock);
});

test('lock extend', async () => {
	await redisClient.FLUSHDB();

	const lock = await locker.lock([ 1, 2 ]);
	expect(lock).toBeInstanceOf(RedisMultiLock);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_2 = locker.lock([ 1, 2 ]);
	await expect(promise_lock_2).rejects.toThrow(RedisLockerAcquireError);

	await lock.extend(TIMEOUT);

	await asyncTimeout(TIMEOUT);

	const promise_lock_3 = locker.lock([ 1, 2 ]);
	await expect(promise_lock_3).rejects.toThrow(RedisLockerAcquireError);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_4 = locker.lock([ 1, 2 ]);
	await expect(promise_lock_4).resolves.toBeInstanceOf(RedisMultiLock);
});
