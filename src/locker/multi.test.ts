import { expect, test } from 'vitest';
import { redisClient } from '../../test/redis.js';
import {
	RedisLockerAcquireError,
	RedisLockerReleaseError,
	RedisMultiLock,
	RedisMultiLocker,
} from '../main.js';
import { asyncTimeout } from '../utils.js';

const TIMEOUT = 200;

const locker = new RedisMultiLocker(redisClient, 'test-multi', {
	ttl: TIMEOUT,
	retry_interval: 0,
	retry_count: 1,
});

test('lock', async () => {
	const lock = await locker.lock([1, 2]);
	expect(lock).toBeInstanceOf(RedisMultiLock);
});

test('lock for same IDs', async () => {
	const lock_promise = locker.lock([1, 2]);
	await expect(lock_promise).rejects.toThrow(RedisLockerAcquireError);
});

test('lock for one same and one different IDs', async () => {
	const lock_promise = locker.lock([2, 3]);
	await expect(lock_promise).rejects.toThrow(RedisLockerAcquireError);
});

test('lock totally different IDs', async () => {
	const lock = await locker.lock([3, 4]);
	expect(lock).toBeInstanceOf(RedisMultiLock);
});

let lock_global: RedisMultiLock;
test('lock after timeout', async () => {
	await asyncTimeout(TIMEOUT);

	lock_global = await locker.lock([1, 2]);
	expect(lock_global).toBeInstanceOf(RedisMultiLock);
});

test('release lock', async () => {
	await lock_global.release();
});

test('release lock second time', async () => {
	await expect(lock_global.release()).rejects.toThrow(RedisLockerReleaseError);
});

test('lock released IDs', async () => {
	await asyncTimeout(TIMEOUT);

	const lock = await locker.lock([1, 2]);
	expect(lock).toBeInstanceOf(RedisMultiLock);
});

test('lock extend', async () => {
	await redisClient.FLUSHDB();

	const lock = await locker.lock([1, 2]);
	expect(lock).toBeInstanceOf(RedisMultiLock);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_2 = locker.lock([1, 2]);
	await expect(promise_lock_2).rejects.toThrow(RedisLockerAcquireError);

	await lock.extend(TIMEOUT);

	await asyncTimeout(TIMEOUT);

	const promise_lock_3 = locker.lock([1, 2]);
	await expect(promise_lock_3).rejects.toThrow(RedisLockerAcquireError);

	await asyncTimeout(TIMEOUT * 0.5);

	const promise_lock_4 = locker.lock([1, 2]);
	await expect(promise_lock_4).resolves.toBeInstanceOf(RedisMultiLock);
});

test.each(['array', 'set'])(
	'locks, extends, and releases a 100-ID %s batch',
	async (kind) => {
		const batch_locker = new RedisMultiLocker(redisClient, `batch-${kind}`, {
			retry_count: 1,
		});
		const ids = Array.from({ length: 100 }, (_, index) => index);
		const lock = await batch_locker.lock(kind === 'array' ? ids : new Set(ids));

		try {
			await lock.extend(1000);
			await expect(batch_locker.lock([99, 100])).rejects.toThrow(
				RedisLockerAcquireError,
			);
			const disjoint = await batch_locker.lock([100]);
			await disjoint.release();
		} finally {
			await lock.release();
		}

		const next = await batch_locker.lock(ids);
		await next.release();
	},
);

test('accepts a 100-element array containing duplicate IDs', async () => {
	const batch_locker = new RedisMultiLocker(redisClient, 'batch-duplicates', {
		retry_count: 1,
	});
	const lock = await batch_locker.lock(Array.from({ length: 100 }, () => 'id'));
	try {
		await lock.extend(1000);
		await expect(batch_locker.lock(['id'])).rejects.toThrow(
			RedisLockerAcquireError,
		);
	} finally {
		await lock.release();
	}

	const next = await batch_locker.lock(['id']);
	await next.release();
});
