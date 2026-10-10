import { createClient } from 'redis';
import { afterEach, describe, expect, test, vi } from 'vitest';
import {
	type RedisLock,
	RedisLocker,
	RedisLockerAcquireError,
	type RedisMultiLock,
	RedisMultiLocker,
} from '../main.js';

/**
 * Acquires one resource with either locker implementation.
 * @param locker Locker to test.
 * @param ttl Optional TTL override.
 * @returns Lock object.
 */
function acquire(
	locker: RedisLocker | RedisMultiLocker,
	ttl?: number,
): Promise<RedisLock | RedisMultiLock> {
	return locker instanceof RedisLocker
		? locker.lock(ttl)
		: locker.lock(['id'], ttl);
}

afterEach(() => {
	vi.restoreAllMocks();
});

describe.each([
	['single', RedisLocker],
	['multi', RedisMultiLocker],
] as const)('%s locker options', (_, Locker) => {
	test.each([
		0,
		-1,
		1000.5,
		// eslint-disable-next-line unicorn/prefer-global-number-constants -- Oxlint requires Number.NaN.
		Number.NaN,
		Infinity,
		-Infinity,
		Number.MAX_SAFE_INTEGER + 1,
		null,
		'1000',
	])('rejects invalid duration %s before Redis commands', async (duration) => {
		const client = createClient();
		const set = vi.spyOn(client, 'SET').mockResolvedValue('OK');
		const evaluate = vi.spyOn(client, 'EVAL').mockResolvedValue(1);
		const ttl = duration as number;

		expect(() => new Locker(client, 'validation', { ttl })).toThrow(RangeError);
		expect(set).not.toHaveBeenCalled();
		expect(evaluate).not.toHaveBeenCalled();

		const locker = new Locker(client, 'validation');
		await expect(acquire(locker, ttl)).rejects.toThrow(RangeError);
		expect(set).not.toHaveBeenCalled();
		expect(evaluate).not.toHaveBeenCalled();

		const lock = await acquire(locker);
		set.mockClear();
		evaluate.mockClear();
		await expect(lock.extend(ttl)).rejects.toThrow(RangeError);
		expect(set).not.toHaveBeenCalled();
		expect(evaluate).not.toHaveBeenCalled();
	});

	test.each([1, 1000, Number.MAX_SAFE_INTEGER])(
		'accepts positive safe duration %s',
		async (ttl) => {
			const client = createClient();
			vi.spyOn(client, 'SET').mockResolvedValue('OK');
			vi.spyOn(client, 'EVAL').mockResolvedValue(1);
			const locker = new Locker(client, 'validation', { ttl });

			await acquire(locker);
			const lock = await acquire(locker, ttl);
			await expect(lock.extend(ttl)).resolves.toBeUndefined();
		},
	);

	test.each([1, 3])(
		'waits only between %s failed attempts',
		async (retry_count) => {
			const client = createClient();
			const set = vi.spyOn(client, 'SET').mockResolvedValue(null);
			const evaluate = vi.spyOn(client, 'EVAL').mockResolvedValue(0);
			const timeout = vi.spyOn(globalThis, 'setTimeout');
			const locker = new Locker(client, 'retries', {
				retry_count,
				retry_interval: 0,
			});

			await expect(acquire(locker)).rejects.toThrow(RedisLockerAcquireError);
			expect(set.mock.calls.length + evaluate.mock.calls.length).toBe(
				retry_count,
			);
			expect(timeout).toHaveBeenCalledTimes(retry_count - 1);
		},
	);

	test('stops retrying after a successful second attempt', async () => {
		const client = createClient();
		const set = vi
			.spyOn(client, 'SET')
			.mockResolvedValueOnce(null)
			.mockResolvedValue('OK');
		const evaluate = vi
			.spyOn(client, 'EVAL')
			.mockResolvedValueOnce(0)
			.mockResolvedValue(1);
		const timeout = vi.spyOn(globalThis, 'setTimeout');
		const locker = new Locker(client, 'retries', {
			retry_count: 3,
			retry_interval: 0,
		});

		await acquire(locker);
		expect(set.mock.calls.length + evaluate.mock.calls.length).toBe(2);
		expect(timeout).toHaveBeenCalledTimes(1);
	});
});

describe('multi-lock batch limits', () => {
	test.each([0, 101, 5000])(
		'rejects arrays and Sets of %s IDs before Redis commands',
		async (count) => {
			const client = createClient();
			const evaluate = vi.spyOn(client, 'EVAL');
			const locker = new RedisMultiLocker(client, 'batch');
			const ids = Array.from({ length: count }, (_, index) => index);

			await expect(locker.lock(ids)).rejects.toThrow(RangeError);
			await expect(locker.lock(new Set(ids))).rejects.toThrow(RangeError);
			expect(evaluate).not.toHaveBeenCalled();
		},
	);

	test('counts duplicate array elements toward the limit', async () => {
		const client = createClient();
		const evaluate = vi.spyOn(client, 'EVAL');
		const locker = new RedisMultiLocker(client, 'batch');

		await expect(
			locker.lock(Array.from({ length: 101 }, () => 'id')),
		).rejects.toThrow(RangeError);
		expect(evaluate).not.toHaveBeenCalled();
	});
});
