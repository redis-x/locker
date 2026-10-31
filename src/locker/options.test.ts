import { getRedisXClient, RedisXClient } from '@redis-x/client';
import { createClient } from 'redis';
import { afterEach, describe, expect, test, vi } from 'vitest';
import {
	type RedisXLock,
	RedisXLocker,
	RedisXLockerAcquireError,
	type RedisXMultiLock,
	RedisXMultiLocker,
} from '../main.js';

/**
 * Creates an unconnected client that answers lock commands without Redis.
 * @param isSuccessful Whether the SET or EVALSHA command with this 1-based number succeeds.
 * @returns Client and its command spy.
 */
function createMockClient(
	isSuccessful: (attempt: number) => boolean = () => true,
) {
	const client = createClient();
	let attempt = 0;
	const sendCommand = vi
		.spyOn(client, 'sendCommand')
		.mockImplementation(([command]) => {
			if (command === 'SCRIPT') {
				return Promise.resolve('sha');
			}

			attempt++;
			if (command === 'SET') {
				return Promise.resolve(isSuccessful(attempt) ? 'OK' : null);
			}

			return Promise.resolve(isSuccessful(attempt) ? 1 : 0);
		});

	return { client, sendCommand };
}

/**
 * Lists the commands a mock client received, except script loading.
 * @param sendCommand Command spy from {@link createMockClient}.
 * @returns Command names.
 */
function lockCommands(
	sendCommand: ReturnType<typeof createMockClient>['sendCommand'],
): unknown[] {
	return sendCommand.mock.calls
		.map(([[command]]) => command)
		.filter((command) => command !== 'SCRIPT');
}

/**
 * Acquires one resource with either locker implementation.
 * @param locker Locker to test.
 * @param ttl Optional TTL override.
 * @returns Lock object.
 */
function acquire(
	locker: RedisXLocker | RedisXMultiLocker,
	ttl?: number,
): Promise<RedisXLock | RedisXMultiLock> {
	return locker instanceof RedisXLocker
		? locker.lock(ttl)
		: locker.lock(['id'], ttl);
}

const LOCKERS = [
	['single', RedisXLocker],
	['multi', RedisXMultiLocker],
] as const;

afterEach(() => {
	vi.restoreAllMocks();
});

describe.each(LOCKERS)('%s locker options', (_, Locker) => {
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
		const { client, sendCommand } = createMockClient();
		const ttl = duration as number;

		expect(() => new Locker(client, 'validation', { ttl })).toThrow(RangeError);
		expect(sendCommand).not.toHaveBeenCalled();

		const locker = new Locker(client, 'validation');
		await expect(acquire(locker, ttl)).rejects.toThrow(RangeError);
		expect(sendCommand).not.toHaveBeenCalled();

		const lock = await acquire(locker);
		sendCommand.mockClear();
		await expect(lock.extend(ttl)).rejects.toThrow(RangeError);
		expect(sendCommand).not.toHaveBeenCalled();
	});

	test.each([1, 1000, Number.MAX_SAFE_INTEGER])(
		'accepts positive safe duration %s',
		async (ttl) => {
			const { client } = createMockClient();
			const locker = new Locker(client, 'validation', { ttl });

			await acquire(locker);
			const lock = await acquire(locker, ttl);
			await expect(lock.extend(ttl)).resolves.toBeUndefined();
		},
	);

	test.each([1, 3])(
		'waits only between %s failed attempts',
		async (retry_count) => {
			const { client, sendCommand } = createMockClient(() => false);
			const timeout = vi.spyOn(globalThis, 'setTimeout');
			const locker = new Locker(client, 'retries', {
				retry_count,
				retry_interval: 0,
			});

			await expect(acquire(locker)).rejects.toThrow(RedisXLockerAcquireError);
			expect(lockCommands(sendCommand)).toHaveLength(retry_count);
			expect(timeout).toHaveBeenCalledTimes(retry_count - 1);
		},
	);

	test('stops retrying after a successful second attempt', async () => {
		const { client, sendCommand } = createMockClient((attempt) => attempt > 1);
		const timeout = vi.spyOn(globalThis, 'setTimeout');
		const locker = new Locker(client, 'retries', {
			retry_count: 3,
			retry_interval: 0,
		});

		await acquire(locker);
		expect(lockCommands(sendCommand)).toHaveLength(2);
		expect(timeout).toHaveBeenCalledTimes(1);
	});
});

describe.each(LOCKERS)('%s locker clients', (_, Locker) => {
	test('accepts a RedisX client', async () => {
		const { client, sendCommand } = createMockClient();
		const locker = new Locker(new RedisXClient(client), 'client');

		const lock = await acquire(locker);
		await lock.release();
		expect(lockCommands(sendCommand)).toHaveLength(2);
	});

	test('loads each script once for lockers sharing a redis client', async () => {
		const { client, sendCommand } = createMockClient();
		const first = await acquire(new Locker(client, 'first'));
		const second = await acquire(new Locker(client, 'second'));
		await first.release();
		await second.release();

		const loaded = sendCommand.mock.calls
			.filter(([[command]]) => command === 'SCRIPT')
			.map(([args]) => args[2]);
		expect(loaded.length).toBeGreaterThan(0);
		expect(new Set(loaded).size).toBe(loaded.length);
		expect(lockCommands(sendCommand)).not.toContain('EVAL');
	});

	test('shares the RedisX client that getRedisXClient returns for a redis client', async () => {
		const { client, sendCommand } = createMockClient();
		const plain = await acquire(new Locker(client, 'plain'));
		const shared = await acquire(new Locker(getRedisXClient(client), 'shared'));
		await plain.release();
		await shared.release();

		const loaded = sendCommand.mock.calls
			.filter(([[command]]) => command === 'SCRIPT')
			.map(([args]) => args[2]);
		expect(loaded.length).toBeGreaterThan(0);
		expect(new Set(loaded).size).toBe(loaded.length);
	});
});

describe.each(LOCKERS)('%s locker retry options', (_, Locker) => {
	test.each([
		{ retry_count: 0 },
		{ retry_count: -1 },
		{ retry_count: 1.5 },
		// eslint-disable-next-line unicorn/prefer-global-number-constants -- Oxlint requires Number.NaN.
		{ retry_count: Number.NaN },
		{ retry_count: Infinity },
		{ retry_count: Number.MAX_SAFE_INTEGER + 1 },
		{ retry_count: null },
		{ retry_count: '3' },
		{ retry_interval: -1 },
		{ retry_interval: 0.5 },
		// eslint-disable-next-line unicorn/prefer-global-number-constants -- Oxlint requires Number.NaN.
		{ retry_interval: Number.NaN },
		{ retry_interval: Infinity },
		{ retry_interval: 2_147_483_648 },
		{ retry_interval: null },
		{ retry_interval: '100' },
	])('rejects invalid value %o', (options) => {
		expect(
			() =>
				new Locker(
					createClient(),
					'validation',
					options as { retry_count?: number; retry_interval?: number },
				),
		).toThrow(RangeError);
	});

	test.each([
		{ retry_count: 1, retry_interval: 0 },
		{ retry_count: Number.MAX_SAFE_INTEGER, retry_interval: 2_147_483_647 },
	])('accepts boundary value %o', (options) => {
		expect(
			() => new Locker(createClient(), 'validation', options),
		).not.toThrow();
	});
});

describe('multi-lock batch size', () => {
	test('rejects an empty array or Set before Redis commands', async () => {
		const { client, sendCommand } = createMockClient();
		const locker = new RedisXMultiLocker(client, 'batch');

		await expect(locker.lock([])).rejects.toThrow(RangeError);
		await expect(locker.lock(new Set())).rejects.toThrow(RangeError);
		expect(sendCommand).not.toHaveBeenCalled();
	});

	test('counts unique IDs after converting them to strings', async () => {
		const { client, sendCommand } = createMockClient();
		const locker = new RedisXMultiLocker(client, 'batch');
		const ids = Array.from({ length: 5000 }, (_, index) => index);

		await locker.lock([...ids, ...ids.map(String)]);
		expect(sendCommand).toHaveBeenLastCalledWith([
			'EVALSHA',
			'sha',
			'1',
			locker._redis_key,
			expect.any(String),
			'5000',
			...ids.map(String),
		]);
	});
});
