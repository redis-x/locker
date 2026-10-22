import { afterEach, expect, test, vi } from 'vitest';
import { redisClient } from '../../test/redis.js';
import {
	RedisLockerAcquireError,
	RedisLockerReleaseError,
	RedisMultiLocker,
} from '../main.js';
import { asyncTimeout } from '../utils.js';

const keys = new Set<string>();

/**
 * Creates a locker whose namespace is cleaned up after the test.
 * @param name Test namespace.
 * @param options Locker options.
 * @returns Locker instance.
 */
function createLocker(
	name: string,
	options?: ConstructorParameters<typeof RedisMultiLocker>[2],
): RedisMultiLocker {
	const locker = new RedisMultiLocker(redisClient, `test-multi-${name}`, {
		retry_count: 1,
		...options,
	});
	keys.add(locker._redis_key);
	return locker;
}

/**
 * Reads remaining lease durations for TTL regression assertions.
 * @param locker Locker namespace.
 * @param ids Resource IDs.
 * @returns Remaining milliseconds for each resource.
 */
function remainingTtls(
	locker: RedisMultiLocker,
	ids: string[],
): Promise<number[]> {
	return redisClient.sendCommand([
		'HPTTL',
		locker._redis_key,
		'FIELDS',
		String(ids.length),
		...ids,
	]) as Promise<number[]>;
}

afterEach(async () => {
	vi.restoreAllMocks();
	if (keys.size > 0) {
		await redisClient.DEL([...keys]);
		keys.clear();
	}
});

test.each(['array', 'set'])(
	"caller changes to %s IDs do not change the lock's resources",
	async (kind) => {
		const locker = createLocker(`snapshot-${kind}`);
		const values = [1, '1', 'two'];
		const ids = kind === 'array' ? values : new Set(values);
		const pending = locker.lock(ids);
		if (Array.isArray(ids)) {
			ids.splice(0, ids.length, 'replacement');
		} else {
			ids.clear();
			ids.add('replacement');
		}

		const original = await pending;
		const replacement = await locker.lock(['replacement']);

		await original.extend(1000);
		await original.release();
		const next = await locker.lock(['1', 'two']);
		await expect(locker.lock(['replacement'])).rejects.toThrow(
			RedisLockerAcquireError,
		);
		await next.release();
		await replacement.release();
	},
);

test('conflicting batches leave both the owner and free IDs unchanged', async () => {
	const locker = createLocker('conflict');
	const owner = await locker.lock([1, 2]);

	await expect(locker.lock([1, 2])).rejects.toThrow(RedisLockerAcquireError);
	await expect(locker.lock([3, 2])).rejects.toThrow(RedisLockerAcquireError);
	const disjoint = await locker.lock([3, 4]);
	await owner.extend(1000);
	await owner.release();
	await disjoint.release();
});

test('released IDs can be reacquired and the old handle cannot release again', async () => {
	const locker = createLocker('release');
	const original = await locker.lock([1, 2]);
	await original.release();
	const current = await locker.lock([1, 2]);

	await expect(original.release()).rejects.toThrow(RedisLockerReleaseError);
	await current.release();
});

test('only one of two concurrent overlapping batches acquires a lock', async () => {
	const first = createLocker('concurrent');
	const second = createLocker('concurrent');
	const results = await Promise.allSettled([
		first.lock(['one', 'shared']),
		second.lock(['shared', 'two']),
	]);

	expect(
		results.filter((result) => result.status === 'fulfilled'),
	).toHaveLength(1);
	await Promise.all(
		results.map(async (result) => {
			if (result.status === 'fulfilled') {
				await result.value.release();
			} else {
				expect(result.reason).toBeInstanceOf(RedisLockerAcquireError);
			}
		}),
	);
});

test.each(['expiration', 'release'])(
	'retries after %s start a fresh full lease',
	async (state) => {
		const first = createLocker(`retry-${state}`);
		const owner = await first.lock(['id'], state === 'expiration' ? 150 : 5000);
		const waiter = createLocker(`retry-${state}`, {
			retry_count: 5,
			retry_interval: 200,
		});
		const pending = waiter.lock(['id'], 1000);
		if (state === 'release') {
			await asyncTimeout(100);
			await owner.release();
		}

		const lock = await pending;

		const [ttl] = await remainingTtls(waiter, ['id']);
		expect(ttl).toBeGreaterThan(900);
		await lock.release();
	},
);

test('client clock changes cannot steal another lock', async () => {
	const locker = createLocker('clock');
	const clock = vi.spyOn(Date, 'now').mockReturnValue(0);
	const owner = await locker.lock(['id']);
	clock.mockReturnValue(8_000_000_000_000_000);

	await expect(locker.lock(['id'])).rejects.toThrow(RedisLockerAcquireError);
	await owner.extend(1000);
	await owner.release();
});

test('request transport delays do not consume the server-side TTL', async () => {
	const locker = createLocker('request-delay');
	const evaluate = redisClient.EVAL.bind(redisClient);
	vi.spyOn(redisClient, 'EVAL').mockImplementationOnce(async (...args) => {
		await asyncTimeout(200);
		return evaluate(...args);
	});
	const lock = await locker.lock(['id'], 1000);

	const ttls = await remainingTtls(locker, ['id']);
	expect(ttls[0]).toBeGreaterThan(900);
	await lock.release();
});

test("expired handles cannot extend or release a successor's lock", async () => {
	const locker = createLocker('stale');
	const stale = await locker.lock(['one', 'two'], 40);
	await asyncTimeout(100);
	const current = await locker.lock(['one', 'two']);
	const before = await remainingTtls(locker, ['one', 'two']);

	await expect(stale.extend(1000)).rejects.toThrow(RedisLockerAcquireError);
	await expect(stale.release()).rejects.toThrow(RedisLockerReleaseError);
	const after = await remainingTtls(locker, ['one', 'two']);
	expect(after[0]).toBeGreaterThan(0);
	expect(after[0]).toBeLessThanOrEqual(before[0]);
	expect(after[1]).toBeGreaterThan(0);
	expect(after[1]).toBeLessThanOrEqual(before[1]);
	await current.release();
});

test.each(['missing', 'replaced'])(
	'losing a %s resource prevents changes to the rest of the batch',
	async (state) => {
		const locker = createLocker(`partial-${state}`);
		const old = await locker.lock(['one', 'two']);
		await redisClient.HDEL(locker._redis_key, 'two');
		const current =
			state === 'replaced' ? await locker.lock(['two']) : undefined;

		const before = await remainingTtls(locker, ['one']);

		await expect(old.extend(1000)).rejects.toThrow(RedisLockerAcquireError);
		await expect(old.release()).rejects.toThrow(RedisLockerReleaseError);
		const after = await remainingTtls(locker, ['one']);
		expect(after[0]).toBeGreaterThan(0);
		expect(after[0]).toBeLessThanOrEqual(before[0]);
		await current?.release();
	},
);

test('extension adds to the remaining TTL without affecting other owners', async () => {
	const locker = createLocker('extend');
	const owner = await locker.lock(['one', 'two'], 5000);
	const other = await locker.lock(['three'], 5000);
	const before = await remainingTtls(locker, ['one', 'two', 'three']);
	await owner.extend(1000);
	const after = await remainingTtls(locker, ['one', 'two', 'three']);

	expect(after[0]).toBeGreaterThan(before[0] + 900);
	expect(after[0]).toBeLessThanOrEqual(before[0] + 1000);
	expect(after[0]).toBe(after[1]);
	expect(after[2]).toBeGreaterThan(0);
	expect(after[2]).toBeLessThanOrEqual(before[2]);
	await owner.release();
	await other.release();
});

test('expiration errors leave requested IDs available and existing locks intact', async () => {
	const locker = createLocker('invalid-expiration');
	await expect(
		locker.lock(['one', 'two'], Number.MAX_SAFE_INTEGER),
	).rejects.toThrow('invalid expire time');

	const owner = await locker.lock(['existing']);
	await expect(
		locker.lock(['one', 'two'], Number.MAX_SAFE_INTEGER),
	).rejects.toThrow('invalid expire time');
	const recovered = await locker.lock(['one', 'two']);
	await expect(locker.lock(['existing'])).rejects.toThrow(
		RedisLockerAcquireError,
	);
	const before = await remainingTtls(locker, ['existing']);
	await expect(owner.extend(Number.MAX_SAFE_INTEGER)).rejects.toThrow(
		'invalid expire time',
	);
	const after = await remainingTtls(locker, ['existing']);
	expect(after[0]).toBeGreaterThan(0);
	expect(after[0]).toBeLessThanOrEqual(before[0]);
	await owner.release();
	await recovered.release();
});

test.each(['array', 'set'])(
	'locks, extends, and releases a 100-ID %s batch',
	async (kind) => {
		const batch_locker = createLocker(`batch-${kind}`);
		const ids = Array.from({ length: 100 }, (_, index) => index);
		const lock = await batch_locker.lock(kind === 'array' ? ids : new Set(ids));

		await lock.extend(1000);
		await expect(batch_locker.lock([99, 100])).rejects.toThrow(
			RedisLockerAcquireError,
		);
		const disjoint = await batch_locker.lock([100]);
		await disjoint.release();
		await lock.release();

		const next = await batch_locker.lock(ids);
		await next.release();
	},
);

test('accepts an array of more than 100 duplicate IDs', async () => {
	const batch_locker = createLocker('batch-duplicates');
	const lock = await batch_locker.lock(Array.from({ length: 150 }, () => 'id'));
	await lock.extend(1000);
	await expect(batch_locker.lock(['id'])).rejects.toThrow(
		RedisLockerAcquireError,
	);
	await lock.release();

	const next = await batch_locker.lock(['id']);
	await next.release();
});
