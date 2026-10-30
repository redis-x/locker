export class RedisXLockerAcquireError extends Error {
	constructor() {
		super('Failed to acquire lock');
	}
}

export class RedisXLockerReleaseError extends Error {
	constructor() {
		super('Failed to release lock');
	}
}
