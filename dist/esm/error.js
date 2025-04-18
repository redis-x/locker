export class RedisLockerAcquireError extends Error {
    constructor() {
        super('Failed to acquire lock');
    }
}
export class RedisLockerReleaseError extends Error {
    constructor() {
        super('Failed to release lock');
    }
}
