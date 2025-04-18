export const REDIS_PREFIX = '@x:locker:';
/**
 * Resolves after a given time.
 * @param ms Time in milliseconds.
 * @returns -
 */
export function asyncTimeout(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
