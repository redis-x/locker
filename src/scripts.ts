import type { RedisXClient, RedisXScript } from '@redis-x/client';
import lua_multi_extend from './lua/multi-extend.lua' with { type: 'text' };
import lua_multi_lock from './lua/multi-lock.lua' with { type: 'text' };
import lua_multi_release from './lua/multi-release.lua' with { type: 'text' };
import lua_single_extend from './lua/single-extend.lua' with { type: 'text' };
import lua_single_release from './lua/single-release.lua' with { type: 'text' };

type Script = RedisXScript<string[], unknown>;

export type Scripts = {
	single_extend: Script;
	single_release: Script;
	multi_lock: Script;
	multi_extend: Script;
	multi_release: Script;
};

const SCRIPTS = new WeakMap<RedisXClient, Scripts>();

/**
 * Returns lock scripts for a RedisX client, creating them once per client.
 * @param redisXClient RedisX client.
 * @returns Lock scripts.
 */
export function getScripts(redisXClient: RedisXClient): Scripts {
	let scripts = SCRIPTS.get(redisXClient);
	if (!scripts) {
		scripts = {
			single_extend: redisXClient.createScript(lua_single_extend),
			single_release: redisXClient.createScript(lua_single_release),
			multi_lock: redisXClient.createScript(lua_multi_lock),
			multi_extend: redisXClient.createScript(lua_multi_extend),
			multi_release: redisXClient.createScript(lua_multi_release),
		};
		SCRIPTS.set(redisXClient, scripts);
	}

	return scripts;
}
