using Microsoft.Extensions.Logging;

namespace StackExchange.Redis.Snippets.Lua;

public class LuaRedisClient<TScript> : RedisClient where TScript : Enum
{
    private readonly LuaScriptExecutor<TScript> _luaScriptExecutor;
    public LuaRedisClient(string connectionString, string scriptEmbeddedResourceName, ILogger<RedisClient> logger) 
        : base(connectionString, logger) 
    {
        _luaScriptExecutor = LuaScriptExecutor<TScript>.Create(scriptEmbeddedResourceName);
    }

    public async Task<RedisResult> ExecuteAsync(TScript script, RedisKey[] keys, RedisValue[] values)
    {
        return await RetryOnRedisExceptionAsync((db) => _luaScriptExecutor.ScriptEvaluateAsync(db, script, CommandFlags.DemandMaster, keys, values)).ConfigureAwait(false);
    }
}

