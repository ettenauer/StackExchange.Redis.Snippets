using System.Reflection;

namespace StackExchange.Redis.Snippets.Lua;

public class LuaScriptExecutor<TScript> where TScript : Enum
{
    private readonly Dictionary<TScript, string> _scriptsByName = new();

    public static LuaScriptExecutor<TScript> Create(string embeddedResourceName)
    {
        var executor = new LuaScriptExecutor<TScript>();
        executor.LoadScripts(embeddedResourceName);
        return executor;
    }

    private LuaScriptExecutor() { }

    public Task<RedisResult> ScriptEvaluateAsync(IDatabase database, TScript script, CommandFlags commandFlags, RedisKey[]? keys = null, RedisValue[]? values = null)
    {
        if (!_scriptsByName.ContainsKey(script)) throw new NotSupportedException($"Script of type {script} is not supported.");

        return ScriptEvaluateAsync(database, script, (db, s) => db.ScriptEvaluateAsync(s, keys, values, commandFlags));
    }

    private async Task<RedisResult> ScriptEvaluateAsync(IDatabase database, TScript script, Func<IDatabase, string, Task<RedisResult>> evalFunc)
    {
        try
        {
            return await evalFunc(database, _scriptsByName[script]).ConfigureAwait(false);
        }
        catch (RedisServerException rex) when (rex.Message.Contains("NOSCRIPT"))
        {
            // https://stackoverflow.com/a/24156675/1985167
            return await evalFunc(database, _scriptsByName[script]).ConfigureAwait(false);
        }
    }

    private void LoadScripts(string embeddedResourceName)
    {
        foreach (var scriptName in Enum.GetValues(typeof(TScript)).OfType<TScript>())
        {
            var scriptText = GetResourceTextFile(embeddedResourceName, $"{scriptName}.lua");
            _scriptsByName[scriptName] = scriptText;
        }
    }

    private string GetResourceTextFile(string embeddedResourceName, string filename)
    {
        using var stream = GetType().Assembly.GetManifestResourceStream($"{embeddedResourceName}.{filename}");
        using var sr = new StreamReader(stream ?? throw new ArgumentException("lua script stream cannot be null"));
        return sr.ReadToEnd();
    }
}