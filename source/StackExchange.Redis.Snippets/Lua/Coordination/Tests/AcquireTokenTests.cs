using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Reflection;
using Xunit;

namespace StackExchange.Redis.Snippets.Lua.Coordination.Tests;

public class AcquireTokenTests
{
    private LuaRedisClient<CoordinationLuaScripts> _luaScriptClient;
    private Mock<ILogger<RedisClient>> _loggerMock;

    public AcquireTokenTests()
    { 
        _loggerMock = new Mock<ILogger<RedisClient>>();
        var scriptEmbeddedResourceName = $"{new AssemblyName(GetType().Assembly.FullName).Name}.Lua.Coordination";
        _luaScriptClient = new LuaRedisClient<CoordinationLuaScripts>("localhost:6379", scriptEmbeddedResourceName, _loggerMock.Object);
    }

    [Fact]
    public async Task AcquireToken_Free_FencingToken()
    {
        var leaseInSeconds = 3;
        var token = (int) await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "AcquireLock_Free_FencingToken" },
            new RedisValue[]
            {
                $"{Environment.MachineName}-1",
                leaseInSeconds
            });

        token.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task AcquireToken_Acquire_Abort()
    {
        var leaseInSeconds = 3;
        var token = (int)await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "AcquireLock_Acquire_Abort" },
            new RedisValue[]
            {
                $"{Environment.MachineName}-1",
                leaseInSeconds
            });

        token.Should().BeGreaterThan(0);

        token = (int)await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "AcquireLock_Acquire_Abort" },
            new RedisValue[]
            {
                        $"{Environment.MachineName}-2",
                        leaseInSeconds
            });

        //Note: lock is acquired by different client, cannot be acquired until timeout
        token.Should().Be(-1);
    }

    [Fact]
    public async Task AcquireToken_AcquireLockAferTimeout_Abort()
    {
        var leaseInSeconds = 3;
        var token = (int)await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "AcquireLock_AcquireLockAferTimeout_Abort" },
            new RedisValue[]
            {
                $"{Environment.MachineName}-1",
                leaseInSeconds
            });

        token.Should().BeGreaterThan(0);

        await Task.Delay(TimeSpan.FromSeconds(leaseInSeconds));

        token = (int)await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "AcquireLock_AcquireLockAferTimeout_Abort" },
            new RedisValue[]
            {
                        $"{Environment.MachineName}-2",
                        leaseInSeconds
            });

        //Note: lock is acquired by different client after timeout
        token.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task AcquireToken_AcquiredBySameClient_FencingToken()
    {
        var leaseInSeconds = 3;
        var token = (int)await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "AcquireLock_AcquiredBySameClient_FencingToken" },
            new RedisValue[]
            {
                $"{Environment.MachineName}-1",
                leaseInSeconds
            });

        token.Should().BeGreaterThan(0);

        token = (int)await _luaScriptClient.ExecuteAsync(CoordinationLuaScripts.AcquireToken,
            new RedisKey[] { "mylock" },
            new RedisValue[]
            {
                        $"{Environment.MachineName}-1",
                        leaseInSeconds
            });

        //Note: lock can be acquired by same client even if timeout is not reached yet
        token.Should().BeGreaterThan(0);
    }
}

