using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using System.Text;

namespace StackExchange.Redis.Snippets;

public abstract class RedisClient
{
    protected readonly ILogger<RedisClient> Logger;

    private bool _isDisposed;
    private readonly object _disposeLock = new();

    private readonly IConnectionMultiplexer _connection;
    private readonly ConfigurationOptions _configuration;
    private readonly AsyncRetryPolicy _retryPolicy;
   
    protected RedisClient(string connectionString, ILogger<RedisClient> logger)
    {
        ArgumentNullException.ThrowIfNull(connectionString, nameof(connectionString));

        Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _retryPolicy = Policy.Handle<RedisException>()
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
        _configuration = ConfigurationOptions.Parse(connectionString);
        _connection = ConnectionMultiplexer.Connect(_configuration);
        _connection.ConnectionFailed += MultiplexerOnConnectionFailed;
        _connection.ConnectionRestored += MultiplexerOnConnectionRestored;
        _connection.InternalError += MultiplexerOnInternalError;
    }

    protected async Task<T> RetryOnRedisExceptionAsync<T>(Func<IDatabase, Task<T>> func)
    {
        return await _retryPolicy.ExecuteAsync(() => func(_connection.GetDatabase())).ConfigureAwait(false);
    }

    private void MultiplexerOnConnectionFailed(object? sender, ConnectionFailedEventArgs connectionFailedEventArgs)
    {
        Logger.LogWarning("redis connection has failed:\n", Log(connectionFailedEventArgs));
    }

    private void MultiplexerOnConnectionRestored(object? sender, ConnectionFailedEventArgs connectionFailedEventArgs)
    {
        Logger.LogInformation("redis connection has been restored:\n", Log(connectionFailedEventArgs));
    }

    private void MultiplexerOnInternalError(object? sender, InternalErrorEventArgs internalErrorEventArgs)
    {
        var sb = new StringBuilder("internal error from redis client:");
        sb.AppendLine($"Origin: {internalErrorEventArgs.Origin}");
        sb.AppendLine($"ConnectionType: {internalErrorEventArgs.ConnectionType}");
        sb.AppendLine($"EndPoint: {internalErrorEventArgs.EndPoint?.ToString() ?? string.Empty}");
        Logger.LogError(internalErrorEventArgs.Exception, sb.ToString());
    }

    private static string Log(ConnectionFailedEventArgs connectionFailedEventArgs)
    {
        if (connectionFailedEventArgs == null)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        sb.AppendLine($"FailureType: {connectionFailedEventArgs.FailureType}");
        sb.AppendLine($"ConnectionType: {connectionFailedEventArgs.ConnectionType}");
        sb.AppendLine($"EndPoint: {connectionFailedEventArgs.EndPoint?.ToString() ?? string.Empty}");
        sb.AppendLine($"Exception: {connectionFailedEventArgs.EndPoint?.ToString() ?? string.Empty}");
        return sb.ToString();
    }

    public void Dispose()
    {
        if (_isDisposed)
            return;

        lock (_disposeLock)
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
        }
    }
}

