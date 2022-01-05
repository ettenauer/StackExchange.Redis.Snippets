using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.Snippets.Stream.IsolatedConsumer;

public class IsolatedConsumer
{
    public const int BackOffDelayInMilliseconds = 500;

    private readonly string _streamId;
    private readonly string _consumerGroupId;
    private readonly string _clientId;

    private readonly RedisStreamClient _streamClient;
    private readonly ILogger<IsolatedConsumer> _logger;
    private Func<StreamResult, CancellationToken, Task> _callback;

    private CancellationTokenSource? _tokenSource;

    public IsolatedConsumer(
        RedisStreamClient streamClient,
        string streamId,
        string consumerGroupId,
        string clientId,
        Func<StreamResult, CancellationToken, Task> callback,
        ILogger<IsolatedConsumer> logger)
    {
        _streamClient = streamClient ?? throw new ArgumentNullException(nameof(streamClient));
        _streamId = streamId ?? throw new ArgumentNullException(nameof(streamId));
        _consumerGroupId = consumerGroupId ?? throw new ArgumentNullException(nameof(consumerGroupId));
        _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
        _callback = callback ?? throw new ArgumentNullException(nameof(callback));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public DateTime? ExecutedAt { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        //Note: ensure existing scheduler is signaled to terminate 
        await StopAsync(cancellationToken).ConfigureAwait(false);

        _tokenSource = new CancellationTokenSource();

        _ = MessageLoopAsync($"{nameof(IsolatedConsumer)}-{DateTime.UtcNow}", _tokenSource.Token);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_tokenSource?.IsCancellationRequested ?? true)
            return Task.CompletedTask;

        _tokenSource?.Cancel();
        _tokenSource?.Dispose();
        _tokenSource = null;

        return Task.CompletedTask;
    }

    //Note: we use loop in thread to have none-overlapping consumption pattern (would not be case with timer)
    private async Task MessageLoopAsync(string name, CancellationToken cancellationToken)
    {
        ExecutedAt = DateTime.MinValue;
        RedisValue? position = null;

        do
        {
            try
            {
                //Note: backoff execute first to still have backoff in case of exceptions in flow below
                await Task.Delay(TimeSpan.FromMilliseconds(BackOffDelayInMilliseconds), cancellationToken).ConfigureAwait(false);

                //Note: we load initially all records from stream, which will caus push of case of application will be started
                var streamEntries = await _streamClient.ReadStreamEntriesAsync(_streamId, position)
                    .ConfigureAwait(false);

                foreach (var streamEntry in streamEntries)
                {
                    try
                    {
                        await _callback(streamEntry, cancellationToken).ConfigureAwait(false);

                        position = streamEntry.Id;
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, $"Failed to process stream entry {streamEntry.Id} for {_streamId}");
                    }
                }

                ExecutedAt = DateTime.UtcNow;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Failed to consume stream entries, will be retried");
            }

        } while (!cancellationToken.IsCancellationRequested);

        ExecutedAt = null;

        _logger.LogInformation($"Scheduler Loop {name} terminated");
    }
}

