using Microsoft.Extensions.Logging;

namespace StackExchange.Redis.Snippets.Stream.CompetitiveConsumer;

public class CompetitiveConsumer
{
    public const int BackOffDelayInMilliseconds = 500;

    private readonly string _streamId;
    private readonly string _consumerGroupId;
    private readonly string _clientId;

    private readonly RedisStreamClient _streamClient;
    private readonly ILogger<CompetitiveConsumer> _logger;
    private Func<StreamResult, CancellationToken, Task> _callback;

    private CancellationTokenSource? _tokenSource;

    public CompetitiveConsumer(
        RedisStreamClient streamClient,
        string streamId,
        string consumerGroupId,
        string clientId,
        Func<StreamResult, CancellationToken, Task> callback,
        ILogger<CompetitiveConsumer> logger)
    {
        _streamClient = streamClient ?? throw new ArgumentNullException(nameof(streamClient));
        _streamId = streamId ?? throw new ArgumentNullException(nameof(streamId));
        _consumerGroupId = consumerGroupId ?? throw new ArgumentNullException(nameof(consumerGroupId));
        _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
        _callback = callback ?? throw new ArgumentNullException(nameof(callback));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public DateTime? MainLoopExecutionAt { get; private set; }

    public DateTime? MaintenanceLoopExecutionAt { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        //Note: ensure existing scheduler is signaled to terminate 
        await StopAsync(cancellationToken).ConfigureAwait(false);

        //Note: ensure stream exists before writting to stream
        await _streamClient.CreateStreamAndConsumerGroupAsync(_streamId, _consumerGroupId).ConfigureAwait(false);

        _tokenSource = new CancellationTokenSource();

        _ = SchedulerLoopAsync($"{nameof(CompetitiveConsumer)}-{DateTime.UtcNow}", _tokenSource.Token);
        _ = MaintenanceSchedulerLoop($"{nameof(CompetitiveConsumer)}-{DateTime.UtcNow}", _tokenSource.Token);
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

    private async Task SchedulerLoopAsync(string name, CancellationToken cancellationToken)
    {
        //Note: spread boostrapping between multiple nodes so they are not in sync
        await Task.Delay(TimeSpan.FromMilliseconds(new Random().Next(10, BackOffDelayInMilliseconds)), cancellationToken).ConfigureAwait(false);

        var readFromBeginning = true;
        var pendingMessageCheckTime = NextPendingMessageCheck();
        MainLoopExecutionAt = DateTime.MinValue;

        do
        {
            try
            {
                //Note: backoff execute first to still have backoff in case of exceptions in flow below
                await Task.Delay(TimeSpan.FromMilliseconds(BackOffDelayInMilliseconds), cancellationToken).ConfigureAwait(false);

                //Note:  we don't cross check sequence, ghost records can happen in theory, if EXEC fails due to wrong data type, but that's very unlikely to reach PROD
                var streamEntries = await _streamClient.ReadStreamEntriesAsync(_streamId, _consumerGroupId, _clientId, readFromBeginning)
                    .ConfigureAwait(false);

                if (streamEntries.Count == 0)
                    readFromBeginning = false;

                foreach (var streamEntry in streamEntries)
                {
                    try
                    {
                        await _callback(streamEntry, cancellationToken).ConfigureAwait(false);

                        //Note: we acknowledge not corrupted stream entries to prevent endless retry which will result always in failure
                        await _streamClient.AcknowledgStreamEntryAsync(_streamId, _consumerGroupId, streamEntry.Id).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, $"Failed to process stream entry {streamEntry.Id} for {_streamId}");
                    }
                }

                if (pendingMessageCheckTime < DateTime.UtcNow)
                {
                    readFromBeginning = true;
                    pendingMessageCheckTime = NextPendingMessageCheck();
                }

                MainLoopExecutionAt = DateTime.UtcNow;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Failed to consume stream entries, will be retried");
            }

        } while (!cancellationToken.IsCancellationRequested);

        MainLoopExecutionAt = null;

        _logger.LogInformation($"Scheduler Loop {name} terminated");
    }

    private async Task MaintenanceSchedulerLoop(string name, CancellationToken cancellationToken)
    {
        var cleanupCheckTime = DateTime.MinValue;
        MainLoopExecutionAt = DateTime.MinValue;

        do
        {
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(BackOffDelayInMilliseconds), cancellationToken).ConfigureAwait(false);

                //Note: timeout is choosen to be less sensitive in trade of latency
                var claimableEntryIds = await _streamClient.ClaimStreamEntryIdsAsync(_streamId, _consumerGroupId, _clientId, TimeSpan.FromMilliseconds(BackOffDelayInMilliseconds * 4))
                    .ConfigureAwait(false);

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    foreach (var entryId in claimableEntryIds)
                        _logger.LogDebug("Client {0} claimed entry {1} for stream {2} since timeout has been reached", _clientId, entryId, _streamId);
                }

                if (cleanupCheckTime < DateTime.UtcNow)
                {
                    //Note: if client is not connected within 2 days will be removed from consumer group
                    await _streamClient.RemoveObsoleteClientsAsync(_streamId, _consumerGroupId, TimeSpan.FromDays(2))
                        .ConfigureAwait(false);

                    cleanupCheckTime = NextClientCleanUpCheck();
                }

                MaintenanceLoopExecutionAt = DateTime.UtcNow;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Failed to execute scheduler {name}");
            }

        } while (!cancellationToken.IsCancellationRequested);

        MaintenanceLoopExecutionAt = null;

        _logger.LogInformation($"Scheduler Loop {name} terminated");
    }

    private DateTime NextPendingMessageCheck()
    {
        //Note: pending message check done after 10 iteration
        return DateTime.UtcNow.AddMilliseconds(BackOffDelayInMilliseconds * 10);
    }

    private DateTime NextClientCleanUpCheck()
    {
        //Note: cleanup check done after 100 iteration
        return DateTime.UtcNow.AddMilliseconds(BackOffDelayInMilliseconds * 100);
    }
}

