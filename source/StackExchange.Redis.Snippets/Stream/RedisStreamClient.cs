using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.Snippets.Stream;

public class RedisStreamClient : RedisClient
{
    public RedisStreamClient(string connectionString, ILogger<RedisClient> logger) 
        : base(connectionString, logger) { }

    public async Task CreateStreamAndConsumerGroupAsync(RedisKey streamId, RedisValue groupName)
    {
        //Note: we check if stream exists otherwise we create stream wenn we create consumer group
        var streamExists = await RetryOnRedisExceptionAsync((db) => db.KeyExistsAsync(streamId)).ConfigureAwait(false);

        StreamGroupInfo[]? groups = null;
        if (streamExists)
        {
            groups = await RetryOnRedisExceptionAsync((db) => db.StreamGroupInfoAsync(streamId, CommandFlags.DemandMaster)).ConfigureAwait(false);
        }

        if (!streamExists || groups?.Count(_ => _.Name.Equals(groupName, StringComparison.InvariantCulture)) == 0)
        {
            await RetryOnRedisExceptionAsync(
                    (db) => db.StreamCreateConsumerGroupAsync(streamId, groupName, StreamPosition.Beginning, true, CommandFlags.DemandMaster))
                .ConfigureAwait(false);
        }
    }

    public async Task<long> AcknowledgStreamEntryAsync(RedisKey streamId, RedisValue groupName, RedisValue entryId)
    {
        return await RetryOnRedisExceptionAsync((db) => db.StreamAcknowledgeAsync(streamId, groupName, entryId, CommandFlags.DemandMaster))
            .ConfigureAwait(false);
    }

    public async Task<IReadOnlyCollection<RedisValue>> ClaimStreamEntryIdsAsync(RedisKey streamId, RedisValue groupName, string clientId, TimeSpan idleTimeout)
    {
        var pendingEntries = new List<string>();

        var entries = await RetryOnRedisExceptionAsync((db) => db.StreamPendingAsync(streamId, groupName, CommandFlags.DemandMaster))
            .ConfigureAwait(false);

        foreach (var consumer in entries.Consumers)
        {
            var claimableEntries = await RetryOnRedisExceptionAsync(
                    (db) => db.StreamPendingMessagesAsync(streamId, groupName, consumer.PendingMessageCount, consumer.Name, flags: CommandFlags.DemandMaster))
                .ConfigureAwait(false);

            foreach (var claimableEntry in claimableEntries)
            {
                if (claimableEntry.IdleTimeInMilliseconds > idleTimeout.TotalMilliseconds)
                {
                    //note: make stuck changelog-entries visible in log
                    if (claimableEntry.DeliveryCount > 10000)
                    {
                        Logger.LogWarning($"Stuck changeLog-entry {claimableEntry.MessageId} {claimableEntry.ConsumerName} detected, delivery count {claimableEntry.DeliveryCount}, groupName {groupName}, streamId {streamId}");
                    }

                    pendingEntries.Add(claimableEntry.MessageId);
                }
            }
        }

        if (pendingEntries.Count > 0)
        {
            var claimedEntries = await RetryOnRedisExceptionAsync(
                    (db) => db.StreamClaimAsync(streamId, groupName, clientId, (long)TimeSpan.FromSeconds(30).TotalMilliseconds, messageIds: pendingEntries.Select(_ => new RedisValue(_)).ToArray(), flags: CommandFlags.DemandMaster))
                .ConfigureAwait(false);

            return claimedEntries.Select(c => c.Id).ToList();
        }

        return Array.Empty<RedisValue>();
    }

    public async Task<IReadOnlyCollection<StreamResult>> ReadStreamEntriesAsync(RedisKey streamId, RedisValue groupName, RedisValue clientId, bool readPending, int count = 500)
    {
        var result = await RetryOnRedisExceptionAsync((db) => db.StreamReadGroupAsync(streamId, groupName, clientId, position: readPending ? StreamPosition.Beginning : StreamPosition.NewMessages, count: count, flags: CommandFlags.DemandMaster))
            .ConfigureAwait(false);

        return result.Select(_ => new StreamResult { Id = _.Id, Values = _.Values }).ToList();
    }

    public async Task<IReadOnlyCollection<string>> RemoveObsoleteClientsAsync(RedisKey streamId, RedisValue groupName, TimeSpan idleTimeout)
    {
        var consumerInfos = await RetryOnRedisExceptionAsync((db) => db.StreamConsumerInfoAsync(streamId, groupName, CommandFlags.DemandMaster))
            .ConfigureAwait(false);

        var deletedClients = new List<string>();
        foreach (var consumerInfo in consumerInfos)
        {
            if (consumerInfo.IdleTimeInMilliseconds > idleTimeout.TotalMilliseconds)
            {
                await RetryOnRedisExceptionAsync((db) => db.StreamDeleteConsumerAsync(streamId, groupName, consumerInfo.Name, CommandFlags.DemandMaster))
                    .ConfigureAwait(false);

                deletedClients.Add(consumerInfo.Name);
            }
        }

        return deletedClients;
    }

    public async Task<string> WriteStreamEntryAsync(RedisKey streamId, NameValueEntry[] streamPairs)
    {
        return await RetryOnRedisExceptionAsync((db) => db.StreamAddAsync(streamId, streamPairs, maxLength: 10000, useApproximateMaxLength: true, flags: CommandFlags.DemandMaster)).ConfigureAwait(false);
    }

    public async Task<IReadOnlyCollection<StreamResult>> ReadStreamEntriesAsync(RedisKey streamId, RedisValue? position = null)
    {
        //IMPORTANT: max count is set to max size of stream, so the whole stream can be loaded with one request
        var result = await RetryOnRedisExceptionAsync((db) => db.StreamReadAsync(streamId, position: position ?? StreamPosition.Beginning, count: 10000, flags: CommandFlags.DemandMaster))
            .ConfigureAwait(false);

        return result.Select(_ => new StreamResult { Id = _.Id, Values = _.Values }).ToList();
    }
}

