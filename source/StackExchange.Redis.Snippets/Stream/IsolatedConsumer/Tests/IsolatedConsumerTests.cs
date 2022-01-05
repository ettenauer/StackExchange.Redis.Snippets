
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace StackExchange.Redis.Snippets.Stream.IsolatedConsumer.Tests;

public class IsolatedConsumerTests
{
    private readonly RedisStreamClient _streamClient;
    private Mock<ILogger<RedisClient>> _loggerMock;
    private Mock<ILogger<IsolatedConsumer>> _consumerLoggerMock;

    public IsolatedConsumerTests()
    {
        _loggerMock = new Mock<ILogger<RedisClient>>();
        _consumerLoggerMock = new Mock<ILogger<IsolatedConsumer>>();
        _streamClient = new RedisStreamClient("localhost:6379", _loggerMock.Object);
    }

    [Fact]
    public async Task TwoConsumers_PendingMessages_MessagesAreSplit()
    {
        var consumerAEntries = new HashSet<string>();
        var consumerBEntries = new HashSet<string>();

        var consumerA = new IsolatedConsumer(streamClient: _streamClient,
            streamId: "isoStream1",
            consumerGroupId: "cg1",
            clientId: "clientA",
            callback: async (r, t) => { consumerAEntries.Add(r["id"]); },
            logger: _consumerLoggerMock.Object);

        var consumerB = new IsolatedConsumer(streamClient: _streamClient,
            streamId: "isoStream1",
            consumerGroupId: "cg1",
            clientId: "clientB",
            callback: async (r, t) => { consumerBEntries.Add(r["id"]); },
            logger: _consumerLoggerMock.Object);

        await consumerA.StartAsync(CancellationToken.None);
        await consumerB.StartAsync(CancellationToken.None);

        for (int i = 0; i < 1000; i++)
            await _streamClient.WriteStreamEntryAsync("isoStream1", new NameValueEntry[] { new NameValueEntry("id", i) });

        await Task.Delay(TimeSpan.FromSeconds(10));

        await consumerA.StopAsync(CancellationToken.None);
        await consumerB.StopAsync(CancellationToken.None);

        consumerAEntries.Count.Should().Be(1000);
        consumerBEntries.Count.Should().Be(1000);
    }
}

