using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace StackExchange.Redis.Snippets.Stream.CompetitiveConsumer.Tests;

public class CompetitiveConsumerTests
{
    private readonly RedisStreamClient _streamClient;
    private Mock<ILogger<RedisClient>> _loggerMock;
    private Mock<ILogger<CompetitiveConsumer>> _consumerLoggerMock;

    public CompetitiveConsumerTests()
    {
        _loggerMock =  new Mock<ILogger<RedisClient>>();
        _consumerLoggerMock = new Mock<ILogger<CompetitiveConsumer>>();
        _streamClient = new RedisStreamClient("localhost:6379", _loggerMock.Object);
    }

    [Fact]
    public async Task TwoConsumers_PendingMessages_MessagesAreSplit()
    {   
        var consumerAEntries = new HashSet<string>();
        var consumerBEntries = new HashSet<string>();

        var consumerA = new CompetitiveConsumer(streamClient: _streamClient,
            streamId: "comStream1",
            consumerGroupId: "cg1",
            clientId: "clientA",
            callback: async (r, t) => { consumerAEntries.Add(r["id"]); },
            logger: _consumerLoggerMock.Object);

        var consumerB = new CompetitiveConsumer(streamClient: _streamClient,
            streamId: "comStream1",
            consumerGroupId: "cg1",
            clientId: "clientB",
            callback: async (r, t) => { consumerBEntries.Add(r["id"]); },
            logger: _consumerLoggerMock.Object);

        await consumerA.StartAsync(CancellationToken.None);
        await consumerB.StartAsync(CancellationToken.None);

        for (int i = 0; i < 1000; i++)
            await _streamClient.WriteStreamEntryAsync("comStream1", new NameValueEntry[] { new NameValueEntry("id", i) });

        await Task.Delay(TimeSpan.FromSeconds(10));

        await consumerA.StopAsync(CancellationToken.None);
        await consumerB.StopAsync(CancellationToken.None);

        consumerAEntries.Count.Should().Be(500);
        consumerBEntries.Count.Should().Be(500);
    }

    [Fact]
    public async Task TwoConsumers_StopOneConsumer_MessagesAreConsumeByOne()
    {
        var consumerAEntries = new HashSet<string>();
        var consumerBEntries = new HashSet<string>();

        var consumerA = new CompetitiveConsumer(streamClient: _streamClient,
            streamId: "comStream2",
            consumerGroupId: "cg1",
            clientId: "clientA",
            callback: async (r, t) => { consumerAEntries.Add(r["id"]); },
            logger: _consumerLoggerMock.Object);

        var consumerB = new CompetitiveConsumer(streamClient: _streamClient,
            streamId: "comStream2",
            consumerGroupId: "cg1",
            clientId: "clientB",
            callback: async (r, t) => { throw new Exception("test"); }, // disrupt consumer
            logger: _consumerLoggerMock.Object);

        await consumerA.StartAsync(CancellationToken.None);
        await consumerB.StartAsync(CancellationToken.None);

        for (int i = 0; i < 1000; i++)
            await _streamClient.WriteStreamEntryAsync("comStream2", new NameValueEntry[] { new NameValueEntry("id", i) });

        await consumerB.StopAsync(CancellationToken.None); // stop clientB to cause claim of messages by consumerA

        await Task.Delay(TimeSpan.FromSeconds(20));

        await consumerA.StopAsync(CancellationToken.None);

        consumerAEntries.Count.Should().Be(1000);
        consumerBEntries.Count.Should().Be(0);
    }
}

