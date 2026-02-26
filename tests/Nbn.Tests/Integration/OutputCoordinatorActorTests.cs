using System.Threading.Channels;
using Nbn.Proto.Io;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.IO;
using Nbn.Tests.TestSupport;
using Proto;

namespace Nbn.Tests.Integration;

public sealed class OutputCoordinatorActorTests
{
    [Fact]
    public async Task OutputVector_SingleShard_PreservesFixedWidth_And_RejectsInvalidPayloads()
    {
        using var metrics = new MeterCollector(IoTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var brainId = Guid.NewGuid();
        var root = system.Root;

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth: 2)));
            var vectors = Channel.CreateUnbounded<OutputVectorEvent>();
            root.Spawn(Props.FromProducer(() => new VectorSubscriberProbe(brainId, coordinator, vectors.Writer)));

            await Task.Delay(100);

            root.Send(coordinator, new EmitOutput(OutputIndex: 5, Value: 1f, TickId: 1));
            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 10f, 20f }, 2));

            var first = await ReadVectorAsync(vectors.Reader, TimeSpan.FromSeconds(2));
            Assert.Equal((ulong)2, first.TickId);
            Assert.Equal(new[] { 10f, 20f }, first.Values);

            var mismatched = new OutputVectorEvent
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 3
            };
            mismatched.Values.Add(new[] { 1f, 2f, 3f });
            root.Send(coordinator, mismatched);

            await AssertNoVectorAsync(vectors.Reader, TimeSpan.FromMilliseconds(150));

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.io.output.single.rejected",
                    ("brain_id", brainId.ToString("D")),
                    ("reason", "output_index_out_of_range"),
                    ("output_width", "2")));

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.io.output.vector.rejected",
                    ("brain_id", brainId.ToString("D")),
                    ("reason", "vector_width_mismatch"),
                    ("output_width", "2")));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task OutputVector_MultiShardSegments_AggregateIntoSingleOrderedVectorPerTick()
    {
        using var metrics = new MeterCollector(IoTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var brainId = Guid.NewGuid();
        var root = system.Root;

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth: 6)));
            var vectors = Channel.CreateUnbounded<OutputVectorEvent>();
            root.Spawn(Props.FromProducer(() => new VectorSubscriberProbe(brainId, coordinator, vectors.Writer)));

            await Task.Delay(100);

            root.Send(coordinator, new EmitOutputVectorSegment(4, new[] { 4f, 5f }, 11));
            root.Send(coordinator, new EmitOutputVectorSegment(2, new[] { 2f, 3f }, 11));
            await AssertNoVectorAsync(vectors.Reader, TimeSpan.FromMilliseconds(150));

            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 0f, 1f }, 11));

            var vector = await ReadVectorAsync(vectors.Reader, TimeSpan.FromSeconds(2));
            Assert.Equal((ulong)11, vector.TickId);
            Assert.Equal(new[] { 0f, 1f, 2f, 3f, 4f, 5f }, vector.Values);

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.io.output.vector.published",
                    ("brain_id", brainId.ToString("D")),
                    ("output_width", "6")));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task OutputVector_CompletedTickSegment_IsRejected_AndDoesNotEmitDuplicateVector()
    {
        using var metrics = new MeterCollector(IoTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var brainId = Guid.NewGuid();
        var root = system.Root;

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth: 2)));
            var vectors = Channel.CreateUnbounded<OutputVectorEvent>();
            root.Spawn(Props.FromProducer(() => new VectorSubscriberProbe(brainId, coordinator, vectors.Writer)));

            await Task.Delay(100);

            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 3f, 4f }, 7));
            var first = await ReadVectorAsync(vectors.Reader, TimeSpan.FromSeconds(2));
            Assert.Equal((ulong)7, first.TickId);
            Assert.Equal(new[] { 3f, 4f }, first.Values);

            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 9f, 9f }, 7));
            await AssertNoVectorAsync(vectors.Reader, TimeSpan.FromMilliseconds(150));

            await WaitForMetricSumAsync(
                () => metrics.SumLong(
                    "nbn.io.output.vector.published",
                    ("brain_id", brainId.ToString("D")),
                    ("output_width", "2")),
                minValue: 1,
                timeout: TimeSpan.FromSeconds(2));

            await WaitForMetricSumAsync(
                () => metrics.SumLong(
                    "nbn.io.output.vector.rejected",
                    ("brain_id", brainId.ToString("D")),
                    ("reason", "tick_already_completed"),
                    ("output_width", "2")),
                minValue: 1,
                timeout: TimeSpan.FromSeconds(2));

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.io.output.vector.published",
                    ("brain_id", brainId.ToString("D")),
                    ("output_width", "2")));

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.io.output.vector.rejected",
                    ("brain_id", brainId.ToString("D")),
                    ("reason", "tick_already_completed"),
                    ("output_width", "2")));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task OutputVector_OverlappingShardPayload_IsRejected_AndSurfacedViaTelemetry()
    {
        using var metrics = new MeterCollector(IoTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var brainId = Guid.NewGuid();
        var root = system.Root;

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth: 4)));
            var vectors = Channel.CreateUnbounded<OutputVectorEvent>();
            root.Spawn(Props.FromProducer(() => new VectorSubscriberProbe(brainId, coordinator, vectors.Writer)));

            await Task.Delay(100);

            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 1f, 2f }, 20));
            root.Send(coordinator, new EmitOutputVectorSegment(1, new[] { 9f, 9f }, 20));
            root.Send(coordinator, new EmitOutputVectorSegment(2, new[] { 3f, 4f }, 20));

            var vector = await ReadVectorAsync(vectors.Reader, TimeSpan.FromSeconds(2));
            Assert.Equal(new[] { 1f, 2f, 3f, 4f }, vector.Values);

            Assert.Equal(
                1,
                metrics.SumLong(
                    "nbn.io.output.vector.rejected",
                    ("brain_id", brainId.ToString("D")),
                    ("reason", "segment_overlap"),
                    ("output_width", "4")));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task OutputVector_UnsubscribedVectorSubscriber_StopsReceiving_WhileOthersContinue()
    {
        using var metrics = new MeterCollector(IoTelemetry.MeterNameValue);
        var system = new ActorSystem();
        var brainId = Guid.NewGuid();
        var root = system.Root;

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, outputWidth: 2)));
            var firstSubscriberVectors = Channel.CreateUnbounded<OutputVectorEvent>();
            var secondSubscriberVectors = Channel.CreateUnbounded<OutputVectorEvent>();
            var firstSubscriber = root.Spawn(Props.FromProducer(() => new VectorSubscriberProbe(brainId, coordinator, firstSubscriberVectors.Writer)));
            root.Spawn(Props.FromProducer(() => new VectorSubscriberProbe(brainId, coordinator, secondSubscriberVectors.Writer)));

            await Task.Delay(100);

            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 1f, 2f }, 40));

            var firstA = await ReadVectorAsync(firstSubscriberVectors.Reader, TimeSpan.FromSeconds(2));
            var firstB = await ReadVectorAsync(secondSubscriberVectors.Reader, TimeSpan.FromSeconds(2));
            Assert.Equal((ulong)40, firstA.TickId);
            Assert.Equal((ulong)40, firstB.TickId);
            Assert.Equal(new[] { 1f, 2f }, firstA.Values);
            Assert.Equal(new[] { 1f, 2f }, firstB.Values);

            root.Send(firstSubscriber, new UnsubscribeVectorOutputs());
            await Task.Delay(100);

            root.Send(coordinator, new EmitOutputVectorSegment(0, new[] { 3f, 4f }, 41));

            await AssertNoVectorAsync(firstSubscriberVectors.Reader, TimeSpan.FromMilliseconds(200));
            var secondB = await ReadVectorAsync(secondSubscriberVectors.Reader, TimeSpan.FromSeconds(2));
            Assert.Equal((ulong)41, secondB.TickId);
            Assert.Equal(new[] { 3f, 4f }, secondB.Values);

            Assert.Equal(
                2,
                metrics.SumLong(
                    "nbn.io.output.vector.published",
                    ("brain_id", brainId.ToString("D")),
                    ("output_width", "2")));
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    private static async Task<OutputVectorEvent> ReadVectorAsync(ChannelReader<OutputVectorEvent> reader, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        return await reader.ReadAsync(cts.Token);
    }

    private static async Task AssertNoVectorAsync(ChannelReader<OutputVectorEvent> reader, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            _ = await reader.ReadAsync(cts.Token);
            Assert.Fail("Expected no OutputVectorEvent.");
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task WaitForMetricSumAsync(Func<long> readMetric, long minValue, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!cts.IsCancellationRequested)
        {
            if (readMetric() >= minValue)
            {
                return;
            }

            await Task.Delay(25, cts.Token);
        }

        Assert.Fail($"Timed out waiting for metric sum to reach {minValue}.");
    }

    private sealed class VectorSubscriberProbe : IActor
    {
        private readonly Guid _brainId;
        private readonly PID _outputCoordinator;
        private readonly ChannelWriter<OutputVectorEvent> _writer;

        public VectorSubscriberProbe(Guid brainId, PID outputCoordinator, ChannelWriter<OutputVectorEvent> writer)
        {
            _brainId = brainId;
            _outputCoordinator = outputCoordinator;
            _writer = writer;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started:
                    context.Request(_outputCoordinator, new SubscribeOutputsVector { BrainId = _brainId.ToProtoUuid() });
                    break;
                case UnsubscribeVectorOutputs:
                    context.Request(_outputCoordinator, new UnsubscribeOutputsVector { BrainId = _brainId.ToProtoUuid() });
                    break;
                case OutputVectorEvent output
                    when output.BrainId is not null
                         && output.BrainId.TryToGuid(out var brain)
                         && brain == _brainId:
                    _writer.TryWrite(output);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed record UnsubscribeVectorOutputs;
}
