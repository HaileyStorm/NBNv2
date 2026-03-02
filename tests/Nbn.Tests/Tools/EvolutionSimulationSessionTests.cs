using System.Security.Cryptography;
using System.Text;
using Nbn.Proto;
using Nbn.Shared;
using Nbn.Tools.EvolutionSim;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Tools;

public sealed class EvolutionSimulationSessionTests
{
    [Fact]
    public void InverseCompatibilityRunPolicy_MapsLowerSimilarityToHigherRunCount()
    {
        var policy = new InverseCompatibilityRunPolicy(1, 8, 1d);

        Assert.Equal(1u, policy.ResolveRunCount(1f));
        Assert.Equal(8u, policy.ResolveRunCount(0f));

        var highSimilarity = policy.ResolveRunCount(0.9f);
        var mediumSimilarity = policy.ResolveRunCount(0.55f);
        var lowSimilarity = policy.ResolveRunCount(0.2f);

        Assert.True(highSimilarity < mediumSimilarity);
        Assert.True(mediumSimilarity < lowSimilarity);
    }

    [Fact]
    public async Task RunAsync_WithSameSeed_ReplaysDeterministically()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 9918UL, maxIterations: 8, commitToSpeciation: true);

        var firstClient = new DeterministicFakeClient();
        var firstSession = new EvolutionSimulationSession(options, parents, firstClient);
        var firstStatus = await firstSession.RunAsync(CancellationToken.None);

        var secondClient = new DeterministicFakeClient();
        var secondSession = new EvolutionSimulationSession(options, parents, secondClient);
        var secondStatus = await secondSession.RunAsync(CancellationToken.None);

        Assert.Equal(firstClient.Events, secondClient.Events);
        Assert.Equal(firstStatus.Iterations, secondStatus.Iterations);
        Assert.Equal(firstStatus.CompatiblePairs, secondStatus.CompatiblePairs);
        Assert.Equal(firstStatus.SpeciationCommitSuccesses, secondStatus.SpeciationCommitSuccesses);
        Assert.True(firstStatus.SpeciationCommitSuccesses > 0);
    }

    [Fact]
    public async Task Controller_StartStop_AreIdempotent_AndStopCancelsActiveLoop()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 4421UL, maxIterations: 0, commitToSpeciation: false);
        var client = new DeterministicFakeClient(assessmentDelay: TimeSpan.FromSeconds(5));
        var session = new EvolutionSimulationSession(options, parents, client);
        var controller = new EvolutionSimulationController(session);

        Assert.True(controller.Start());
        Assert.False(controller.Start());

        await WaitForConditionAsync(
            () => controller.GetStatus().Running,
            TimeSpan.FromSeconds(2));

        Assert.True(await controller.StopAsync());
        Assert.False(controller.GetStatus().Running);
        Assert.False(await controller.StopAsync());
    }

    [Fact]
    public async Task RunAsync_UsesHigherRunCountAsCompatibilityDropsAcrossBands()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 555UL, maxIterations: 3, commitToSpeciation: false);
        var client = new DeterministicFakeClient(similarities: new[] { 0.95f, 0.55f, 0.2f });
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.Equal(3, client.RequestedRunCounts.Count);
        Assert.True(client.RequestedRunCounts[0] < client.RequestedRunCounts[1]);
        Assert.True(client.RequestedRunCounts[1] < client.RequestedRunCounts[2]);
        Assert.Equal((ulong)3, status.CompatiblePairs);
        Assert.Equal((ulong)3, status.ReproductionCalls);
    }

    private static IReadOnlyList<ArtifactRef> CreateParentPool()
    {
        return new[]
        {
            BuildArtifact("parent-a", "file:///tmp/a"),
            BuildArtifact("parent-b", "file:///tmp/b"),
            BuildArtifact("parent-c", "file:///tmp/c")
        };
    }

    private static EvolutionSimulationOptions CreateOptions(ulong seed, int maxIterations, bool commitToSpeciation)
    {
        return new EvolutionSimulationOptions
        {
            IoAddress = "127.0.0.1:12072",
            IoId = "io-gateway",
            Seed = seed,
            Interval = TimeSpan.Zero,
            MaxIterations = maxIterations,
            MaxParentPoolSize = 128,
            RequestTimeout = TimeSpan.FromSeconds(2),
            CommitToSpeciation = commitToSpeciation,
            SpawnChildren = false,
            StrengthSource = Repro.StrengthSource.StrengthBaseOnly,
            RunPolicy = new InverseCompatibilityRunPolicy(1, 8, 1d)
        };
    }

    private static ArtifactRef BuildArtifact(string value, string? storeUri = null)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        var sha = Convert.ToHexString(bytes).ToLowerInvariant();
        return sha.ToArtifactRef(sizeBytes: 256, mediaType: "application/x-nbn", storeUri: storeUri);
    }

    private static async Task WaitForConditionAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(predicate(), "Timed out waiting for condition.");
    }

    private sealed class DeterministicFakeClient : IEvolutionSimulationClient
    {
        private readonly Queue<float>? _similarityQueue;
        private readonly TimeSpan _assessmentDelay;

        public DeterministicFakeClient(IEnumerable<float>? similarities = null, TimeSpan? assessmentDelay = null)
        {
            _similarityQueue = similarities is null ? null : new Queue<float>(similarities);
            _assessmentDelay = assessmentDelay ?? TimeSpan.Zero;
        }

        public List<string> Events { get; } = new();
        public List<uint> RequestedRunCounts { get; } = new();

        public async Task<CompatibilityAssessment> AssessCompatibilityAsync(
            ArtifactRef parentA,
            ArtifactRef parentB,
            ulong seed,
            Repro.StrengthSource strengthSource,
            CancellationToken cancellationToken)
        {
            if (_assessmentDelay > TimeSpan.Zero)
            {
                await Task.Delay(_assessmentDelay, cancellationToken);
            }

            float similarity;
            if (_similarityQueue is { Count: > 0 })
            {
                similarity = _similarityQueue.Dequeue();
            }
            else
            {
                similarity = (float)((seed % 1000UL) / 999d);
            }

            var compatible = similarity >= 0.2f;
            Events.Add($"assess:{seed}:{Short(parentA)}:{Short(parentB)}:{similarity:F3}:{compatible}");
            return new CompatibilityAssessment(
                Success: true,
                Compatible: compatible,
                SimilarityScore: similarity,
                AbortReason: compatible ? string.Empty : "incompatible");
        }

        public Task<ReproductionOutcome> ReproduceAsync(
            ArtifactRef parentA,
            ArtifactRef parentB,
            ulong seed,
            uint runCount,
            bool spawnChildren,
            Repro.StrengthSource strengthSource,
            CancellationToken cancellationToken)
        {
            RequestedRunCounts.Add(runCount);
            Events.Add($"repro:{seed}:{runCount}:{Short(parentA)}:{Short(parentB)}");
            var child = BuildArtifact($"child:{seed}:{runCount}:{Short(parentA)}:{Short(parentB)}", parentA.StoreUri);
            return Task.FromResult(
                new ReproductionOutcome(
                    Success: true,
                    Compatible: true,
                    AbortReason: string.Empty,
                    ChildDefinitions: new[] { child }));
        }

        public Task<SpeciationCommitOutcome> CommitSpeciationAsync(
            ArtifactRef childDefinition,
            ArtifactRef parentA,
            ArtifactRef parentB,
            CancellationToken cancellationToken)
        {
            Events.Add($"speciation:{Short(childDefinition)}");
            return Task.FromResult(new SpeciationCommitOutcome(Success: true, FailureDetail: string.Empty));
        }

        private static string Short(ArtifactRef reference)
        {
            return reference.TryToSha256Hex(out var sha)
                ? sha[..8]
                : "missing";
        }
    }
}
