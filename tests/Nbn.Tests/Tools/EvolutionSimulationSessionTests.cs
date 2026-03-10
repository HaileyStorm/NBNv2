using System.Security.Cryptography;
using System.Text;
using System.Reflection;
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

    [Fact]
    public async Task RunAsync_BrainIdParentMode_UsesBrainIds_AndCommitsWithoutSpawn()
    {
        var parents = CreateBrainParentPool();
        var options = CreateOptions(
            seed: 7001UL,
            maxIterations: 4,
            commitToSpeciation: true,
            parentMode: EvolutionParentMode.BrainIds);
        var client = new DeterministicFakeClient();
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.True(client.ObservedBrainIdParents);
        Assert.Equal(
            parents.Count,
            client.CommittedCandidates.Take(parents.Count).Count(candidate => candidate.ChildBrainId.HasValue));
        Assert.True(status.ReproductionCalls > 0);
        Assert.True(status.SpeciationCommitAttempts > 0);
        Assert.Equal(status.SpeciationCommitAttempts, status.SpeciationCommitSuccesses);
    }

    [Fact]
    public async Task RunAsync_BrainIdParentMode_WithoutSpawn_AddsCommittedArtifactChildrenToPool()
    {
        var parents = CreateBrainParentPool();
        var options = CreateOptions(
            seed: 7002UL,
            maxIterations: 24,
            commitToSpeciation: true,
            parentMode: EvolutionParentMode.BrainIds);
        var client = new DeterministicFakeClient(similarities: Enumerable.Repeat(0.90f, 128))
        {
            CommitCandidateSimilarity = 0.90f,
            ReproductionDiagnosticSimilarity = 0.90f
        };
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.True(client.ObservedBrainIdParents);
        Assert.True(client.ObservedArtifactParents);
        Assert.True(status.ChildrenAddedToPool > 0);
        Assert.True(status.ParentPoolSize > parents.Count);
    }

    [Fact]
    public async Task RunAsync_CommitCandidates_FallBackToAssessmentSimilarity_WhenMissing()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 1442UL, maxIterations: 2, commitToSpeciation: true);
        var client = new DeterministicFakeClient(similarities: new[] { 0.35f, 0.55f });
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.True(status.SpeciationCommitAttempts > 0);
        Assert.NotEmpty(client.CommittedCandidates);
        var seededFounderCount = parents
            .Select(parent => BuildBrainParentKeyOrArtifactKey(parent))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        var iterationCommitCandidates = client.CommittedCandidates
            .Skip(seededFounderCount)
            .ToArray();
        Assert.NotEmpty(iterationCommitCandidates);
        Assert.All(iterationCommitCandidates, candidate =>
        {
            Assert.True(candidate.SimilarityScore.HasValue);
            Assert.InRange(candidate.SimilarityScore.Value, 0f, 1f);
        });
    }

    [Fact]
    public async Task RunAsync_PrefersCommittedSourceSpeciesSimilarity_ForCommitTelemetry()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 9123UL, maxIterations: 2, commitToSpeciation: true);
        var client = new DeterministicFakeClient(similarities: new[]
        {
            0.81f, 0.82f, 0.83f, // founder seeding assessments
            0.34f, 0.93f         // iteration assessments
        })
        {
            ReproductionDiagnosticSimilarity = 0.82f,
            CommitCandidateSimilarity = 0.82f,
            CommitOutcomeSourceSpeciesSimilarity = 0.61f
        };
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.Equal((ulong)2, status.AssessmentSimilaritySamples);
        Assert.Equal(0.34f, status.MinAssessmentSimilarityObserved, 3);
        Assert.Equal((ulong)2, status.ReproductionSimilaritySamples);
        Assert.Equal(0.82f, status.MinReproductionSimilarityObserved, 3);
        Assert.True(status.SpeciationCommitSimilaritySamples >= 2);
        Assert.Equal(0.61f, status.MinSpeciationCommitSimilarityObserved, 3);
        Assert.True(status.MinSimilarityObserved <= status.MinAssessmentSimilarityObserved);
        Assert.True(status.MinAssessmentSimilarityObserved < status.MinSpeciationCommitSimilarityObserved);
        Assert.True(status.MinSpeciationCommitSimilarityObserved < status.MinReproductionSimilarityObserved);
    }

    [Fact]
    public async Task RunAsync_CommitTelemetryFallsBackToCandidateSimilarity_WhenOutcomeOmitsCommittedSimilarity()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 9124UL, maxIterations: 2, commitToSpeciation: true);
        var client = new DeterministicFakeClient(similarities: new[]
        {
            0.81f, 0.82f, 0.83f, // founder seeding assessments
            0.34f, 0.93f         // iteration assessments
        })
        {
            ReproductionDiagnosticSimilarity = 0.82f,
            CommitCandidateSimilarity = 0.82f
        };
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.True(status.SpeciationCommitSimilaritySamples >= 2);
        Assert.Equal(0.82f, status.MinSpeciationCommitSimilarityObserved, 3);
    }

    [Fact]
    public async Task RunAsync_SeedsInitialParentsIntoSpeciation_BeforeIterationCommits()
    {
        var parents = CreateBrainParentPool();
        var options = CreateOptions(
            seed: 7811UL,
            maxIterations: 1,
            commitToSpeciation: true,
            parentMode: EvolutionParentMode.BrainIds);
        var client = new DeterministicFakeClient();
        var session = new EvolutionSimulationSession(options, parents, client);

        await session.RunAsync(CancellationToken.None);

        var initialParentIds = parents
            .Where(parent => parent.BrainId.HasValue)
            .Select(parent => parent.BrainId!.Value)
            .ToHashSet();
        Assert.NotEmpty(initialParentIds);
        Assert.Contains(
            client.CommittedCandidates,
            candidate => candidate.ChildBrainId.HasValue && initialParentIds.Contains(candidate.ChildBrainId.Value));

        var firstSpeciationIndex = client.Events.FindIndex(
            static entry => entry.StartsWith("speciation:", StringComparison.Ordinal));
        var firstReproIndex = client.Events.FindIndex(
            static entry => entry.StartsWith("repro:", StringComparison.Ordinal));
        Assert.True(firstSpeciationIndex >= 0);
        Assert.True(firstReproIndex < 0 || firstSpeciationIndex < firstReproIndex);
    }

    [Fact]
    public async Task SeedInitialParentsAsync_BrainMode_PrefersBestSeededPartnerForDuplicateFounderInstances()
    {
        var parents = CreateOrderedBrainParentPool(4);
        var options = CreateOptions(
            seed: 78115UL,
            maxIterations: 1,
            commitToSpeciation: true,
            parentMode: EvolutionParentMode.BrainIds);
        var client = new DeterministicFakeClient(similarities: new[]
        {
            1.0f,        // parent 2 -> parent 1
            0.0f, 0.0f,  // parent 3 -> parents 1/2
            0.0f, 0.0f, 1.0f // parent 4 -> parents 1/2/3
        });
        var session = new EvolutionSimulationSession(options, parents, client);

        var seedInitialParentsMethod = typeof(EvolutionSimulationSession).GetMethod(
            "SeedInitialParentsAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(seedInitialParentsMethod);

        var seedTask = Assert.IsAssignableFrom<Task>(
            seedInitialParentsMethod!.Invoke(session, [CancellationToken.None]));
        await seedTask;

        Assert.Equal(4, client.CommittedParentPairs.Count);
        Assert.Equal(
            BuildBrainParentKey(parents[0]),
            client.CommittedParentPairs[1].ParentBKey);
        Assert.Equal(
            BuildBrainParentKey(parents[2]),
            client.CommittedParentPairs[3].ParentBKey);
    }

    [Fact]
    public async Task RunAsync_SeedParents_UseAssessedFounderSimilarityInsteadOfSyntheticPerfectScores()
    {
        var parents = CreateDuplicatedBrainParentPool();
        var options = CreateOptions(
            seed: 7812UL,
            maxIterations: 1,
            commitToSpeciation: true,
            parentMode: EvolutionParentMode.BrainIds);
        var client = new DeterministicFakeClient(similarities: new[] { 0.24f, 0.24f, 0.90f });
        var session = new EvolutionSimulationSession(options, parents, client);

        await session.RunAsync(CancellationToken.None);

        var uniqueFounderIds = parents
            .Where(parent => parent.BrainId.HasValue)
            .Select(parent => parent.BrainId!.Value)
            .Distinct()
            .ToHashSet();
        var founderSeedCommits = client.CommittedCandidates
            .Where(candidate => candidate.ChildBrainId.HasValue && uniqueFounderIds.Contains(candidate.ChildBrainId.Value))
            .ToArray();

        Assert.Equal(2, founderSeedCommits.Length);
        Assert.False(founderSeedCommits[0].SimilarityScore.HasValue);
        Assert.False(founderSeedCommits[0].LineageSimilarityScore.HasValue);
        Assert.False(founderSeedCommits[0].LineageParentASimilarityScore.HasValue);
        Assert.False(founderSeedCommits[0].LineageParentBSimilarityScore.HasValue);
        Assert.True(founderSeedCommits[1].SimilarityScore.HasValue);
        Assert.True(founderSeedCommits[1].LineageSimilarityScore.HasValue);
        Assert.True(founderSeedCommits[1].LineageParentASimilarityScore.HasValue);
        Assert.True(founderSeedCommits[1].LineageParentBSimilarityScore.HasValue);
        Assert.Equal(0.24f, founderSeedCommits[1].SimilarityScore.GetValueOrDefault(), 3);
        Assert.Equal(0.24f, founderSeedCommits[1].LineageSimilarityScore.GetValueOrDefault(), 3);
        Assert.Equal(1f, founderSeedCommits[1].LineageParentASimilarityScore.GetValueOrDefault(), 3);
        Assert.Equal(0.24f, founderSeedCommits[1].LineageParentBSimilarityScore.GetValueOrDefault(), 3);

        var firstAssessIndex = client.Events.FindIndex(
            static entry => entry.StartsWith("assess:", StringComparison.Ordinal));
        var secondSpeciationIndex = client.Events.FindLastIndex(
            static entry => entry.StartsWith("speciation:", StringComparison.Ordinal));
        Assert.True(firstAssessIndex >= 0);
        Assert.True(secondSpeciationIndex > firstAssessIndex);
    }

    [Fact]
    public async Task RunAsync_SeedParents_TreatEquivalentArtifactExportsAsSameFounderAcrossStoreUris()
    {
        var parents = CreateDuplicatedArtifactParentPool();
        var options = CreateOptions(seed: 7813UL, maxIterations: 1, commitToSpeciation: true);
        var client = new DeterministicFakeClient(similarities: new[] { 0.24f, 0.90f });
        var session = new EvolutionSimulationSession(options, parents, client);

        await session.RunAsync(CancellationToken.None);

        var uniqueFounderShas = parents
            .Where(parent => parent.ArtifactRef is not null)
            .Select(parent => parent.ArtifactRef!.ToSha256Hex())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var founderSeedCommits = client.CommittedCandidates
            .Where(candidate => candidate.ChildDefinition is not null
                && uniqueFounderShas.Contains(candidate.ChildDefinition.ToSha256Hex()))
            .ToArray();

        Assert.Equal(2, founderSeedCommits.Length);
        Assert.False(founderSeedCommits[0].SimilarityScore.HasValue);
        Assert.Equal(0.24f, founderSeedCommits[1].SimilarityScore.GetValueOrDefault(), 3);
        Assert.Equal(0.24f, founderSeedCommits[1].LineageSimilarityScore.GetValueOrDefault(), 3);

        var firstAssess = client.Events.First(
            static entry => entry.StartsWith("assess:", StringComparison.Ordinal));
        var firstAssessParts = firstAssess.Split(':');
        Assert.True(firstAssessParts.Length >= 6);
        Assert.NotEqual(firstAssessParts[2], firstAssessParts[3]);
    }

    [Fact]
    public async Task RunAsync_SeedParents_TreatInitialFounderSpeciesAsCoevalForSelectionBias()
    {
        var parents = CreateDuplicatedBrainParentPool();
        var options = CreateOptions(
            seed: 7814UL,
            maxIterations: 1,
            commitToSpeciation: true,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Stability,
            RunPressureMode = EvolutionRunPressureMode.Neutral
        };
        var client = new DeterministicFakeClient(similarities: new[] { 0.24f, 0.1f });
        client.CommitOutcomeSpeciesIds.Enqueue("species-alpha");
        client.CommitOutcomeSourceSpeciesIds.Enqueue(string.Empty);
        client.CommitOutcomeSpeciesIds.Enqueue("species-beta");
        client.CommitOutcomeSourceSpeciesIds.Enqueue(string.Empty);
        var session = new EvolutionSimulationSession(options, parents, client);

        await session.RunAsync(CancellationToken.None);

        var selections = SampleSelectedSpeciesCounts(session, sampleCount: 6_000, excludedIndex: -1);

        Assert.True(selections.TryGetValue("species-alpha", out var alphaCount));
        Assert.True(selections.TryGetValue("species-beta", out var betaCount));
        var ratio = alphaCount / (double)betaCount;
        Assert.InRange(ratio, 0.85d, 1.15d);
    }

    [Fact]
    public async Task RunAsync_WithDuplicatedLogicalParents_SelectsDistinctParentKeysWhenAvailable()
    {
        var parents = CreateDuplicatedBrainParentPool();
        var options = CreateOptions(
            seed: 7815UL,
            maxIterations: 64,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Neutral,
            RunPressureMode = EvolutionRunPressureMode.Neutral
        };
        var client = new DeterministicFakeClient(similarities: Enumerable.Repeat(0.90f, 256));
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.Equal((ulong)64, status.CompatibilityChecks);
        Assert.NotEmpty(client.AssessedParentPairs);
        Assert.DoesNotContain(
            client.AssessedParentPairs,
            static pair => string.Equals(pair.ParentAKey, pair.ParentBKey, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task RunAsync_WhenParentPoolReachesCapacity_ContinuesTurnoverWithNewChildren()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 42001UL, maxIterations: 12, commitToSpeciation: false) with
        {
            MaxParentPoolSize = 4
        };
        var client = new DeterministicFakeClient(similarities: Enumerable.Repeat(0.90f, 32));
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.Equal(4, status.ParentPoolSize);
        Assert.Equal((ulong)12, status.ReproductionCalls);
        Assert.True(status.ChildrenAddedToPool > 1, "Expected replacement-based turnover after reaching capacity.");
    }

    [Fact]
    public async Task RunAsync_WhenDominantSpeciesAlreadyFillsPool_DoesNotEvictRarerCommittedSpecies()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 42002UL, maxIterations: 8, commitToSpeciation: true) with
        {
            MaxParentPoolSize = 6
        };
        var client = new DeterministicFakeClient(similarities: Enumerable.Repeat(0.90f, 64));
        foreach (var speciesId in new[]
                 {
                     "species-alpha",
                     "species-alpha",
                     "species-alpha",
                     "species-beta",
                     "species-gamma",
                     "species-delta",
                     "species-alpha",
                     "species-alpha",
                     "species-alpha",
                     "species-alpha",
                     "species-alpha"
                 })
        {
            client.CommitOutcomeSpeciesIds.Enqueue(speciesId);
        }

        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        var speciesCounts = SnapshotParentPoolSpeciesCounts(session);
        Assert.Equal(6, status.ParentPoolSize);
        Assert.Equal((ulong)8, status.SpeciationCommitSuccesses);
        Assert.Equal((ulong)3, status.ChildrenAddedToPool);
        Assert.Equal(3, speciesCounts["species-alpha"]);
        Assert.Equal(1, speciesCounts["species-beta"]);
        Assert.Equal(1, speciesCounts["species-gamma"]);
        Assert.Equal(1, speciesCounts["species-delta"]);
    }

    [Fact]
    public async Task RunAsync_WhenCommitSimilarityPlateaus_IncreasesMutationPressureDeterministically()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 77001UL, maxIterations: 224, commitToSpeciation: true);
        var client = new DeterministicFakeClient(similarities: Enumerable.Repeat(0.90f, 1024))
        {
            CommitCandidateSimilarity = 0.95f,
            ReproductionDiagnosticSimilarity = 0.90f
        };
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.Equal((ulong)224, status.ReproductionCalls);
        Assert.Equal((ulong)224, status.SpeciationCommitSuccesses);
        Assert.True(client.RequestedRunCounts.Count > 100);
        Assert.True(
            client.RequestedRunCounts.Max() > client.RequestedRunCounts.Min(),
            "Expected adaptive run pressure to raise run_count after commit-similarity plateau.");
    }

    [Fact]
    public async Task RunAsync_WhenCommitSimilarityPlateaus_StabilityModeLowersRunPressureDeterministically()
    {
        var parents = CreateParentPool();
        var options = CreateOptions(seed: 77002UL, maxIterations: 224, commitToSpeciation: true) with
        {
            RunPressureMode = EvolutionRunPressureMode.Stability
        };
        var client = new DeterministicFakeClient(similarities: Enumerable.Repeat(0.90f, 1024))
        {
            CommitCandidateSimilarity = 0.95f,
            ReproductionDiagnosticSimilarity = 0.90f
        };
        var session = new EvolutionSimulationSession(options, parents, client);

        var status = await session.RunAsync(CancellationToken.None);

        Assert.Equal((ulong)224, status.ReproductionCalls);
        Assert.Equal((ulong)224, status.SpeciationCommitSuccesses);
        Assert.True(client.RequestedRunCounts.Count > 100);
        Assert.True(
            client.RequestedRunCounts.Max() > client.RequestedRunCounts.Min(),
            "Expected stability mode to reduce run_count after commit-similarity plateau.");
        Assert.True(
            client.RequestedRunCounts[^1] <= client.RequestedRunCounts[0],
            "Expected later run_count values to be lower or equal under stability mode.");
    }

    [Fact]
    public async Task RunAsync_ParentSelectionBias_DivergencePrefersNewerParentsThanStability()
    {
        var parents = CreateOrderedBrainParentPool(6);
        var rankByBrain = parents
            .Select((parent, index) => (parent, index))
            .Where(entry => entry.parent.BrainId.HasValue)
            .ToDictionary(entry => entry.parent.BrainId!.Value, entry => entry.index);
        var similarities = Enumerable.Repeat(0.9f, 2000).ToArray();

        var divergenceOptions = CreateOptions(
            seed: 99001UL,
            maxIterations: 400,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Divergence,
            RunPressureMode = EvolutionRunPressureMode.Neutral
        };

        var stabilityOptions = divergenceOptions with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Stability
        };

        var divergenceClient = new DeterministicFakeClient(similarities: similarities);
        var divergenceSession = new EvolutionSimulationSession(divergenceOptions, parents, divergenceClient);
        await divergenceSession.RunAsync(CancellationToken.None);

        var stabilityClient = new DeterministicFakeClient(similarities: similarities);
        var stabilitySession = new EvolutionSimulationSession(stabilityOptions, parents, stabilityClient);
        await stabilitySession.RunAsync(CancellationToken.None);

        Assert.NotEmpty(divergenceClient.ObservedBrainSelections);
        Assert.NotEmpty(stabilityClient.ObservedBrainSelections);

        var divergenceAverageRank = divergenceClient.ObservedBrainSelections
            .Select(brainId => rankByBrain[brainId])
            .Average();
        var stabilityAverageRank = stabilityClient.ObservedBrainSelections
            .Select(brainId => rankByBrain[brainId])
            .Average();

        Assert.True(
            divergenceAverageRank > stabilityAverageRank,
            $"Expected divergence bias to favor newer parents. divergence={divergenceAverageRank:0.###}, stability={stabilityAverageRank:0.###}");
    }

    [Fact]
    public void SelectParentIndex_WithMultipleSpecies_NormalizesSelectionBySpeciesRepresentation()
    {
        var parents = CreateOrderedBrainParentPool(6);
        var options = CreateOptions(
            seed: 99123UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Divergence
        };
        var session = new EvolutionSimulationSession(options, parents, new DeterministicFakeClient());

        SeedParentSpeciesMetadata(
            session,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "species-alpha",
                [BuildBrainParentKey(parents[1])] = "species-alpha",
                [BuildBrainParentKey(parents[2])] = "species-alpha",
                [BuildBrainParentKey(parents[3])] = "species-alpha",
                [BuildBrainParentKey(parents[4])] = "species-beta",
                [BuildBrainParentKey(parents[5])] = "species-beta"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-beta"] = 1UL
            });

        var selections = SampleSelectedSpeciesCounts(session, sampleCount: 6_000, excludedIndex: -1);

        Assert.True(selections.TryGetValue("species-alpha", out var alphaCount));
        Assert.True(selections.TryGetValue("species-beta", out var betaCount));
        var ratio = alphaCount / (double)betaCount;
        Assert.InRange(ratio, 0.85d, 1.15d);
    }

    [Fact]
    public void SelectParentIndex_Divergence_WithSingleLineageFamily_DoesNotPreferNewestDerivedSpecies()
    {
        var parents = CreateOrderedBrainParentPool(4);
        var options = CreateOptions(
            seed: 99123UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Divergence
        };
        var session = new EvolutionSimulationSession(options, parents, new DeterministicFakeClient());

        SeedParentSpeciesMetadata(
            session,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "species-alpha",
                [BuildBrainParentKey(parents[1])] = "species-alpha-branch-a",
                [BuildBrainParentKey(parents[2])] = "species-alpha-branch-b",
                [BuildBrainParentKey(parents[3])] = "species-alpha-branch-c"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-alpha-branch-a"] = 2UL,
                ["species-alpha-branch-b"] = 3UL,
                ["species-alpha-branch-c"] = 4UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "family-alpha",
                [BuildBrainParentKey(parents[1])] = "family-alpha",
                [BuildBrainParentKey(parents[2])] = "family-alpha",
                [BuildBrainParentKey(parents[3])] = "family-alpha"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha",
                ["species-alpha-branch-b"] = "family-alpha",
                ["species-alpha-branch-c"] = "family-alpha"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL
            });

        var selections = SampleSelectedSpeciesCounts(session, sampleCount: 6_000, excludedIndex: -1);

        Assert.True(selections.TryGetValue("species-alpha", out var rootCount));
        Assert.True(selections.TryGetValue("species-alpha-branch-c", out var newestCount));
        var ratio = newestCount / (double)rootCount;
        Assert.InRange(ratio, 0.85d, 1.15d);
    }

    [Fact]
    public void SelectParentIndex_WithMultipleLineageFamilies_PreservesFamilyAgeBiasAcrossFamilies()
    {
        var parents = CreateOrderedBrainParentPool(6);
        var options = CreateOptions(
            seed: 99124UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Divergence
        };
        var session = new EvolutionSimulationSession(options, parents, new DeterministicFakeClient());

        SeedParentSpeciesMetadata(
            session,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "species-alpha",
                [BuildBrainParentKey(parents[1])] = "species-alpha-branch-a",
                [BuildBrainParentKey(parents[2])] = "species-alpha-branch-b",
                [BuildBrainParentKey(parents[3])] = "species-beta",
                [BuildBrainParentKey(parents[4])] = "species-gamma",
                [BuildBrainParentKey(parents[5])] = "species-delta"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-beta"] = 2UL,
                ["species-gamma"] = 3UL,
                ["species-delta"] = 4UL,
                ["species-alpha-branch-a"] = 5UL,
                ["species-alpha-branch-b"] = 6UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "family-alpha",
                [BuildBrainParentKey(parents[1])] = "family-alpha",
                [BuildBrainParentKey(parents[2])] = "family-alpha",
                [BuildBrainParentKey(parents[3])] = "family-beta",
                [BuildBrainParentKey(parents[4])] = "family-gamma",
                [BuildBrainParentKey(parents[5])] = "family-delta"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha",
                ["species-alpha-branch-b"] = "family-alpha",
                ["species-beta"] = "family-beta",
                ["species-gamma"] = "family-gamma",
                ["species-delta"] = "family-delta"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL,
                ["family-beta"] = 2UL,
                ["family-gamma"] = 3UL,
                ["family-delta"] = 4UL
            });

        var selections = SampleSelectedLineageFamilyCounts(session, sampleCount: 6_000, excludedIndex: -1);

        Assert.True(selections.TryGetValue("family-alpha", out var alphaCount));
        Assert.True(selections.TryGetValue("family-beta", out var betaCount));
        Assert.True(selections.TryGetValue("family-gamma", out var gammaCount));
        Assert.True(selections.TryGetValue("family-delta", out var deltaCount));
        Assert.True(betaCount > alphaCount);
        Assert.True(gammaCount > alphaCount);
        Assert.True(deltaCount > alphaCount);
    }

    [Fact]
    public void SelectParentIndex_WithMultipleLineageFamilies_PreservesSingleFamilySpeciesBiasWithinEachFamily()
    {
        var singleFamilyParents = CreateOrderedBrainParentPool(4);
        var options = CreateOptions(
            seed: 99126UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Stability
        };

        var singleFamilySession = new EvolutionSimulationSession(options, singleFamilyParents, new DeterministicFakeClient());
        SeedParentSpeciesMetadata(
            singleFamilySession,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(singleFamilyParents[0])] = "species-alpha",
                [BuildBrainParentKey(singleFamilyParents[1])] = "species-alpha",
                [BuildBrainParentKey(singleFamilyParents[2])] = "species-alpha",
                [BuildBrainParentKey(singleFamilyParents[3])] = "species-alpha-branch-a"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-alpha-branch-a"] = 2UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(singleFamilyParents[0])] = "family-alpha",
                [BuildBrainParentKey(singleFamilyParents[1])] = "family-alpha",
                [BuildBrainParentKey(singleFamilyParents[2])] = "family-alpha",
                [BuildBrainParentKey(singleFamilyParents[3])] = "family-alpha"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL
            });

        var singleFamilySelections = SampleSelectedSpeciesCounts(
            singleFamilySession,
            sampleCount: 6_000,
            excludedIndex: -1);

        Assert.True(singleFamilySelections.TryGetValue("species-alpha", out var singleFamilyAlphaCount));
        Assert.True(singleFamilySelections.TryGetValue("species-alpha-branch-a", out var singleFamilyBranchCount));
        var singleFamilyRatio = singleFamilyAlphaCount / (double)singleFamilyBranchCount;

        var multiFamilyParents = CreateOrderedBrainParentPool(6);
        var multiFamilySession = new EvolutionSimulationSession(options, multiFamilyParents, new DeterministicFakeClient());
        SeedParentSpeciesMetadata(
            multiFamilySession,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(multiFamilyParents[0])] = "species-alpha",
                [BuildBrainParentKey(multiFamilyParents[1])] = "species-alpha",
                [BuildBrainParentKey(multiFamilyParents[2])] = "species-alpha",
                [BuildBrainParentKey(multiFamilyParents[3])] = "species-alpha-branch-a",
                [BuildBrainParentKey(multiFamilyParents[4])] = "species-beta",
                [BuildBrainParentKey(multiFamilyParents[5])] = "species-beta"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-alpha-branch-a"] = 2UL,
                ["species-beta"] = 3UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(multiFamilyParents[0])] = "family-alpha",
                [BuildBrainParentKey(multiFamilyParents[1])] = "family-alpha",
                [BuildBrainParentKey(multiFamilyParents[2])] = "family-alpha",
                [BuildBrainParentKey(multiFamilyParents[3])] = "family-alpha",
                [BuildBrainParentKey(multiFamilyParents[4])] = "family-beta",
                [BuildBrainParentKey(multiFamilyParents[5])] = "family-beta"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha",
                ["species-beta"] = "family-beta"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL,
                ["family-beta"] = 2UL
            });

        var multiFamilySelections = SampleSelectedSpeciesCounts(
            multiFamilySession,
            sampleCount: 6_000,
            excludedIndex: -1);

        Assert.True(multiFamilySelections.TryGetValue("species-alpha", out var multiFamilyAlphaCount));
        Assert.True(multiFamilySelections.TryGetValue("species-alpha-branch-a", out var multiFamilyBranchCount));
        var multiFamilyRatio = multiFamilyAlphaCount / (double)multiFamilyBranchCount;
        var ratioOfRatios = multiFamilyRatio / singleFamilyRatio;
        Assert.InRange(ratioOfRatios, 0.85d, 1.15d);
    }

    [Fact]
    public void TrySelectParents_WithTwoFounderFamilies_AvoidsSameFamilySelfPairingWhenAlternativesExist()
    {
        var parents = CreateOrderedBrainParentPool(6);
        var options = CreateOptions(
            seed: 99127UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Stability
        };
        var session = new EvolutionSimulationSession(options, parents, new DeterministicFakeClient());

        SeedParentSpeciesMetadata(
            session,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "species-alpha",
                [BuildBrainParentKey(parents[1])] = "species-alpha-branch-a",
                [BuildBrainParentKey(parents[2])] = "species-alpha-branch-b",
                [BuildBrainParentKey(parents[3])] = "species-beta",
                [BuildBrainParentKey(parents[4])] = "species-beta-branch-a",
                [BuildBrainParentKey(parents[5])] = "species-beta-branch-b"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-beta"] = 1UL,
                ["species-alpha-branch-a"] = 2UL,
                ["species-alpha-branch-b"] = 3UL,
                ["species-beta-branch-a"] = 2UL,
                ["species-beta-branch-b"] = 3UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "family-alpha",
                [BuildBrainParentKey(parents[1])] = "family-alpha",
                [BuildBrainParentKey(parents[2])] = "family-alpha",
                [BuildBrainParentKey(parents[3])] = "family-beta",
                [BuildBrainParentKey(parents[4])] = "family-beta",
                [BuildBrainParentKey(parents[5])] = "family-beta"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha",
                ["species-alpha-branch-b"] = "family-alpha",
                ["species-beta"] = "family-beta",
                ["species-beta-branch-a"] = "family-beta",
                ["species-beta-branch-b"] = "family-beta"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL,
                ["family-beta"] = 1UL
            });

        var selectedPairs = SampleSelectedLineageFamilyPairs(session, sampleCount: 6_000);

        Assert.DoesNotContain(
            selectedPairs,
            static pair => string.Equals(pair.ParentAFamily, pair.ParentBFamily, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void TrySelectParents_WithThreeFounderFamilies_AvoidsSameFamilySelfPairingWhenAlternativesExist()
    {
        var parents = CreateOrderedBrainParentPool(8);
        var options = CreateOptions(
            seed: 99128UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            ParentSelectionBias = EvolutionParentSelectionBias.Stability
        };
        var session = new EvolutionSimulationSession(options, parents, new DeterministicFakeClient());

        SeedParentSpeciesMetadata(
            session,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "species-alpha",
                [BuildBrainParentKey(parents[1])] = "species-alpha-branch-a",
                [BuildBrainParentKey(parents[2])] = "species-alpha-branch-b",
                [BuildBrainParentKey(parents[3])] = "species-beta",
                [BuildBrainParentKey(parents[4])] = "species-beta-branch-a",
                [BuildBrainParentKey(parents[5])] = "species-gamma",
                [BuildBrainParentKey(parents[6])] = "species-gamma-branch-a",
                [BuildBrainParentKey(parents[7])] = "species-gamma-branch-b"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-beta"] = 1UL,
                ["species-gamma"] = 1UL,
                ["species-alpha-branch-a"] = 2UL,
                ["species-alpha-branch-b"] = 3UL,
                ["species-beta-branch-a"] = 2UL,
                ["species-gamma-branch-a"] = 2UL,
                ["species-gamma-branch-b"] = 3UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "family-alpha",
                [BuildBrainParentKey(parents[1])] = "family-alpha",
                [BuildBrainParentKey(parents[2])] = "family-alpha",
                [BuildBrainParentKey(parents[3])] = "family-beta",
                [BuildBrainParentKey(parents[4])] = "family-beta",
                [BuildBrainParentKey(parents[5])] = "family-gamma",
                [BuildBrainParentKey(parents[6])] = "family-gamma",
                [BuildBrainParentKey(parents[7])] = "family-gamma"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha",
                ["species-alpha-branch-b"] = "family-alpha",
                ["species-beta"] = "family-beta",
                ["species-beta-branch-a"] = "family-beta",
                ["species-gamma"] = "family-gamma",
                ["species-gamma-branch-a"] = "family-gamma",
                ["species-gamma-branch-b"] = "family-gamma"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL,
                ["family-beta"] = 1UL,
                ["family-gamma"] = 1UL
            });

        var selectedPairs = SampleSelectedLineageFamilyPairs(session, sampleCount: 6_000);

        Assert.DoesNotContain(
            selectedPairs,
            static pair => string.Equals(pair.ParentAFamily, pair.ParentBFamily, StringComparison.OrdinalIgnoreCase));

        var parentAFamilyCounts = selectedPairs
            .GroupBy(pair => pair.ParentAFamily, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.OrdinalIgnoreCase);
        Assert.True(parentAFamilyCounts.TryGetValue("family-alpha", out var alphaCount));
        Assert.True(parentAFamilyCounts.TryGetValue("family-beta", out var betaCount));
        Assert.True(parentAFamilyCounts.TryGetValue("family-gamma", out var gammaCount));
        Assert.True(alphaCount > 0);
        Assert.True(betaCount > 0);
        Assert.True(gammaCount > 0);
    }

    [Fact]
    public void TryAddParentToPoolAtCapacity_WhenCandidateStaysWithinDominantFamily_PreservesRarerFamilies()
    {
        var parents = CreateOrderedBrainParentPool(6);
        var options = CreateOptions(
            seed: 99125UL,
            maxIterations: 1,
            commitToSpeciation: false,
            parentMode: EvolutionParentMode.BrainIds) with
        {
            MaxParentPoolSize = 6
        };
        var session = new EvolutionSimulationSession(options, parents, new DeterministicFakeClient());

        SeedParentSpeciesMetadata(
            session,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "species-alpha",
                [BuildBrainParentKey(parents[1])] = "species-alpha-branch-a",
                [BuildBrainParentKey(parents[2])] = "species-alpha-branch-b",
                [BuildBrainParentKey(parents[3])] = "species-beta",
                [BuildBrainParentKey(parents[4])] = "species-gamma",
                [BuildBrainParentKey(parents[5])] = "species-delta"
            },
            new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = 1UL,
                ["species-beta"] = 2UL,
                ["species-gamma"] = 3UL,
                ["species-delta"] = 4UL,
                ["species-alpha-branch-a"] = 5UL,
                ["species-alpha-branch-b"] = 6UL
            },
            lineageFamilyByParentKey: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                [BuildBrainParentKey(parents[0])] = "family-alpha",
                [BuildBrainParentKey(parents[1])] = "family-alpha",
                [BuildBrainParentKey(parents[2])] = "family-alpha",
                [BuildBrainParentKey(parents[3])] = "family-beta",
                [BuildBrainParentKey(parents[4])] = "family-gamma",
                [BuildBrainParentKey(parents[5])] = "family-delta"
            },
            lineageFamilyBySpeciesId: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["species-alpha"] = "family-alpha",
                ["species-alpha-branch-a"] = "family-alpha",
                ["species-alpha-branch-b"] = "family-alpha",
                ["species-beta"] = "family-beta",
                ["species-gamma"] = "family-gamma",
                ["species-delta"] = "family-delta"
            },
            lineageFamilyOrdinals: new Dictionary<string, ulong>(StringComparer.OrdinalIgnoreCase)
            {
                ["family-alpha"] = 1UL,
                ["family-beta"] = 2UL,
                ["family-gamma"] = 3UL,
                ["family-delta"] = 4UL
            });
        ClearProtectedParentPoolKeys(session);

        var tryAddParentMethod = typeof(EvolutionSimulationSession).GetMethod(
            "TryAddCommittedCandidateToPool",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(tryAddParentMethod);

        var candidateBrainId = Guid.Parse("00000007-0007-0007-0007-000000000007");
        var added = Assert.IsType<bool>(tryAddParentMethod!.Invoke(
            session,
            [new SpeciationCommitCandidate(candidateBrainId, null), "species-alpha-branch-c", "species-alpha-branch-b"]));
        Assert.True(added);

        var familyCounts = SnapshotParentPoolLineageFamilyCounts(session);
        Assert.Equal(3, familyCounts["family-alpha"]);
        Assert.Equal(1, familyCounts["family-beta"]);
        Assert.Equal(1, familyCounts["family-gamma"]);
        Assert.Equal(1, familyCounts["family-delta"]);
    }

    private static IReadOnlyDictionary<string, int> SnapshotParentPoolSpeciesCounts(EvolutionSimulationSession session)
    {
        var parentPoolField = typeof(EvolutionSimulationSession).GetField(
            "_parentPool",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentSpeciesField = typeof(EvolutionSimulationSession).GetField(
            "_parentSpeciesByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(parentPoolField);
        Assert.NotNull(parentSpeciesField);

        var parentPool = Assert.IsType<List<EvolutionParentRef>>(parentPoolField.GetValue(session));
        var parentSpeciesByKey =
            Assert.IsType<Dictionary<string, string>>(parentSpeciesField.GetValue(session));
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var parent in parentPool)
        {
            if (!TryBuildParentKey(parent, out var key))
            {
                continue;
            }

            var speciesId = parentSpeciesByKey.TryGetValue(key, out var trackedSpeciesId)
                && !string.IsNullOrWhiteSpace(trackedSpeciesId)
                    ? trackedSpeciesId.Trim()
                    : "(unknown)";
            counts[speciesId] = counts.TryGetValue(speciesId, out var count)
                ? count + 1
                : 1;
        }

        return counts;
    }

    private static IReadOnlyDictionary<string, int> SnapshotParentPoolLineageFamilyCounts(EvolutionSimulationSession session)
    {
        var parentPoolField = typeof(EvolutionSimulationSession).GetField(
            "_parentPool",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentLineageFamilyField = typeof(EvolutionSimulationSession).GetField(
            "_parentLineageFamilyByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(parentPoolField);
        Assert.NotNull(parentLineageFamilyField);

        var parentPool = Assert.IsType<List<EvolutionParentRef>>(parentPoolField.GetValue(session));
        var parentLineageFamilyByKey =
            Assert.IsType<Dictionary<string, string>>(parentLineageFamilyField.GetValue(session));
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var parent in parentPool)
        {
            if (!TryBuildParentKey(parent, out var key))
            {
                continue;
            }

            var lineageFamilyId = parentLineageFamilyByKey.TryGetValue(key, out var trackedLineageFamilyId)
                                  && !string.IsNullOrWhiteSpace(trackedLineageFamilyId)
                ? trackedLineageFamilyId.Trim()
                : "(unknown)";
            counts[lineageFamilyId] = counts.TryGetValue(lineageFamilyId, out var count)
                ? count + 1
                : 1;
        }

        return counts;
    }

    private static IReadOnlyDictionary<string, int> SampleSelectedSpeciesCounts(
        EvolutionSimulationSession session,
        int sampleCount,
        int excludedIndex)
    {
        var selectParentIndexMethod = typeof(EvolutionSimulationSession).GetMethod(
            "SelectParentIndex",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentPoolField = typeof(EvolutionSimulationSession).GetField(
            "_parentPool",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentSpeciesField = typeof(EvolutionSimulationSession).GetField(
            "_parentSpeciesByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(selectParentIndexMethod);
        Assert.NotNull(parentPoolField);
        Assert.NotNull(parentSpeciesField);

        var parentPool = Assert.IsType<List<EvolutionParentRef>>(parentPoolField.GetValue(session));
        var parentSpeciesByKey =
            Assert.IsType<Dictionary<string, string>>(parentSpeciesField.GetValue(session));
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < sampleCount; i++)
        {
            var selectedIndex = Assert.IsType<int>(selectParentIndexMethod.Invoke(session, [excludedIndex]));
            Assert.InRange(selectedIndex, 0, parentPool.Count - 1);
            Assert.True(TryBuildParentKey(parentPool[selectedIndex], out var parentKey));
            var speciesId = parentSpeciesByKey.TryGetValue(parentKey, out var trackedSpeciesId)
                            && !string.IsNullOrWhiteSpace(trackedSpeciesId)
                ? trackedSpeciesId.Trim()
                : "(unknown)";
            counts[speciesId] = counts.TryGetValue(speciesId, out var count)
                ? count + 1
                : 1;
        }

        return counts;
    }

    private static IReadOnlyDictionary<string, int> SampleSelectedLineageFamilyCounts(
        EvolutionSimulationSession session,
        int sampleCount,
        int excludedIndex)
    {
        var selectParentIndexMethod = typeof(EvolutionSimulationSession).GetMethod(
            "SelectParentIndex",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentPoolField = typeof(EvolutionSimulationSession).GetField(
            "_parentPool",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentLineageFamilyField = typeof(EvolutionSimulationSession).GetField(
            "_parentLineageFamilyByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(selectParentIndexMethod);
        Assert.NotNull(parentPoolField);
        Assert.NotNull(parentLineageFamilyField);

        var parentPool = Assert.IsType<List<EvolutionParentRef>>(parentPoolField.GetValue(session));
        var parentLineageFamilyByKey =
            Assert.IsType<Dictionary<string, string>>(parentLineageFamilyField.GetValue(session));
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < sampleCount; i++)
        {
            var selectedIndex = Assert.IsType<int>(selectParentIndexMethod.Invoke(session, [excludedIndex]));
            Assert.InRange(selectedIndex, 0, parentPool.Count - 1);
            Assert.True(TryBuildParentKey(parentPool[selectedIndex], out var parentKey));
            var lineageFamilyId = parentLineageFamilyByKey.TryGetValue(parentKey, out var trackedLineageFamilyId)
                                  && !string.IsNullOrWhiteSpace(trackedLineageFamilyId)
                ? trackedLineageFamilyId.Trim()
                : "(unknown)";
            counts[lineageFamilyId] = counts.TryGetValue(lineageFamilyId, out var count)
                ? count + 1
                : 1;
        }

        return counts;
    }

    private static IReadOnlyList<(string ParentAFamily, string ParentBFamily)> SampleSelectedLineageFamilyPairs(
        EvolutionSimulationSession session,
        int sampleCount)
    {
        var trySelectParentsMethod = typeof(EvolutionSimulationSession).GetMethod(
            "TrySelectParents",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentLineageFamilyField = typeof(EvolutionSimulationSession).GetField(
            "_parentLineageFamilyByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(trySelectParentsMethod);
        Assert.NotNull(parentLineageFamilyField);

        var parentLineageFamilyByKey =
            Assert.IsType<Dictionary<string, string>>(parentLineageFamilyField.GetValue(session));
        var pairs = new List<(string ParentAFamily, string ParentBFamily)>(sampleCount);
        for (var i = 0; i < sampleCount; i++)
        {
            var arguments = new object?[] { null, null };
            var selected = Assert.IsType<bool>(trySelectParentsMethod.Invoke(session, arguments));
            Assert.True(selected);

            var parentA = (EvolutionParentRef)arguments[0]!;
            var parentB = (EvolutionParentRef)arguments[1]!;
            Assert.True(TryBuildParentKey(parentA, out var parentAKey));
            Assert.True(TryBuildParentKey(parentB, out var parentBKey));

            var parentAFamily = parentLineageFamilyByKey.TryGetValue(parentAKey, out var trackedParentAFamily)
                                && !string.IsNullOrWhiteSpace(trackedParentAFamily)
                ? trackedParentAFamily.Trim()
                : "(unknown)";
            var parentBFamily = parentLineageFamilyByKey.TryGetValue(parentBKey, out var trackedParentBFamily)
                                && !string.IsNullOrWhiteSpace(trackedParentBFamily)
                ? trackedParentBFamily.Trim()
                : "(unknown)";
            pairs.Add((parentAFamily, parentBFamily));
        }

        return pairs;
    }

    private static void ClearProtectedParentPoolKeys(EvolutionSimulationSession session)
    {
        var protectedParentPoolKeysField = typeof(EvolutionSimulationSession).GetField(
            "_protectedParentPoolKeys",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(protectedParentPoolKeysField);
        var protectedParentPoolKeys =
            Assert.IsType<HashSet<string>>(protectedParentPoolKeysField.GetValue(session));
        protectedParentPoolKeys.Clear();
    }

    private static void SeedParentSpeciesMetadata(
        EvolutionSimulationSession session,
        IReadOnlyDictionary<string, string> speciesByParentKey,
        IReadOnlyDictionary<string, ulong> speciesOrdinals,
        IReadOnlyDictionary<string, string>? lineageFamilyByParentKey = null,
        IReadOnlyDictionary<string, string>? lineageFamilyBySpeciesId = null,
        IReadOnlyDictionary<string, ulong>? lineageFamilyOrdinals = null)
    {
        var parentSpeciesField = typeof(EvolutionSimulationSession).GetField(
            "_parentSpeciesByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var parentLineageFamilyField = typeof(EvolutionSimulationSession).GetField(
            "_parentLineageFamilyByParentKey",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var lineageFamilyBySpeciesField = typeof(EvolutionSimulationSession).GetField(
            "_lineageFamilyBySpeciesId",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var speciesFirstSeenField = typeof(EvolutionSimulationSession).GetField(
            "_speciesFirstSeenOrdinals",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var lineageFamilyFirstSeenField = typeof(EvolutionSimulationSession).GetField(
            "_lineageFamilyFirstSeenOrdinals",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var nextSpeciesOrdinalField = typeof(EvolutionSimulationSession).GetField(
            "_nextSpeciesOrdinal",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var nextLineageFamilyOrdinalField = typeof(EvolutionSimulationSession).GetField(
            "_nextLineageFamilyOrdinal",
            BindingFlags.Instance | BindingFlags.NonPublic);

        Assert.NotNull(parentSpeciesField);
        Assert.NotNull(parentLineageFamilyField);
        Assert.NotNull(lineageFamilyBySpeciesField);
        Assert.NotNull(speciesFirstSeenField);
        Assert.NotNull(lineageFamilyFirstSeenField);
        Assert.NotNull(nextSpeciesOrdinalField);
        Assert.NotNull(nextLineageFamilyOrdinalField);

        var parentSpeciesByKey =
            Assert.IsType<Dictionary<string, string>>(parentSpeciesField.GetValue(session));
        var parentLineageFamilyByKey =
            Assert.IsType<Dictionary<string, string>>(parentLineageFamilyField.GetValue(session));
        var lineageFamilyBySpecies =
            Assert.IsType<Dictionary<string, string>>(lineageFamilyBySpeciesField.GetValue(session));
        var speciesFirstSeenOrdinals =
            Assert.IsType<Dictionary<string, ulong>>(speciesFirstSeenField.GetValue(session));
        var lineageFamilyFirstSeenOrdinals =
            Assert.IsType<Dictionary<string, ulong>>(lineageFamilyFirstSeenField.GetValue(session));
        parentSpeciesByKey.Clear();
        foreach (var entry in speciesByParentKey)
        {
            parentSpeciesByKey[entry.Key] = entry.Value;
        }

        parentLineageFamilyByKey.Clear();
        foreach (var entry in lineageFamilyByParentKey ?? speciesByParentKey)
        {
            parentLineageFamilyByKey[entry.Key] = entry.Value;
        }

        lineageFamilyBySpecies.Clear();
        foreach (var entry in lineageFamilyBySpeciesId ?? speciesOrdinals.Keys.ToDictionary(
                     static key => key,
                     static key => key,
                     StringComparer.OrdinalIgnoreCase))
        {
            lineageFamilyBySpecies[entry.Key] = entry.Value;
        }

        speciesFirstSeenOrdinals.Clear();
        foreach (var entry in speciesOrdinals)
        {
            speciesFirstSeenOrdinals[entry.Key] = entry.Value;
        }

        lineageFamilyFirstSeenOrdinals.Clear();
        foreach (var entry in lineageFamilyOrdinals ?? speciesOrdinals)
        {
            lineageFamilyFirstSeenOrdinals[entry.Key] = entry.Value;
        }

        nextSpeciesOrdinalField.SetValue(session, speciesOrdinals.Values.DefaultIfEmpty(0UL).Max() + 1UL);
        nextLineageFamilyOrdinalField.SetValue(
            session,
            (lineageFamilyOrdinals ?? speciesOrdinals).Values.DefaultIfEmpty(0UL).Max() + 1UL);
    }

    private static string BuildBrainParentKey(EvolutionParentRef parentRef)
    {
        Assert.True(TryBuildParentKey(parentRef, out var parentKey));
        return parentKey;
    }

    private static string BuildBrainParentKeyOrArtifactKey(EvolutionParentRef parentRef)
    {
        Assert.True(TryBuildParentKey(parentRef, out var parentKey));
        return parentKey;
    }

    private static IReadOnlyList<EvolutionParentRef> CreateParentPool()
    {
        return new[]
        {
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-a", "file:///tmp/a")),
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-b", "file:///tmp/b")),
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-c", "file:///tmp/c"))
        };
    }

    private static bool TryBuildParentKey(EvolutionParentRef parentRef, out string key)
    {
        key = string.Empty;
        if (parentRef.BrainId is Guid brainId && brainId != Guid.Empty)
        {
            key = $"brain:{brainId:D}";
            return true;
        }

        if (parentRef.ArtifactRef is { } artifactRef)
        {
            if (artifactRef.TryToSha256Hex(out var sha))
            {
                key = $"artifact:{sha}";
                return true;
            }

            var storeUri = string.IsNullOrWhiteSpace(artifactRef.StoreUri)
                ? string.Empty
                : artifactRef.StoreUri.Trim();
            if (!string.IsNullOrWhiteSpace(storeUri))
            {
                var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType)
                    ? string.Empty
                    : artifactRef.MediaType.Trim();
                key = $"artifact-uri:{storeUri}|{mediaType}|{artifactRef.SizeBytes}";
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<EvolutionParentRef> CreateBrainParentPool()
    {
        return new[]
        {
            EvolutionParentRef.FromBrainId(Guid.Parse("11111111-1111-1111-1111-111111111111")),
            EvolutionParentRef.FromBrainId(Guid.Parse("22222222-2222-2222-2222-222222222222")),
            EvolutionParentRef.FromBrainId(Guid.Parse("33333333-3333-3333-3333-333333333333"))
        };
    }

    private static IReadOnlyList<EvolutionParentRef> CreateDuplicatedBrainParentPool()
    {
        return new[]
        {
            EvolutionParentRef.FromBrainId(Guid.Parse("11111111-1111-1111-1111-111111111111")),
            EvolutionParentRef.FromBrainId(Guid.Parse("11111111-1111-1111-1111-111111111111")),
            EvolutionParentRef.FromBrainId(Guid.Parse("22222222-2222-2222-2222-222222222222")),
            EvolutionParentRef.FromBrainId(Guid.Parse("22222222-2222-2222-2222-222222222222"))
        };
    }

    private static IReadOnlyList<EvolutionParentRef> CreateDuplicatedArtifactParentPool()
    {
        return new[]
        {
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-a", "file:///tmp/a-1")),
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-a", "file:///tmp/a-2")),
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-b", "file:///tmp/b-1")),
            EvolutionParentRef.FromArtifactRef(BuildArtifact("parent-b", "file:///tmp/b-2"))
        };
    }

    private static IReadOnlyList<EvolutionParentRef> CreateOrderedBrainParentPool(int count)
    {
        var parents = new List<EvolutionParentRef>(count);
        for (var index = 1; index <= count; index++)
        {
            parents.Add(EvolutionParentRef.FromBrainId(Guid.Parse($"{index:D8}-{index:D4}-{index:D4}-{index:D4}-{index:D12}")));
        }

        return parents;
    }

    private static EvolutionSimulationOptions CreateOptions(
        ulong seed,
        int maxIterations,
        bool commitToSpeciation,
        EvolutionParentMode parentMode = EvolutionParentMode.ArtifactRefs)
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
            ParentMode = parentMode,
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
        public List<SpeciationCommitCandidate> CommittedCandidates { get; } = new();
        public List<uint> RequestedRunCounts { get; } = new();
        public List<Guid> ObservedBrainSelections { get; } = new();
        public List<(string ParentAKey, string ParentBKey)> AssessedParentPairs { get; } = new();
        public List<(string ParentAKey, string ParentBKey)> CommittedParentPairs { get; } = new();
        public Queue<string?> CommitOutcomeSpeciesIds { get; } = new();
        public Queue<string?> CommitOutcomeSourceSpeciesIds { get; } = new();
        public bool ObservedBrainIdParents { get; private set; }
        public bool ObservedArtifactParents { get; private set; }
        public float ReproductionDiagnosticSimilarity { get; set; } = 0.5f;
        public float? CommitCandidateSimilarity { get; set; }
        public float? CommitOutcomeSourceSpeciesSimilarity { get; set; }

        public async Task<CompatibilityAssessment> AssessCompatibilityAsync(
            EvolutionParentRef parentA,
            EvolutionParentRef parentB,
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

            ObserveParentKinds(parentA, parentB);
            AssessedParentPairs.Add((BuildParentSelectionKey(parentA), BuildParentSelectionKey(parentB)));
            if (parentA.BrainId is Guid parentABrainId && parentABrainId != Guid.Empty)
            {
                ObservedBrainSelections.Add(parentABrainId);
            }

            if (parentB.BrainId is Guid parentBBrainId && parentBBrainId != Guid.Empty)
            {
                ObservedBrainSelections.Add(parentBBrainId);
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
            EvolutionParentRef parentA,
            EvolutionParentRef parentB,
            ulong seed,
            uint runCount,
            bool spawnChildren,
            Repro.StrengthSource strengthSource,
            CancellationToken cancellationToken)
        {
            RequestedRunCounts.Add(runCount);
            ObserveParentKinds(parentA, parentB);
            Events.Add($"repro:{seed}:{runCount}:{Short(parentA)}:{Short(parentB)}");
            var child = BuildArtifact(
                $"child:{seed}:{runCount}:{Short(parentA)}:{Short(parentB)}",
                parentA.ArtifactRef?.StoreUri);
            return Task.FromResult(
                new ReproductionOutcome(
                    Success: true,
                    Compatible: true,
                    AbortReason: string.Empty,
                    ChildDefinitions: new[] { child },
                    CommitCandidates: new[]
                    {
                        new SpeciationCommitCandidate(
                            ChildBrainId: null,
                            ChildDefinition: child,
                            SimilarityScore: CommitCandidateSimilarity)
                    },
                    Diagnostics: new ReproductionDiagnostics(
                        RunCount: runCount,
                        RunsWithMutations: runCount,
                        MutationEvents: runCount,
                        SimilaritySamples: 1,
                        MinSimilarity: ReproductionDiagnosticSimilarity,
                        MaxSimilarity: ReproductionDiagnosticSimilarity)));
        }

        public Task<SpeciationCommitOutcome> CommitSpeciationAsync(
            SpeciationCommitCandidate candidate,
            EvolutionParentRef parentA,
            EvolutionParentRef parentB,
            CancellationToken cancellationToken)
        {
            ObserveParentKinds(parentA, parentB);
            CommittedCandidates.Add(candidate);
            CommittedParentPairs.Add((BuildParentSelectionKey(parentA), BuildParentSelectionKey(parentB)));
            var candidateLabel = candidate.ChildBrainId is Guid brainId && brainId != Guid.Empty
                ? $"brain:{brainId:D}"
                : candidate.ChildDefinition is { } definition
                    ? $"artifact:{ShortArtifact(definition)}"
                    : "missing";
            Events.Add($"speciation:{candidateLabel}:{Short(parentA)}:{Short(parentB)}");
            var speciesId = CommitOutcomeSpeciesIds.Count > 0
                ? CommitOutcomeSpeciesIds.Dequeue() ?? string.Empty
                : string.Empty;
            var sourceSpeciesId = CommitOutcomeSourceSpeciesIds.Count > 0
                ? CommitOutcomeSourceSpeciesIds.Dequeue() ?? string.Empty
                : string.Empty;
            return Task.FromResult(new SpeciationCommitOutcome(
                Success: true,
                FailureDetail: string.Empty,
                ExpectedNoOp: false,
                SpeciesId: speciesId,
                SourceSpeciesId: sourceSpeciesId,
                SourceSpeciesSimilarityScore: CommitOutcomeSourceSpeciesSimilarity));
        }

        private void ObserveParentKinds(EvolutionParentRef parentA, EvolutionParentRef parentB)
        {
            if (parentA.IsBrainId || parentB.IsBrainId)
            {
                ObservedBrainIdParents = true;
            }

            if (parentA.IsArtifactRef || parentB.IsArtifactRef)
            {
                ObservedArtifactParents = true;
            }
        }

        private static string BuildParentSelectionKey(EvolutionParentRef parent)
        {
            return TryBuildParentKey(parent, out var key)
                ? key
                : "missing";
        }

        private static string Short(EvolutionParentRef reference)
        {
            if (reference.BrainId is Guid brainId && brainId != Guid.Empty)
            {
                var label = $"brain:{brainId:D}";
                return label.Length <= 14 ? label : label[..14];
            }

            return reference.ArtifactRef is { } artifact
                ? ShortArtifact(artifact)
                : "missing";
        }

        private static string ShortArtifact(ArtifactRef artifactRef)
        {
            return artifactRef.TryToSha256Hex(out var sha)
                ? sha[..8]
                : "missing";
        }
    }
}
