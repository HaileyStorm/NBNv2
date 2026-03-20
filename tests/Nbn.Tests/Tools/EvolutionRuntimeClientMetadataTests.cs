using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Shared;
using Nbn.Tools.EvolutionSim;
using Proto;
using Proto.Remote;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Tools;

public sealed class EvolutionRuntimeClientMetadataTests
{
    [Fact]
    public void BuildRemoteConfig_AllInterfaces_UsesResolvedAdvertisedHost()
    {
        using var _ = new EnvironmentVariableScope(("NBN_DEFAULT_ADVERTISE_HOST", "10.9.8.7"));

        var method = typeof(EvolutionRuntimeClient).GetMethod(
            "BuildRemoteConfig",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var config = Assert.IsType<RemoteConfig>(method!.Invoke(null, new object?[]
        {
            NetworkAddressDefaults.DefaultBindHost,
            12074,
            null,
            null
        }));

        Assert.Equal("10.9.8.7", config.AdvertisedHost);
    }

    [Fact]
    public void ExtractReproductionData_MapsSimilarityScoresIntoCommitCandidates()
    {
        var child = BuildArtifact("child-a", "store://artifact-root");
        var result = new Repro.ReproduceResult
        {
            ChildDef = child,
            Report = new Repro.SimilarityReport
            {
                Compatible = true,
                SimilarityScore = 0.72f,
                LineageSimilarityScore = 0.41f,
                LineageParentASimilarityScore = 0.36f,
                LineageParentBSimilarityScore = 0.46f,
                FunctionScore = 0.68f,
                ConnectivityScore = 0.55f,
                RegionSpanScore = 0.91f
            }
        };

        var tuple = InvokeExtractReproductionData(result);
        var candidates = GetTupleItem<IReadOnlyList<SpeciationCommitCandidate>>(tuple, "Item2");

        var candidate = Assert.Single(candidates);
        Assert.NotNull(candidate.ChildDefinition);
        Assert.Equal(0.41f, candidate.SimilarityScore!.Value, 3);
        Assert.Equal(0.41f, candidate.LineageSimilarityScore!.Value, 3);
        Assert.Equal(0.36f, candidate.LineageParentASimilarityScore!.Value, 3);
        Assert.Equal(0.46f, candidate.LineageParentBSimilarityScore!.Value, 3);
        Assert.Equal(0.68f, candidate.FunctionScore!.Value, 3);
        Assert.Equal(0.55f, candidate.ConnectivityScore!.Value, 3);
        Assert.Equal(0.91f, candidate.RegionSpanScore!.Value, 3);
    }

    [Fact]
    public void BuildSpeciationDecisionMetadataJson_IncludesReportScores()
    {
        var candidate = new SpeciationCommitCandidate(
            ChildBrainId: Guid.Parse("c5fe1378-a8ca-4f91-9527-5d38b4583139"),
            ChildDefinition: null,
            SimilarityScore: 0.81f,
            FunctionScore: 0.62f,
            ConnectivityScore: 0.73f,
            RegionSpanScore: 0.48f);

        var json = InvokeBuildMetadataJson(candidate);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("evolution_sim", root.GetProperty("source").GetString());

        var report = root.GetProperty("report");
        Assert.Equal(0.81d, report.GetProperty("similarity_score").GetDouble(), 3);
        Assert.Equal(0.62d, report.GetProperty("function_score").GetDouble(), 3);
        Assert.Equal(0.73d, report.GetProperty("connectivity_score").GetDouble(), 3);
        Assert.Equal(0.48d, report.GetProperty("region_span_score").GetDouble(), 3);

        var lineage = root.GetProperty("lineage");
        Assert.Equal(0.81d, lineage.GetProperty("lineage_similarity_score").GetDouble(), 3);
    }

    [Fact]
    public void BuildSpeciationDecisionMetadataJson_IncludesCandidateBrainArtifactProvenance()
    {
        var childArtifact = BuildArtifact("child-brain-base", "store://artifact-root");
        var candidate = new SpeciationCommitCandidate(
            ChildBrainId: Guid.Parse("d533a7a7-7f01-4ec0-ae84-cdbe786b2e11"),
            ChildDefinition: childArtifact,
            SimilarityScore: 0.77f,
            LineageSimilarityScore: 0.66f);

        var json = InvokeBuildMetadataJson(candidate);
        using var doc = JsonDocument.Parse(json);
        var artifactNode = doc.RootElement
            .GetProperty("lineage")
            .GetProperty("candidate_brain_base_artifact_ref");

        Assert.Equal(childArtifact.ToSha256Hex(), artifactNode.GetProperty("sha256_hex").GetString());
        Assert.Equal((long)childArtifact.SizeBytes, artifactNode.GetProperty("size_bytes").GetInt64());
        Assert.Equal(childArtifact.MediaType, artifactNode.GetProperty("media_type").GetString());
        Assert.Equal(childArtifact.StoreUri, artifactNode.GetProperty("store_uri").GetString());
    }

    [Fact]
    public void ExtractReproductionData_ComputesMutationAndSimilarityDiagnostics()
    {
        var result = new Repro.ReproduceResult
        {
            Runs =
            {
                new Repro.ReproduceRunOutcome
                {
                    Report = new Repro.SimilarityReport { SimilarityScore = 0.81f },
                    Summary = new Repro.MutationSummary { AxonsAdded = 2 }
                },
                new Repro.ReproduceRunOutcome
                {
                    Report = new Repro.SimilarityReport { SimilarityScore = 0.44f },
                    Summary = new Repro.MutationSummary()
                }
            }
        };

        var tuple = InvokeExtractReproductionData(result);
        var diagnostics = GetTupleItem<ReproductionDiagnostics>(tuple, "Item3");

        Assert.Equal(2UL, diagnostics.RunCount);
        Assert.Equal(1UL, diagnostics.RunsWithMutations);
        Assert.Equal(2UL, diagnostics.MutationEvents);
        Assert.Equal(2UL, diagnostics.SimilaritySamples);
        Assert.Equal(0.44f, diagnostics.MinSimilarity, 3);
        Assert.Equal(0.81f, diagnostics.MaxSimilarity, 3);
    }

    [Fact]
    public void ExtractSourceSpeciesId_PrefersCanonicalSourceSpeciesField()
    {
        const string metadataJson = """
            {"lineage":{"source_species_id":"species-alpha","dominant_species_id":"species-beta"}}
            """;

        var sourceSpeciesId = InvokeExtractSourceSpeciesId(metadataJson);

        Assert.Equal("species-alpha", sourceSpeciesId);
    }

    [Fact]
    public void ExtractSourceSpeciesId_FallsBackToDominantSpeciesField()
    {
        const string metadataJson = """
            {"lineage":{"dominant_species_id":"species-beta"}}
            """;

        var sourceSpeciesId = InvokeExtractSourceSpeciesId(metadataJson);

        Assert.Equal("species-beta", sourceSpeciesId);
    }

    [Fact]
    public void ExtractSourceSpeciesSimilarityScore_PrefersCanonicalSourceSpeciesField()
    {
        const string metadataJson = """
            {"lineage":{"source_species_similarity_score":0.61,"dominant_species_similarity_score":0.72,"lineage_similarity_score":0.83},"report":{"similarity_score":0.94}}
            """;

        var similarity = InvokeExtractSourceSpeciesSimilarityScore(metadataJson);

        Assert.True(similarity.HasValue);
        Assert.Equal(0.61f, similarity.Value, 3);
    }

    [Fact]
    public void ExtractSourceSpeciesSimilarityScore_FallsBackToLegacyDominantSpeciesField()
    {
        const string metadataJson = """
            {"lineage":{"dominant_species_similarity_score":0.72},"report":{"similarity_score":0.94}}
            """;

        var similarity = InvokeExtractSourceSpeciesSimilarityScore(metadataJson);

        Assert.True(similarity.HasValue);
        Assert.Equal(0.72f, similarity.Value, 3);
    }

    [Fact]
    public void ExtractSourceSpeciesSimilarityScore_FallsBackToLegacyLineageSimilarity()
    {
        const string metadataJson = """
            {"lineage":{"lineage_similarity_score":0.83},"report":{"similarity_score":0.94}}
            """;

        var similarity = InvokeExtractSourceSpeciesSimilarityScore(metadataJson);

        Assert.True(similarity.HasValue);
        Assert.Equal(0.83f, similarity.Value, 3);
    }

    [Fact]
    public async Task AssessCompatibilityAsync_MixedParentKinds_UsesArtifactFallbackFromBrainInfo()
    {
        var brainId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var brainArtifact = BuildArtifact("brain-parent", "store://artifact-root");
        var artifactParent = BuildArtifact("artifact-parent", "store://artifact-root");
        var system = new ActorSystem();
        try
        {
            var probe = new MixedParentProbeActor(
                brainId,
                brainArtifact,
                BuildArtifact("child", "store://artifact-root"));
            var ioPid = system.Root.Spawn(Props.FromProducer(() => probe));
            var client = CreateClient(system, ioPid);

            var result = await client.AssessCompatibilityAsync(
                EvolutionParentRef.FromBrainId(brainId),
                EvolutionParentRef.FromArtifactRef(artifactParent),
                seed: 91UL,
                Repro.StrengthSource.StrengthBaseOnly,
                CancellationToken.None);

            Assert.True(result.Success);
            Assert.True(result.Compatible);
            Assert.Equal(0.91f, result.SimilarityScore, 3);
            Assert.Equal(1, probe.BrainInfoRequests);
            Assert.Equal(0, probe.ExportRequests);
            Assert.Equal(1, probe.ArtifactAssessmentRequests);
            Assert.Equal(brainArtifact.ToSha256Hex(), probe.LastAssessParentASha);
            Assert.Equal(artifactParent.ToSha256Hex(), probe.LastAssessParentBSha);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ReproduceAsync_MixedParentKinds_UsesExportFallbackWhenBrainInfoDefinitionMissing()
    {
        var brainId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var brainArtifact = BuildArtifact("brain-parent-export", "store://artifact-root");
        var artifactParent = BuildArtifact("artifact-parent-export", "store://artifact-root");
        var childArtifact = BuildArtifact("child-export", "store://artifact-root");
        var system = new ActorSystem();
        try
        {
            var probe = new MixedParentProbeActor(
                brainId,
                brainArtifact,
                childArtifact,
                returnBaseDefinitionInBrainInfo: false);
            var ioPid = system.Root.Spawn(Props.FromProducer(() => probe));
            var client = CreateClient(system, ioPid);

            var result = await client.ReproduceAsync(
                EvolutionParentRef.FromArtifactRef(artifactParent),
                EvolutionParentRef.FromBrainId(brainId),
                seed: 92UL,
                runCount: 3,
                spawnChildren: false,
                Repro.StrengthSource.StrengthBaseOnly,
                CancellationToken.None);

            Assert.True(result.Success);
            Assert.True(result.Compatible);
            Assert.Equal(1, probe.BrainInfoRequests);
            Assert.Equal(1, probe.ExportRequests);
            Assert.Equal(1, probe.ArtifactReproductionRequests);
            Assert.Equal(artifactParent.ToSha256Hex(), probe.LastReproParentASha);
            Assert.Equal(brainArtifact.ToSha256Hex(), probe.LastReproParentBSha);
            var candidate = Assert.Single(result.CommitCandidates);
            Assert.NotNull(candidate.ChildDefinition);
            Assert.Equal(childArtifact.ToSha256Hex(), candidate.ChildDefinition!.ToSha256Hex());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    private static object InvokeExtractReproductionData(Repro.ReproduceResult result)
    {
        var method = typeof(EvolutionRuntimeClient).GetMethod(
            "ExtractReproductionData",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        var tuple = method!.Invoke(null, new object?[] { result });
        Assert.NotNull(tuple);
        return tuple!;
    }

    private sealed class EnvironmentVariableScope : IDisposable
    {
        private readonly Dictionary<string, string?> _originals = new(StringComparer.Ordinal);

        public EnvironmentVariableScope(params (string Key, string? Value)[] values)
        {
            foreach (var (key, value) in values)
            {
                _originals[key] = Environment.GetEnvironmentVariable(key);
                Environment.SetEnvironmentVariable(key, value);
            }
        }

        public void Dispose()
        {
            foreach (var (key, value) in _originals)
            {
                Environment.SetEnvironmentVariable(key, value);
            }
        }
    }

    private static string InvokeBuildMetadataJson(SpeciationCommitCandidate candidate)
    {
        var method = typeof(EvolutionRuntimeClient).GetMethod(
            "BuildSpeciationDecisionMetadataJson",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        var json = method!.Invoke(null, new object[] { candidate });
        return Assert.IsType<string>(json);
    }

    private static string InvokeExtractSourceSpeciesId(string? decisionMetadataJson)
    {
        var method = typeof(EvolutionRuntimeClient).GetMethod(
            "ExtractSourceSpeciesId",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        var value = method!.Invoke(null, [decisionMetadataJson]);
        return Assert.IsType<string>(value);
    }

    private static float? InvokeExtractSourceSpeciesSimilarityScore(string? decisionMetadataJson)
    {
        var method = typeof(EvolutionRuntimeClient).GetMethod(
            "ExtractSourceSpeciesSimilarityScore",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        var value = method!.Invoke(null, [decisionMetadataJson]);
        return value is null ? null : Assert.IsType<float>(value);
    }

    private static EvolutionRuntimeClient CreateClient(ActorSystem system, PID ioPid)
    {
        var constructor = typeof(EvolutionRuntimeClient).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            types: [typeof(ActorSystem), typeof(PID), typeof(TimeSpan), typeof(Repro.ReproduceConfig)],
            modifiers: null);
        Assert.NotNull(constructor);
        return Assert.IsType<EvolutionRuntimeClient>(constructor!.Invoke(
            [system, ioPid, TimeSpan.FromSeconds(2), new Repro.ReproduceConfig()]));
    }

    private static T GetTupleItem<T>(object tuple, string propertyName)
    {
        var property = tuple.GetType().GetProperty(propertyName);
        object? value;
        if (property is not null)
        {
            value = property.GetValue(tuple);
        }
        else
        {
            var field = tuple.GetType().GetField(propertyName);
            Assert.NotNull(field);
            value = field!.GetValue(tuple);
        }

        return Assert.IsAssignableFrom<T>(value);
    }

    private static ArtifactRef BuildArtifact(string value, string? storeUri = null)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        var sha = Convert.ToHexString(bytes).ToLowerInvariant();
        return sha.ToArtifactRef(sizeBytes: 512, mediaType: "application/x-nbn", storeUri: storeUri);
    }

    private sealed class MixedParentProbeActor : IActor
    {
        private readonly Guid _brainId;
        private readonly ArtifactRef _brainArtifact;
        private readonly ArtifactRef _childArtifact;
        private readonly bool _returnBaseDefinitionInBrainInfo;

        public MixedParentProbeActor(
            Guid brainId,
            ArtifactRef brainArtifact,
            ArtifactRef childArtifact,
            bool returnBaseDefinitionInBrainInfo = true)
        {
            _brainId = brainId;
            _brainArtifact = brainArtifact.Clone();
            _childArtifact = childArtifact.Clone();
            _returnBaseDefinitionInBrainInfo = returnBaseDefinitionInBrainInfo;
        }

        public int BrainInfoRequests { get; private set; }
        public int ExportRequests { get; private set; }
        public int ArtifactAssessmentRequests { get; private set; }
        public int ArtifactReproductionRequests { get; private set; }
        public string LastAssessParentASha { get; private set; } = string.Empty;
        public string LastAssessParentBSha { get; private set; } = string.Empty;
        public string LastReproParentASha { get; private set; } = string.Empty;
        public string LastReproParentBSha { get; private set; } = string.Empty;

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case BrainInfoRequest request
                    when request.BrainId is not null
                         && request.BrainId.TryToGuid(out var infoBrainId)
                         && infoBrainId == _brainId:
                    BrainInfoRequests++;
                    context.Respond(new BrainInfo
                    {
                        BrainId = request.BrainId,
                        BaseDefinition = _returnBaseDefinitionInBrainInfo ? _brainArtifact.Clone() : new ArtifactRef(),
                        LastSnapshot = new ArtifactRef()
                    });
                    break;

                case ExportBrainDefinition request
                    when request.BrainId is not null
                         && request.BrainId.TryToGuid(out var exportBrainId)
                         && exportBrainId == _brainId:
                    ExportRequests++;
                    context.Respond(new BrainDefinitionReady
                    {
                        BrainId = request.BrainId,
                        BrainDef = _brainArtifact.Clone()
                    });
                    break;

                case AssessCompatibilityByArtifacts request:
                    ArtifactAssessmentRequests++;
                    LastAssessParentASha = request.Request.ParentADef.ToSha256Hex();
                    LastAssessParentBSha = request.Request.ParentBDef.ToSha256Hex();
                    context.Respond(new AssessCompatibilityResult
                    {
                        Result = new Repro.ReproduceResult
                        {
                            Report = new Repro.SimilarityReport
                            {
                                Compatible = true,
                                SimilarityScore = 0.91f
                            }
                        }
                    });
                    break;

                case ReproduceByArtifacts request:
                    ArtifactReproductionRequests++;
                    LastReproParentASha = request.Request.ParentADef.ToSha256Hex();
                    LastReproParentBSha = request.Request.ParentBDef.ToSha256Hex();
                    context.Respond(new ReproduceResult
                    {
                        Result = new Repro.ReproduceResult
                        {
                            ChildDef = _childArtifact.Clone(),
                            Report = new Repro.SimilarityReport
                            {
                                Compatible = true,
                                SimilarityScore = 0.93f,
                                LineageSimilarityScore = 0.88f
                            }
                        }
                    });
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
