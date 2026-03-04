using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Nbn.Proto;
using Nbn.Shared;
using Nbn.Tools.EvolutionSim;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Tools;

public sealed class EvolutionRuntimeClientMetadataTests
{
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
                FunctionScore = 0.68f,
                ConnectivityScore = 0.55f,
                RegionSpanScore = 0.91f
            }
        };

        var tuple = InvokeExtractReproductionData(result);
        var candidates = GetTupleItem<IReadOnlyList<SpeciationCommitCandidate>>(tuple, "Item2");

        var candidate = Assert.Single(candidates);
        Assert.NotNull(candidate.ChildDefinition);
        Assert.Equal(0.72f, candidate.SimilarityScore!.Value, 3);
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

    private static string InvokeBuildMetadataJson(SpeciationCommitCandidate candidate)
    {
        var method = typeof(EvolutionRuntimeClient).GetMethod(
            "BuildSpeciationDecisionMetadataJson",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        var json = method!.Invoke(null, new object[] { candidate });
        return Assert.IsType<string>(json);
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
}
