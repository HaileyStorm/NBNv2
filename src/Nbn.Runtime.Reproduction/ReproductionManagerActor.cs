using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;
using Proto;

namespace Nbn.Runtime.Reproduction;

public sealed class ReproductionManagerActor : IActor
{
    private const string NbnMediaType = "application/x-nbn";
    private const string StageBNotImplementedReason = "repro_not_implemented:stage_b";

    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case ReproduceByBrainIdsRequest message:
                context.Respond(HandleReproduceByBrainIds(message));
                break;
            case ReproduceByArtifactsRequest message:
                context.Respond(await HandleReproduceByArtifactsAsync(message).ConfigureAwait(false));
                break;
        }
    }

    private static ReproduceResult HandleReproduceByBrainIds(ReproduceByBrainIdsRequest request)
    {
        if (request.ParentA is null || request.ParentB is null)
        {
            return CreateAbortResult("repro_missing_parent_brain_ids");
        }

        return CreateAbortResult("repro_parent_resolution_unavailable");
    }

    private static async Task<ReproduceResult> HandleReproduceByArtifactsAsync(ReproduceByArtifactsRequest request)
    {
        try
        {
            if (request.ParentADef is null)
            {
                return CreateAbortResult("repro_missing_parent_a_def");
            }

            if (request.ParentBDef is null)
            {
                return CreateAbortResult("repro_missing_parent_b_def");
            }

            if (request.StrengthSource != StrengthSource.StrengthBaseOnly)
            {
                return CreateAbortResult("repro_strength_source_not_supported");
            }

            var parentA = await TryLoadParentAsync(request.ParentADef, "a").ConfigureAwait(false);
            if (parentA.AbortReason is not null)
            {
                return CreateAbortResult(parentA.AbortReason);
            }

            var parentB = await TryLoadParentAsync(request.ParentBDef, "b").ConfigureAwait(false);
            if (parentB.AbortReason is not null)
            {
                return CreateAbortResult(parentB.AbortReason);
            }

            return EvaluateStageA(parentA.Parsed!, parentB.Parsed!, request.Config);
        }
        catch
        {
            return CreateAbortResult("repro_internal_error");
        }
    }

    private static ReproduceResult EvaluateStageA(ParsedParent parentA, ParsedParent parentB, ReproduceConfig? config)
    {
        var presentA = CountPresentRegions(parentA.Header);
        var presentB = CountPresentRegions(parentB.Header);

        if (!AreFormatContractsCompatible(parentA.Header, parentB.Header))
        {
            return CreateAbortResult("repro_format_incompatible", 0f, presentA, presentB);
        }

        if (!HaveMatchingRegionPresence(parentA.Header, parentB.Header))
        {
            return CreateAbortResult("repro_region_presence_mismatch", 0f, presentA, presentB);
        }

        var tolerance = ResolveSpanTolerance(config);
        var spanScore = ComputeRegionSpanScore(parentA.Header, parentB.Header, tolerance, out var spanMismatch);
        if (spanMismatch)
        {
            return CreateAbortResult("repro_region_span_mismatch", spanScore, presentA, presentB);
        }

        return new ReproduceResult
        {
            Report = new SimilarityReport
            {
                Compatible = true,
                AbortReason = StageBNotImplementedReason,
                SimilarityScore = spanScore,
                RegionSpanScore = spanScore,
                FunctionScore = 0f,
                ConnectivityScore = 0f,
                RegionsPresentA = (uint)presentA,
                RegionsPresentB = (uint)presentB,
                RegionsPresentChild = 0
            },
            Summary = new MutationSummary(),
            Spawned = false
        };
    }

    private static bool AreFormatContractsCompatible(NbnHeaderV2 parentA, NbnHeaderV2 parentB)
    {
        if (parentA.AxonStride != parentB.AxonStride)
        {
            return false;
        }

        return QuantizationMapEquals(parentA.Quantization.Strength, parentB.Quantization.Strength)
               && QuantizationMapEquals(parentA.Quantization.PreActivationThreshold, parentB.Quantization.PreActivationThreshold)
               && QuantizationMapEquals(parentA.Quantization.ActivationThreshold, parentB.Quantization.ActivationThreshold)
               && QuantizationMapEquals(parentA.Quantization.ParamA, parentB.Quantization.ParamA)
               && QuantizationMapEquals(parentA.Quantization.ParamB, parentB.Quantization.ParamB);
    }

    private static bool QuantizationMapEquals(QuantizationMap left, QuantizationMap right)
        => left.MapType == right.MapType
           && left.Min.Equals(right.Min)
           && left.Max.Equals(right.Max)
           && left.Gamma.Equals(right.Gamma);

    private static bool HaveMatchingRegionPresence(NbnHeaderV2 parentA, NbnHeaderV2 parentB)
    {
        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var aPresent = parentA.Regions[i].NeuronSpan > 0;
            var bPresent = parentB.Regions[i].NeuronSpan > 0;
            if (aPresent != bPresent)
            {
                return false;
            }
        }

        return true;
    }

    private static float ComputeRegionSpanScore(NbnHeaderV2 parentA, NbnHeaderV2 parentB, float tolerance, out bool spanMismatch)
    {
        spanMismatch = false;
        var totalScore = 0f;
        var compared = 0;

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var spanA = parentA.Regions[i].NeuronSpan;
            var spanB = parentB.Regions[i].NeuronSpan;
            if (spanA == 0 && spanB == 0)
            {
                continue;
            }

            if (spanA == 0 || spanB == 0)
            {
                spanMismatch = true;
                continue;
            }

            var maxSpan = Math.Max(spanA, spanB);
            var diffRatio = maxSpan == 0 ? 0f : MathF.Abs(spanA - spanB) / maxSpan;
            if (diffRatio > tolerance)
            {
                spanMismatch = true;
            }

            totalScore += 1f - Math.Clamp(diffRatio, 0f, 1f);
            compared++;
        }

        if (compared == 0)
        {
            return 1f;
        }

        return Math.Clamp(totalScore / compared, 0f, 1f);
    }

    private static float ResolveSpanTolerance(ReproduceConfig? config)
        => config is null ? 0f : Math.Max(config.MaxRegionSpanDiffRatio, 0f);

    private static int CountPresentRegions(NbnHeaderV2 header)
    {
        var count = 0;
        for (var i = 0; i < header.Regions.Length; i++)
        {
            if (header.Regions[i].NeuronSpan > 0)
            {
                count++;
            }
        }

        return count;
    }

    private static async Task<LoadParentResult> TryLoadParentAsync(ArtifactRef reference, string label)
    {
        var prefix = label == "a" ? "repro_parent_a" : "repro_parent_b";
        if (!string.IsNullOrWhiteSpace(reference.MediaType) && !IsNbnMediaType(reference.MediaType))
        {
            return new LoadParentResult(null, $"{prefix}_media_type_invalid");
        }

        if (!reference.TryToSha256Bytes(out var hashBytes) || hashBytes.Length != Sha256Hash.Length)
        {
            return new LoadParentResult(null, $"{prefix}_sha256_invalid");
        }

        var hash = new Sha256Hash(hashBytes);
        var storeRoot = ResolveArtifactRoot(reference.StoreUri);
        var store = new LocalArtifactStore(new ArtifactStoreOptions(storeRoot));
        var manifest = await store.TryGetManifestAsync(hash).ConfigureAwait(false);
        if (manifest is null)
        {
            return new LoadParentResult(null, $"{prefix}_artifact_not_found");
        }

        if (!IsNbnMediaType(manifest.MediaType))
        {
            return new LoadParentResult(null, $"{prefix}_media_type_invalid");
        }

        byte[] bytes;
        await using (var stream = store.OpenArtifactStream(manifest))
        {
            bytes = await ReadAllBytesAsync(stream, reference.SizeBytes).ConfigureAwait(false);
        }

        NbnHeaderV2 header;
        List<NbnRegionSection> sections;
        try
        {
            header = NbnBinary.ReadNbnHeader(bytes);
            sections = ReadRegions(bytes, header);
        }
        catch
        {
            return new LoadParentResult(null, $"{prefix}_parse_failed");
        }

        var validation = NbnBinaryValidator.ValidateNbn(header, sections);
        if (!validation.IsValid)
        {
            return new LoadParentResult(null, MapValidationAbortReason(validation, prefix));
        }

        return new LoadParentResult(new ParsedParent(header, sections), null);
    }

    private static string MapValidationAbortReason(NbnValidationResult validation, string prefix)
    {
        foreach (var issue in validation.Issues)
        {
            if (IsIoInvariantIssue(issue.Message))
            {
                return $"{prefix}_io_invariants_invalid";
            }
        }

        return $"{prefix}_format_invalid";
    }

    private static bool IsIoInvariantIssue(string message)
        => message.Contains("Axons may not target the input region.", StringComparison.Ordinal)
           || message.Contains("Output region axons may not target the output region.", StringComparison.Ordinal)
           || message.Contains("Duplicate axons from the same source neuron are not allowed.", StringComparison.Ordinal)
           || message.Contains("Input and output regions must not contain deleted neurons.", StringComparison.Ordinal);

    private static List<NbnRegionSection> ReadRegions(ReadOnlySpan<byte> nbnBytes, NbnHeaderV2 header)
    {
        var sections = new List<NbnRegionSection>(NbnConstants.RegionCount);
        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var entry = header.Regions[regionId];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            sections.Add(NbnBinary.ReadNbnRegionSection(nbnBytes, entry.Offset));
        }

        return sections;
    }

    private static async Task<byte[]> ReadAllBytesAsync(Stream stream, ulong reportedSize)
    {
        var capacity = reportedSize > 0 && reportedSize < int.MaxValue ? (int)reportedSize : 0;
        using var ms = capacity > 0 ? new MemoryStream(capacity) : new MemoryStream();
        await stream.CopyToAsync(ms).ConfigureAwait(false);
        return ms.ToArray();
    }

    private static bool IsNbnMediaType(string mediaType)
        => string.Equals(mediaType, NbnMediaType, StringComparison.OrdinalIgnoreCase);

    private static string ResolveArtifactRoot(string? storeUri)
    {
        if (!string.IsNullOrWhiteSpace(storeUri))
        {
            if (Uri.TryCreate(storeUri, UriKind.Absolute, out var uri) && uri.IsFile)
            {
                return uri.LocalPath;
            }

            if (!storeUri.Contains("://", StringComparison.Ordinal))
            {
                return storeUri;
            }
        }

        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        if (!string.IsNullOrWhiteSpace(envRoot))
        {
            return envRoot;
        }

        return Path.Combine(Environment.CurrentDirectory, "artifacts");
    }

    private static ReproduceResult CreateAbortResult(
        string reason,
        float regionSpanScore = 0f,
        int regionsPresentA = 0,
        int regionsPresentB = 0,
        int regionsPresentChild = 0)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = false,
                AbortReason = reason,
                SimilarityScore = 0f,
                RegionSpanScore = Math.Clamp(regionSpanScore, 0f, 1f),
                FunctionScore = 0f,
                ConnectivityScore = 0f,
                RegionsPresentA = (uint)Math.Max(regionsPresentA, 0),
                RegionsPresentB = (uint)Math.Max(regionsPresentB, 0),
                RegionsPresentChild = (uint)Math.Max(regionsPresentChild, 0)
            },
            Summary = new MutationSummary(),
            Spawned = false
        };

    private sealed record ParsedParent(
        NbnHeaderV2 Header,
        IReadOnlyList<NbnRegionSection> Regions);

    private sealed record LoadParentResult(
        ParsedParent? Parsed,
        string? AbortReason);
}
