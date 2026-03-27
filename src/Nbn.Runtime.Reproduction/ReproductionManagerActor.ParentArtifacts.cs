using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Validation;
using Proto;
using ProtoIo = Nbn.Proto.Io;

namespace Nbn.Runtime.Reproduction;

public sealed partial class ReproductionManagerActor
{
    private async Task<ResolvedParentArtifact> ResolveParentArtifactAsync(IContext context, Uuid brainId, string label)
    {
        var prefix = label == "a" ? "repro_parent_a" : "repro_parent_b";

        try
        {
            var info = await context
                .RequestAsync<Nbn.Proto.Io.BrainInfo>(
                    _ioGatewayPid!,
                    new Nbn.Proto.Io.BrainInfoRequest { BrainId = brainId },
                    DefaultRequestTimeout)
                .ConfigureAwait(false);

            if (info is null)
            {
                return new ResolvedParentArtifact(null, null, $"{prefix}_lookup_failed");
            }

            if (!HasArtifactRef(info.BaseDefinition))
            {
                return info.InputWidth == 0 && info.OutputWidth == 0
                    ? new ResolvedParentArtifact(null, null, $"{prefix}_brain_not_found")
                    : new ResolvedParentArtifact(null, null, $"{prefix}_base_def_missing");
            }

            return new ResolvedParentArtifact(
                info.BaseDefinition,
                HasArtifactRef(info.LastSnapshot) ? info.LastSnapshot : null,
                null);
        }
        catch
        {
            return new ResolvedParentArtifact(null, null, $"{prefix}_lookup_failed");
        }
    }

    private async Task<LoadParentResult> TryLoadParentAsync(ArtifactRef reference, string label)
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

        if (TryGetCachedParsedParent(reference, out var cachedParent))
        {
            return new LoadParentResult(cachedParent, null);
        }

        var hash = new Sha256Hash(hashBytes);
        IArtifactStore store;
        try
        {
            store = _artifactStoreResolver.Resolve(reference.StoreUri);
        }
        catch
        {
            return new LoadParentResult(null, $"{prefix}_artifact_store_unavailable");
        }

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
        await using (var stream = await store.TryOpenArtifactAsync(hash).ConfigureAwait(false))
        {
            if (stream is null)
            {
                return new LoadParentResult(null, $"{prefix}_artifact_not_found");
            }

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

        var parsedParent = new ParsedParent(header, sections);
        CacheParsedParent(reference, parsedParent);
        return new LoadParentResult(parsedParent, null);
    }

    private bool TryGetCachedParsedParent(ArtifactRef reference, out ParsedParent parsedParent)
    {
        parsedParent = default!;
        if (!TryBuildParsedParentCacheKey(reference, out var cacheKey))
        {
            return false;
        }

        lock (_parsedParentCacheGate)
        {
            return _parsedParentCache.TryGetValue(cacheKey, out parsedParent!);
        }
    }

    private void CacheParsedParent(ArtifactRef reference, ParsedParent parsedParent)
    {
        if (!TryBuildParsedParentCacheKey(reference, out var cacheKey))
        {
            return;
        }

        lock (_parsedParentCacheGate)
        {
            if (_parsedParentCache.ContainsKey(cacheKey))
            {
                return;
            }

            _parsedParentCache[cacheKey] = parsedParent;
            _parsedParentCacheOrder.Enqueue(cacheKey);
            while (_parsedParentCacheOrder.Count > ParsedParentCacheCapacity)
            {
                var evictedKey = _parsedParentCacheOrder.Dequeue();
                _parsedParentCache.Remove(evictedKey);
            }
        }
    }

    private static bool TryBuildParsedParentCacheKey(ArtifactRef reference, out string cacheKey)
    {
        cacheKey = string.Empty;
        if (!reference.TryToSha256Hex(out var sha) || string.IsNullOrWhiteSpace(sha))
        {
            return false;
        }

        cacheKey = sha;
        return true;
    }

    private async Task<TransformParentResult> ResolveTransformParentAsync(
        ParsedParent baseParent,
        ArtifactRef parentDef,
        ArtifactRef? parentState,
        StrengthSource strengthSource,
        string label)
    {
        if (strengthSource != StrengthSource.StrengthLiveCodes)
        {
            return new TransformParentResult(baseParent);
        }

        if (!HasArtifactRef(parentState))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_ref_missing");
            return new TransformParentResult(baseParent);
        }

        if (!string.IsNullOrWhiteSpace(parentState!.MediaType) && !IsNbsMediaType(parentState.MediaType))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_media_type_invalid");
            return new TransformParentResult(baseParent);
        }

        if (!parentState.TryToSha256Bytes(out var stateHashBytes) || stateHashBytes.Length != Sha256Hash.Length)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_sha256_invalid");
            return new TransformParentResult(baseParent);
        }

        var stateHash = new Sha256Hash(stateHashBytes);
        IArtifactStore store;
        try
        {
            store = _artifactStoreResolver.Resolve(parentState.StoreUri ?? parentDef.StoreUri);
        }
        catch
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_artifact_store_unavailable");
            return new TransformParentResult(baseParent);
        }

        var manifest = await store.TryGetManifestAsync(stateHash).ConfigureAwait(false);
        if (manifest is null)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_artifact_not_found");
            return new TransformParentResult(baseParent);
        }

        if (!IsNbsMediaType(manifest.MediaType))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_media_type_invalid");
            return new TransformParentResult(baseParent);
        }

        byte[] stateBytes;
        await using (var stream = await store.TryOpenArtifactAsync(stateHash).ConfigureAwait(false))
        {
            if (stream is null)
            {
                ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_artifact_not_found");
                return new TransformParentResult(baseParent);
            }

            stateBytes = await ReadAllBytesAsync(stream, parentState.SizeBytes).ConfigureAwait(false);
        }

        NbsHeaderV2 stateHeader;
        List<NbsRegionSection> stateRegions;
        NbsOverlaySection? overlaySection;
        try
        {
            stateHeader = NbnBinary.ReadNbsHeader(stateBytes);
            stateRegions = ReadNbsRegions(stateBytes, baseParent.Header, stateHeader, out overlaySection);
        }
        catch
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_parse_failed");
            return new TransformParentResult(baseParent);
        }

        var stateValidation = NbnBinaryValidator.ValidateNbs(stateHeader, stateRegions, overlaySection);
        if (!stateValidation.IsValid)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_validation_failed", stateValidation.Issues.Count);
            return new TransformParentResult(baseParent);
        }

        if (!IsParentStateCompatibleWithBase(parentDef, baseParent.Header, stateHeader, stateRegions))
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_incompatible_with_base");
            return new TransformParentResult(baseParent);
        }

        if (overlaySection is null)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_missing");
            return new TransformParentResult(baseParent);
        }

        if (overlaySection.Records.Length == 0)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_empty");
            return new TransformParentResult(baseParent);
        }

        var applied = ApplyOverlayStrengthCodes(baseParent, overlaySection);
        if (applied.MatchedRoutes == 0)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_no_matching_routes");
            return new TransformParentResult(baseParent);
        }

        if (applied.IgnoredRoutes > 0)
        {
            ReproductionTelemetry.RecordStrengthOverlayFallback(label, "state_overlay_ignored_routes", applied.IgnoredRoutes);
        }

        ReproductionTelemetry.RecordStrengthOverlayApplied(label, applied.MatchedRoutes);
        return new TransformParentResult(applied.Parent);
    }

    private static List<NbsRegionSection> ReadNbsRegions(
        ReadOnlySpan<byte> nbsBytes,
        NbnHeaderV2 baseHeader,
        NbsHeaderV2 stateHeader,
        out NbsOverlaySection? overlays)
    {
        var offset = NbnBinary.NbsHeaderBytes;
        var includeEnabledBitset = stateHeader.EnabledBitsetIncluded;
        var regions = new List<NbsRegionSection>(NbnConstants.RegionCount);
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            var entry = baseHeader.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var section = NbnBinary.ReadNbsRegionSection(nbsBytes, offset, includeEnabledBitset);
            regions.Add(section);
            offset += section.ByteLength;
        }

        overlays = null;
        if (stateHeader.AxonOverlayIncluded)
        {
            overlays = NbnBinary.ReadNbsOverlaySection(nbsBytes, offset);
        }

        return regions;
    }

    private static bool IsParentStateCompatibleWithBase(
        ArtifactRef parentDef,
        NbnHeaderV2 baseHeader,
        NbsHeaderV2 stateHeader,
        IReadOnlyList<NbsRegionSection> stateRegions)
    {
        if (!parentDef.TryToSha256Bytes(out var baseHashBytes) || baseHashBytes.Length != Sha256Hash.Length)
        {
            return false;
        }

        if (stateHeader.BaseNbnSha256 is null
            || stateHeader.BaseNbnSha256.Length != Sha256Hash.Length
            || !stateHeader.BaseNbnSha256.AsSpan().SequenceEqual(baseHashBytes))
        {
            return false;
        }

        var expectedRegionCount = 0;
        for (var regionId = 0; regionId < baseHeader.Regions.Length; regionId++)
        {
            if (baseHeader.Regions[regionId].NeuronSpan > 0)
            {
                expectedRegionCount++;
            }
        }

        if (stateRegions.Count != expectedRegionCount)
        {
            return false;
        }

        for (var i = 0; i < stateRegions.Count; i++)
        {
            var section = stateRegions[i];
            if (section.RegionId >= baseHeader.Regions.Length)
            {
                return false;
            }

            var entry = baseHeader.Regions[section.RegionId];
            if (entry.NeuronSpan == 0 || section.NeuronSpan != entry.NeuronSpan)
            {
                return false;
            }
        }

        return true;
    }

    private static OverlayApplyResult ApplyOverlayStrengthCodes(ParsedParent baseParent, NbsOverlaySection overlaySection)
    {
        var overlayMap = new Dictionary<(int FromRegion, int FromNeuron, int ToRegion, int ToNeuron), byte>();
        var invalidAddressCount = 0;

        for (var i = 0; i < overlaySection.Records.Length; i++)
        {
            var record = overlaySection.Records[i];
            DecodeAddress(record.FromAddress, out var fromRegion, out var fromNeuron);
            DecodeAddress(record.ToAddress, out var toRegion, out var toNeuron);
            if (!IsValidAddress(fromRegion, fromNeuron) || !IsValidAddress(toRegion, toNeuron))
            {
                invalidAddressCount++;
                continue;
            }

            var normalizedStrength = (byte)Math.Clamp((int)record.StrengthCode, 0, 31);
            overlayMap[(fromRegion, fromNeuron, toRegion, toNeuron)] = normalizedStrength;
        }

        if (overlayMap.Count == 0)
        {
            return new OverlayApplyResult(baseParent, 0, invalidAddressCount);
        }

        var matchedRoutes = new HashSet<(int FromRegion, int FromNeuron, int ToRegion, int ToNeuron)>();
        var sections = new List<NbnRegionSection>(baseParent.Regions.Count);
        var changed = false;

        for (var sectionIndex = 0; sectionIndex < baseParent.Regions.Count; sectionIndex++)
        {
            var section = baseParent.Regions[sectionIndex];
            var axonStarts = BuildAxonStarts(section);
            AxonRecord[]? rewrittenAxons = null;

            for (var neuronId = 0; neuronId < section.NeuronRecords.Length; neuronId++)
            {
                var neuron = section.NeuronRecords[neuronId];
                var axonStart = axonStarts[neuronId];
                for (var axonOffset = 0; axonOffset < neuron.AxonCount; axonOffset++)
                {
                    var axonIndex = axonStart + axonOffset;
                    var current = rewrittenAxons is null ? section.AxonRecords[axonIndex] : rewrittenAxons[axonIndex];
                    var route = ((int)section.RegionId, neuronId, (int)current.TargetRegionId, current.TargetNeuronId);
                    if (!overlayMap.TryGetValue(route, out var overlayStrength))
                    {
                        continue;
                    }

                    matchedRoutes.Add(route);
                    if (current.StrengthCode == overlayStrength)
                    {
                        continue;
                    }

                    rewrittenAxons ??= (AxonRecord[])section.AxonRecords.Clone();
                    rewrittenAxons[axonIndex] = new AxonRecord(overlayStrength, current.TargetNeuronId, current.TargetRegionId);
                    changed = true;
                }
            }

            sections.Add(rewrittenAxons is null
                ? section
                : new NbnRegionSection(
                    section.RegionId,
                    section.NeuronSpan,
                    section.TotalAxons,
                    section.Stride,
                    section.CheckpointCount,
                    section.Checkpoints,
                    section.NeuronRecords,
                    rewrittenAxons));
        }

        var ignoredRoutes = Math.Max(overlayMap.Count - matchedRoutes.Count, 0) + invalidAddressCount;
        if (!changed)
        {
            return new OverlayApplyResult(baseParent, matchedRoutes.Count, ignoredRoutes);
        }

        return new OverlayApplyResult(new ParsedParent(baseParent.Header, sections), matchedRoutes.Count, ignoredRoutes);
    }

    private static bool IsValidAddress(int regionId, int neuronId)
        => regionId >= NbnConstants.RegionMinId
           && regionId <= NbnConstants.RegionMaxId
           && neuronId >= 0
           && neuronId <= NbnConstants.MaxAddressNeuronId;

    private static void DecodeAddress(uint address, out int regionId, out int neuronId)
    {
        regionId = (int)(address >> NbnConstants.AddressNeuronBits);
        neuronId = (int)(address & NbnConstants.AddressNeuronMask);
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

    private static bool IsNbsMediaType(string mediaType)
        => string.Equals(mediaType, NbsMediaType, StringComparison.OrdinalIgnoreCase);

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

        return ArtifactStoreResolverOptions.ResolveDefaultArtifactRootPath();
    }

}
