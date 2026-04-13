using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Proto.Io;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class VizPanelViewModel
{
    private void QueueDefinitionTopologyHydration(Guid brainId, uint? focusRegionId)
    {
        var hydrationKey = BuildDefinitionHydrationKey(brainId, focusRegionId);
        lock (_pendingDefinitionHydrationGate)
        {
            if (!_pendingDefinitionHydrationKeys.Add(hydrationKey))
            {
                return;
            }
        }

        _ = HydrateDefinitionTopologyAsync(brainId, focusRegionId, hydrationKey);
    }

    private async Task HydrateDefinitionTopologyAsync(Guid brainId, uint? focusRegionId, string hydrationKey)
    {
        await _definitionTopologyGate.WaitAsync().ConfigureAwait(false);
        try
        {
            var resolvedReference = await ResolveDefinitionArtifactReferenceAsync(brainId).ConfigureAwait(false);
            var artifactRef = resolvedReference.Reference;
            if (artifactRef is null || !artifactRef.TryToSha256Bytes(out var shaBytes))
            {
                _dispatcher.Post(() =>
                {
                    if (!_topologyByBrainId.TryGetValue(brainId, out var state))
                    {
                        state = new BrainCanvasTopologyState();
                        state.Regions.Add((uint)NbnConstants.InputRegionId);
                        state.Regions.Add((uint)NbnConstants.OutputRegionId);
                        _topologyByBrainId[brainId] = state;
                        PruneTopologyBrainStates(brainId);
                    }

                    state.DefinitionSource = resolvedReference.Source;
                    state.DefinitionLoadStatus = "No definition artifact reference available.";
                    state.LastDefinitionRootsTried.Clear();
                });
                return;
            }

            var definitionShaHex = Convert.ToHexString(shaBytes).ToLowerInvariant();
            var loadAttempt = await TryLoadDefinitionTopologyAsync(artifactRef, focusRegionId).ConfigureAwait(false);
            if (loadAttempt.Topology is null)
            {
                _dispatcher.Post(() =>
                {
                    if (!_topologyByBrainId.TryGetValue(brainId, out var state))
                    {
                        state = new BrainCanvasTopologyState();
                        state.Regions.Add((uint)NbnConstants.InputRegionId);
                        state.Regions.Add((uint)NbnConstants.OutputRegionId);
                        _topologyByBrainId[brainId] = state;
                        PruneTopologyBrainStates(brainId);
                    }

                    state.DefinitionSource = resolvedReference.Source;
                    state.DefinitionLoadStatus = string.IsNullOrWhiteSpace(loadAttempt.Failure)
                        ? $"Definition {definitionShaHex[..8]} not found in {loadAttempt.RootsTried.Count} candidate roots."
                        : $"Definition {definitionShaHex[..8]} load failed: {loadAttempt.Failure}";
                    state.LastDefinitionRootsTried.Clear();
                    state.LastDefinitionRootsTried.AddRange(loadAttempt.RootsTried.Take(12));
                });
                return;
            }

            var loaded = loadAttempt.Topology.Value;

            _dispatcher.Post(() =>
            {
                if (!_topologyByBrainId.TryGetValue(brainId, out var state))
                {
                    state = new BrainCanvasTopologyState();
                    state.Regions.Add((uint)NbnConstants.InputRegionId);
                    state.Regions.Add((uint)NbnConstants.OutputRegionId);
                    _topologyByBrainId[brainId] = state;
                    PruneTopologyBrainStates(brainId);
                }

                if (state.HasDefinitionRegionTopology
                    && string.Equals(state.DefinitionShaHex, definitionShaHex, StringComparison.OrdinalIgnoreCase)
                    && ((focusRegionId.HasValue && state.DefinitionFocusRegions.Contains(focusRegionId.Value))
                        || (!focusRegionId.HasValue && state.HasDefinitionFullTopology)))
                {
                    return;
                }

                if (!string.Equals(state.DefinitionShaHex, definitionShaHex, StringComparison.OrdinalIgnoreCase))
                {
                    state.Regions.Clear();
                    state.RegionRoutes.Clear();
                    state.NeuronAddresses.Clear();
                    state.NeuronRoutes.Clear();
                    state.DefinitionFocusRegions.Clear();
                    state.HasDefinitionFullTopology = false;
                    state.Regions.Add((uint)NbnConstants.InputRegionId);
                    state.Regions.Add((uint)NbnConstants.OutputRegionId);
                }

                state.DefinitionShaHex = definitionShaHex;
                state.DefinitionSource = resolvedReference.Source;
                state.DefinitionLoadStatus = $"Loaded definition topology ({(focusRegionId.HasValue ? $"focus R{focusRegionId.Value}" : "full brain")}).";
                state.LastDefinitionRootsTried.Clear();
                state.LastDefinitionRootsTried.AddRange(loadAttempt.RootsTried.Take(12));
                state.HasDefinitionRegionTopology = true;
                state.Regions.UnionWith(loaded.Regions);
                UnionBounded(state.RegionRoutes, loaded.RegionRoutes, MaxTopologyRegionRoutesPerBrain);

                if (focusRegionId.HasValue)
                {
                    state.DefinitionFocusRegions.Add(focusRegionId.Value);
                    UnionBounded(state.NeuronAddresses, loaded.NeuronAddresses, MaxTopologyNeuronAddressesPerBrain);
                    UnionBounded(state.NeuronRoutes, loaded.NeuronRoutes, MaxTopologyNeuronRoutesPerBrain);
                }
                else
                {
                    state.HasDefinitionFullTopology = true;
                }

                if (SelectedBrain?.BrainId == brainId)
                {
                    _nextDefinitionHydrationRetryUtc = DateTime.MinValue;
                    RefreshCanvasLayoutOnly();
                }
            });
        }
        catch (Exception ex)
        {
            _dispatcher.Post(() =>
            {
                if (SelectedBrain?.BrainId == brainId)
                {
                    Status = $"Definition topology load failed: {ex.Message}";
                }
            });
        }
        finally
        {
            _definitionTopologyGate.Release();
            lock (_pendingDefinitionHydrationGate)
            {
                _pendingDefinitionHydrationKeys.Remove(hydrationKey);
            }
        }
    }

    private void EnsureDefinitionTopologyCoverage()
    {
        if (SelectedBrain is null)
        {
            return;
        }

        var focusRegionId = TryParseRegionId(RegionFocusText, out var parsedFocusRegionId)
            ? parsedFocusRegionId
            : (uint?)null;
        if (_topologyByBrainId.TryGetValue(SelectedBrain.BrainId, out var state)
            && state.HasDefinitionRegionTopology
            && ((focusRegionId.HasValue && state.DefinitionFocusRegions.Contains(focusRegionId.Value))
                || (!focusRegionId.HasValue && state.HasDefinitionFullTopology)))
        {
            return;
        }

        var now = DateTime.UtcNow;
        if (now < _nextDefinitionHydrationRetryUtc)
        {
            return;
        }

        _nextDefinitionHydrationRetryUtc = now + DefinitionHydrationRetryInterval;
        QueueDefinitionTopologyHydration(SelectedBrain.BrainId, focusRegionId);
    }

    private static string BuildDefinitionHydrationKey(Guid brainId, uint? focusRegionId)
        => focusRegionId.HasValue
            ? $"{brainId:D}:focus:{focusRegionId.Value}"
            : $"{brainId:D}:full";

    private async Task<DefinitionReferenceResolution> ResolveDefinitionArtifactReferenceAsync(Guid brainId)
    {
        var exported = await _brain.ExportBrainDefinitionReferenceAsync(brainId, rebaseOverlays: false).ConfigureAwait(false);
        if (exported is not null && exported.TryToSha256Bytes(out _))
        {
            return new DefinitionReferenceResolution(exported, "export");
        }

        var info = await _brain.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
        if (info?.BaseDefinition is { } baseDefinition && baseDefinition.TryToSha256Bytes(out _))
        {
            return new DefinitionReferenceResolution(baseDefinition, "brain-info");
        }

        return new DefinitionReferenceResolution(null, "none");
    }

    private static async Task<DefinitionTopologyLoadAttempt> TryLoadDefinitionTopologyAsync(Nbn.Proto.ArtifactRef artifactRef, uint? focusRegionId)
    {
        if (!artifactRef.TryToSha256Bytes(out var shaBytes))
        {
            return new DefinitionTopologyLoadAttempt(null, Array.Empty<string>(), "ArtifactRef is missing sha256.");
        }

        var hash = Sha256Hash.FromBytes(shaBytes);
        if (ArtifactStoreResolver.IsNonFileStoreUri(artifactRef.StoreUri))
        {
            var resolver = CreateWorkbenchArtifactStoreResolver();
            var targets = new[] { resolver.Describe(artifactRef.StoreUri) };
            try
            {
                var store = resolver.Resolve(artifactRef.StoreUri);
                var definitionBytes = await TryReadArtifactBytesAsync(store, hash).ConfigureAwait(false);
                return definitionBytes is null
                    ? new DefinitionTopologyLoadAttempt(null, targets, null)
                    : new DefinitionTopologyLoadAttempt(BuildDefinitionTopology(definitionBytes, focusRegionId), targets, null);
            }
            catch (Exception ex)
            {
                return new DefinitionTopologyLoadAttempt(null, targets, ex.Message);
            }
        }

        var candidateRoots = ResolveArtifactStoreRoots(artifactRef.StoreUri);
        foreach (var artifactRoot in candidateRoots)
        {
            try
            {
                var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
                var definitionBytes = await TryReadArtifactBytesAsync(store, hash).ConfigureAwait(false);
                if (definitionBytes is null)
                {
                    continue;
                }

                return new DefinitionTopologyLoadAttempt(BuildDefinitionTopology(definitionBytes, focusRegionId), candidateRoots, null);
            }
            catch
            {
                // Continue probing other candidate roots.
            }
        }

        return new DefinitionTopologyLoadAttempt(null, candidateRoots, null);
    }

    private static ArtifactStoreResolver CreateWorkbenchArtifactStoreResolver()
        => new(new ArtifactStoreResolverOptions(
            localStoreRootPath: ResolveWorkbenchArtifactLocalRoot(),
            cacheRootPath: ResolveWorkbenchArtifactCacheRoot()));

    private static async Task<byte[]?> TryReadArtifactBytesAsync(IArtifactStore store, Sha256Hash hash)
    {
        await using var stream = await store.TryOpenArtifactAsync(hash).ConfigureAwait(false);
        if (stream is null)
        {
            return null;
        }

        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer).ConfigureAwait(false);
        return buffer.ToArray();
    }

    private static string ResolveWorkbenchArtifactLocalRoot()
    {
        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_ROOT");
        return string.IsNullOrWhiteSpace(envRoot)
            ? BuildWorkbenchArtifactRoot("artifacts")
            : envRoot.Trim();
    }

    private static string ResolveWorkbenchArtifactCacheRoot()
    {
        var envRoot = Environment.GetEnvironmentVariable("NBN_ARTIFACT_CACHE_ROOT");
        return string.IsNullOrWhiteSpace(envRoot)
            ? BuildWorkbenchArtifactRoot("artifact-cache")
            : envRoot.Trim();
    }

    private static string BuildWorkbenchArtifactRoot(string suffix)
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return Path.Combine(localAppData, "Nbn.Workbench", suffix);
    }

    private static DefinitionTopologyLoadResult BuildDefinitionTopology(byte[] definitionBytes, uint? focusRegionId)
    {
        var header = NbnBinary.ReadNbnHeader(definitionBytes);
        var regions = new HashSet<uint>();
        var regionRoutes = new HashSet<VizActivityCanvasRegionRoute>();
        var neuronAddresses = new HashSet<uint>();
        var neuronRoutes = new HashSet<VizActivityCanvasNeuronRoute>();

        for (var regionId = (uint)NbnConstants.RegionMinId; regionId <= NbnConstants.RegionMaxId; regionId++)
        {
            var entry = header.Regions[(int)regionId];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            regions.Add(regionId);
            var section = NbnBinary.ReadNbnRegionSection(definitionBytes, entry.Offset);
            var axonCursor = 0;
            for (var neuronIndex = 0; neuronIndex < section.NeuronRecords.Length; neuronIndex++)
            {
                var neuron = section.NeuronRecords[neuronIndex];
                var sourceAddress = ComposeAddressForTopology(regionId, (uint)neuronIndex);
                if (focusRegionId.HasValue && focusRegionId.Value == regionId)
                {
                    neuronAddresses.Add(sourceAddress);
                }

                var axonCount = (int)neuron.AxonCount;
                for (var offset = 0; offset < axonCount && axonCursor < section.AxonRecords.Length; offset++, axonCursor++)
                {
                    var axon = section.AxonRecords[axonCursor];
                    var targetRegion = (uint)axon.TargetRegionId;
                    var targetAddress = ComposeAddressForTopology(targetRegion, (uint)Math.Max(0, axon.TargetNeuronId));
                    regionRoutes.Add(new VizActivityCanvasRegionRoute(regionId, targetRegion));

                    if (!focusRegionId.HasValue)
                    {
                        continue;
                    }

                    if (regionId != focusRegionId.Value && targetRegion != focusRegionId.Value)
                    {
                        continue;
                    }

                    neuronRoutes.Add(new VizActivityCanvasNeuronRoute(sourceAddress, targetAddress));
                    if (regionId == focusRegionId.Value)
                    {
                        neuronAddresses.Add(sourceAddress);
                    }

                    if (targetRegion == focusRegionId.Value)
                    {
                        neuronAddresses.Add(targetAddress);
                    }
                }
            }
        }

        regions.Add((uint)NbnConstants.InputRegionId);
        regions.Add((uint)NbnConstants.OutputRegionId);
        return new DefinitionTopologyLoadResult(regions, regionRoutes, neuronAddresses, neuronRoutes);
    }

    private static IReadOnlyList<string> ResolveArtifactStoreRoots(string? storeUri)
    {
        var roots = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        static string? NormalizeStoreRoot(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            var trimmed = raw.Trim().Trim('"');
            if (trimmed.Length == 0)
            {
                return null;
            }

            if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri) && uri.IsFile)
            {
                return uri.LocalPath;
            }

            return trimmed;
        }

        static bool IsArtifactStoreRoot(string path)
            => File.Exists(Path.Combine(path, "artifacts.db"))
               || Directory.Exists(Path.Combine(path, "chunks"));

        void AddCandidate(string? candidate)
        {
            var normalized = NormalizeStoreRoot(candidate);
            if (string.IsNullOrWhiteSpace(normalized))
            {
                return;
            }

            string fullPath;
            try
            {
                fullPath = Path.GetFullPath(normalized);
            }
            catch
            {
                return;
            }

            if (!Directory.Exists(fullPath))
            {
                return;
            }

            var queue = new Queue<(string Path, int Depth)>();
            queue.Enqueue((fullPath, 0));
            while (queue.Count > 0)
            {
                var (path, depth) = queue.Dequeue();
                if (!Directory.Exists(path))
                {
                    continue;
                }

                if (IsArtifactStoreRoot(path) && seen.Add(path))
                {
                    roots.Add(path);
                }

                if (depth >= 2)
                {
                    continue;
                }

                try
                {
                    foreach (var child in Directory.EnumerateDirectories(path))
                    {
                        queue.Enqueue((child, depth + 1));
                    }
                }
                catch
                {
                    // Ignore permission or IO failures while scanning candidates.
                }
            }
        }

        AddCandidate(storeUri);
        AddCandidate(RepoLocator.ResolvePathFromRepo("artifacts"));
        AddCandidate(Directory.GetCurrentDirectory());
        AddCandidate(AppContext.BaseDirectory);
        AddCandidate(BuildWorkbenchArtifactRoot("designer-artifacts"));
        AddCandidate(BuildWorkbenchArtifactRoot("repro-artifacts"));
        return roots;
    }
    private VizActivityCanvasTopology BuildTopologySnapshotForSelectedBrain()
    {
        if (SelectedBrain is null || !_topologyByBrainId.TryGetValue(SelectedBrain.BrainId, out var state))
        {
            return new VizActivityCanvasTopology(
                new HashSet<uint> { (uint)NbnConstants.InputRegionId, (uint)NbnConstants.OutputRegionId },
                new HashSet<VizActivityCanvasRegionRoute>(),
                new HashSet<uint>(),
                new HashSet<VizActivityCanvasNeuronRoute>());
        }

        var regions = new HashSet<uint>(state.Regions) { (uint)NbnConstants.InputRegionId, (uint)NbnConstants.OutputRegionId };
        return new VizActivityCanvasTopology(
            regions,
            new HashSet<VizActivityCanvasRegionRoute>(state.RegionRoutes),
            new HashSet<uint>(state.NeuronAddresses),
            new HashSet<VizActivityCanvasNeuronRoute>(state.NeuronRoutes));
    }

    private void AccumulateTopology(VizEventItem item)
    {
        if (!Guid.TryParse(item.BrainId, out var brainId))
        {
            return;
        }

        if (!_topologyByBrainId.TryGetValue(brainId, out var state))
        {
            state = new BrainCanvasTopologyState();
            state.Regions.Add((uint)NbnConstants.InputRegionId);
            state.Regions.Add((uint)NbnConstants.OutputRegionId);
            _topologyByBrainId[brainId] = state;
            PruneTopologyBrainStates(brainId);
        }

        if (TryParseRegionForTopology(item.Region, out var eventRegion))
        {
            state.Regions.Add(eventRegion);
        }

        var hasSource = TryParseAddressForTopology(item.Source, out var sourceAddress);
        var hasTarget = TryParseAddressForTopology(item.Target, out var targetAddress);
        if (hasSource)
        {
            var sourceRegion = sourceAddress >> NbnConstants.AddressNeuronBits;
            state.Regions.Add(sourceRegion);
            AddBounded(state.NeuronAddresses, sourceAddress, MaxTopologyNeuronAddressesPerBrain);
        }

        if (hasTarget)
        {
            var targetRegion = targetAddress >> NbnConstants.AddressNeuronBits;
            state.Regions.Add(targetRegion);
            AddBounded(state.NeuronAddresses, targetAddress, MaxTopologyNeuronAddressesPerBrain);
        }

        if (hasSource && hasTarget)
        {
            var sourceRegion = sourceAddress >> NbnConstants.AddressNeuronBits;
            var targetRegion = targetAddress >> NbnConstants.AddressNeuronBits;
            AddBounded(
                state.RegionRoutes,
                new VizActivityCanvasRegionRoute(sourceRegion, targetRegion),
                MaxTopologyRegionRoutesPerBrain);
            AddBounded(
                state.NeuronRoutes,
                new VizActivityCanvasNeuronRoute(sourceAddress, targetAddress),
                MaxTopologyNeuronRoutesPerBrain);
        }
    }

    private void PruneTopologyBrainStates(Guid keepBrainId)
    {
        if (_topologyByBrainId.Count <= MaxTopologyBrainStates)
        {
            return;
        }

        var selectedBrainId = SelectedBrain?.BrainId;
        foreach (var brainId in _topologyByBrainId.Keys.ToArray())
        {
            if (_topologyByBrainId.Count <= MaxTopologyBrainStates)
            {
                return;
            }

            if (brainId == keepBrainId || (selectedBrainId.HasValue && brainId == selectedBrainId.Value))
            {
                continue;
            }

            _topologyByBrainId.Remove(brainId);
        }
    }

    private static void UnionBounded<T>(HashSet<T> target, IEnumerable<T> source, int maxCount)
    {
        foreach (var item in source)
        {
            AddBounded(target, item, maxCount);
        }
    }

    private static void AddBounded<T>(HashSet<T> target, T item, int maxCount)
    {
        var boundedMax = Math.Max(1, maxCount);
        if (target.Count >= boundedMax && !target.Contains(item))
        {
            return;
        }

        target.Add(item);
    }
}
