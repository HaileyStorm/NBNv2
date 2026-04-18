using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Media;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using Nbn.Shared.Validation;
using Nbn.Tools.Workbench.Services;
using ProtoControl = Nbn.Proto.Control;
using ProtoShardPlanMode = Nbn.Proto.Control.ShardPlanMode;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class DesignerPanelViewModel
{
    private void NewBrain()
    {
        ClearResetConfirmation();
        var seed = GenerateSeed();
        var brainId = Guid.NewGuid();
        var brain = new DesignerBrainViewModel("Untitled Brain", brainId, seed, 1024);
        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            brain.Regions.Add(new DesignerRegionViewModel(i));
        }

        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        AddDefaultNeuron(inputRegion);

        var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
        AddDefaultNeuron(outputRegion);

        brain.UpdateTotals();

        ApplyLoadedBrainDocument(
            brain,
            loadedLabel: brain.Name,
            status: "New brain created.",
            isDirty: true);
    }

    private void ResetBrain()
    {
        if (Brain is null)
        {
            Status = "No design loaded.";
            return;
        }

        if (IsDesignDirty && !_resetPending)
        {
            _resetPending = true;
            OnPropertyChanged(nameof(ResetBrainButtonLabel));
            OnPropertyChanged(nameof(ResetBrainButtonBackground));
            OnPropertyChanged(nameof(ResetBrainButtonForeground));
            OnPropertyChanged(nameof(ResetBrainButtonBorder));
            Status = ResetPendingText;
            return;
        }

        ClearResetConfirmation();

        foreach (var region in Brain.Regions)
        {
            region.Neurons.Clear();
            if (region.IsInput || region.IsOutput)
            {
                region.Neurons.Add(CreateDefaultNeuron(region, 0));
            }

            region.UpdateCounts();
        }

        Brain.UpdateTotals();
        SelectHomeNeuron(Brain);
        SetDesignDirty(true);
        ResetValidation();
        UpdateLoadedSummary();
        RefreshRegionView();
        Status = "Brain reset.";
    }

    private async Task ImportNbnAsync()
    {
        var file = await PickOpenFileAsync("Import .nbn", "NBN files", "nbn");
        if (file is null)
        {
            Status = "Import canceled.";
            return;
        }

        try
        {
            var bytes = await ReadAllBytesAsync(file);
            _ = TryImportNbnFromBytes(bytes, file.Name, FormatPath(file));
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
        }
    }

    /// <summary>
    /// Imports validated <c>.nbn</c> bytes into the designer surface.
    /// </summary>
    internal bool TryImportNbnFromBytes(byte[] bytes, string fileName, string? documentPath = null)
    {
        if (bytes is null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = "Imported";
        }

        try
        {
            var header = NbnBinary.ReadNbnHeader(bytes);
            var regions = ReadNbnRegions(bytes, header);
            var validation = NbnBinaryValidator.ValidateNbn(header, regions);
            if (!validation.IsValid)
            {
                Status = $"Import failed: Invalid .nbn ({fileName}): {FormatValidationIssueSummary(validation)}";
                return false;
            }

            var brain = BuildDesignerBrainFromNbn(header, regions, Path.GetFileNameWithoutExtension(fileName));
            var normalizedFunctions = NormalizeBrainFunctionConstraints(brain);

            ApplyLoadedBrainDocument(
                brain,
                loadedLabel: fileName,
                status: normalizedFunctions == 0
                    ? "NBN imported."
                    : $"NBN imported. Normalized {normalizedFunctions} neuron function setting(s) for IO constraints.",
                isDirty: normalizedFunctions > 0,
                documentPath: documentPath ?? fileName);
            return true;
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
            return false;
        }
    }

    private async Task ImportNbsAsync()
    {
        var file = await PickOpenFileAsync("Import .nbs", "NBS files", "nbs");
        if (file is null)
        {
            Status = "Import canceled.";
            return;
        }

        try
        {
            var bytes = await ReadAllBytesAsync(file);
            _ = TryImportNbsFromBytes(bytes, file.Name, FormatPath(file), "NBS imported.");
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
        }
    }

    /// <summary>
    /// Imports <c>.nbs</c> bytes so the snapshot can be inspected, exported, or restored.
    /// </summary>
    internal bool TryImportNbsFromBytes(
        byte[] bytes,
        string fileName,
        string? documentPath = null,
        string successStatus = "NBS imported.")
    {
        if (bytes is null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        if (string.IsNullOrWhiteSpace(fileName))
        {
            fileName = "Imported snapshot";
        }

        try
        {
            var header = NbnBinary.ReadNbsHeader(bytes);
            ReadNbsSections(bytes, header, out var regions, out var overlay);

            ApplyLoadedSnapshotDocument(
                bytes,
                loadedLabel: fileName,
                header,
                regions,
                overlay,
                status: successStatus,
                documentPath: documentPath ?? fileName);
            return true;
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
            return false;
        }
    }

    private async Task ExportAsync()
    {
        if (_documentType == DesignerDocumentType.None)
        {
            Status = "Nothing to export.";
            return;
        }

        if (_documentType == DesignerDocumentType.Nbs)
        {
            if (_snapshotBytes is null)
            {
                Status = "Snapshot data missing.";
                return;
            }

            var file = await PickSaveFileAsync("Export .nbs", "NBS files", "nbs", SuggestedName("nbs"));
            if (file is null)
            {
                Status = "Export canceled.";
                return;
            }

            try
            {
                await WriteAllBytesAsync(file, _snapshotBytes);
                Status = $"Exported to {FormatPath(file)}.";
            }
            catch (Exception ex)
            {
                Status = $"Export failed: {ex.Message}";
            }

            return;
        }

        if (!TryBuildNbn(out var header, out var sections, out var error))
        {
            Status = error ?? "Export failed.";
            return;
        }

        var suggestedName = SuggestedName("nbn");
        var saveFile = await PickSaveFileAsync("Export .nbn", "NBN files", "nbn", suggestedName);
        if (saveFile is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            var bytes = NbnBinary.WriteNbn(header, sections);
            await WriteAllBytesAsync(saveFile, bytes);
            SetDesignDirty(false);
            Status = $"Exported to {FormatPath(saveFile)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
        }
    }

    private async Task ExportSnapshotAsync()
    {
        if (!CanExportSnapshot)
        {
            Status = "No design loaded.";
            return;
        }

        if (!TryBuildSnapshot(out var snapshotBytes, out var error))
        {
            Status = error ?? "Snapshot export failed.";
            return;
        }

        var saveFile = await PickSaveFileAsync("Export .nbs", "NBS files", "nbs", SuggestedName("nbs"));
        if (saveFile is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            await WriteAllBytesAsync(saveFile, snapshotBytes);
            Status = $"Snapshot exported to {FormatPath(saveFile)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
        }
    }

    private async Task SaveDefinitionArtifactAsync()
    {
        await StoreCurrentDefinitionArtifactAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Stores the current definition into the configured artifact store and updates the bound reference fields.
    /// </summary>
    internal async Task<Nbn.Proto.ArtifactRef?> StoreCurrentDefinitionArtifactAsync()
    {
        if (!IsDesignLoaded)
        {
            Status = "No design loaded.";
            return null;
        }

        if (!TryBuildNbn(out var header, out var sections, out var error))
        {
            Status = error ?? "Artifact save failed.";
            return null;
        }

        var bytes = NbnBinary.WriteNbn(header, sections);
        return await StoreArtifactDocumentAsync(
                bytes,
                "application/x-nbn",
                ArtifactReferenceSlot.Definition,
                "Definition stored in artifact store",
                clearDesignDirty: true,
                failureStatusPrefix: "Artifact save failed")
            .ConfigureAwait(false);
    }

    private async Task SaveSnapshotArtifactAsync()
    {
        await StoreCurrentSnapshotArtifactAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Stores the current or derived snapshot into the configured artifact store and updates the bound reference fields.
    /// </summary>
    internal async Task<Nbn.Proto.ArtifactRef?> StoreCurrentSnapshotArtifactAsync()
    {
        if (!TryResolveSnapshotBytesForArtifactStore(out var snapshotBytes, out var error))
        {
            Status = error ?? "Snapshot artifact save failed.";
            return null;
        }

        return await StoreArtifactDocumentAsync(
                snapshotBytes,
                "application/x-nbs",
                ArtifactReferenceSlot.Snapshot,
                "Snapshot stored in artifact store",
                clearDesignDirty: false,
                failureStatusPrefix: "Snapshot artifact save failed")
            .ConfigureAwait(false);
    }

    private async Task LoadDefinitionArtifactAsync()
    {
        await LoadDefinitionArtifactFromCurrentReferenceAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the current definition artifact reference back into the designer.
    /// </summary>
    internal async Task<bool> LoadDefinitionArtifactFromCurrentReferenceAsync()
    {
        if (!TryBuildDefinitionArtifactReference(out var artifactRef, out var error))
        {
            Status = error ?? "Definition artifact reference is invalid.";
            return false;
        }

        return await LoadArtifactDocumentAsync(
                artifactRef,
                "Definition",
                "definition",
                "nbn",
                "Definition artifact load failed",
                (artifactBytes, displayName, artifactPath) => TryImportNbnFromBytes(artifactBytes, displayName, artifactPath))
            .ConfigureAwait(false);
    }

    private async Task LoadSnapshotArtifactAsync()
    {
        await LoadSnapshotArtifactFromCurrentReferenceAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Loads the current snapshot artifact reference back into the designer.
    /// </summary>
    internal async Task<bool> LoadSnapshotArtifactFromCurrentReferenceAsync()
    {
        if (!TryBuildSnapshotArtifactReference(out var artifactRef, out var error))
        {
            Status = error ?? "Snapshot artifact reference is invalid.";
            return false;
        }

        return await LoadArtifactDocumentAsync(
                artifactRef,
                "Snapshot",
                "snapshot",
                "nbs",
                "Snapshot artifact load failed",
                (artifactBytes, displayName, artifactPath) => TryImportNbsFromBytes(
                    artifactBytes,
                    displayName,
                    artifactPath,
                    "NBS imported from artifact reference."))
            .ConfigureAwait(false);
    }

    private async Task RestoreArtifactBrainAsync()
    {
        await RestoreBrainFromCurrentArtifactReferencesAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Promotes the currently selected definition and optional snapshot references for runtime restore.
    /// </summary>
    internal async Task<ProtoControl.PlacementAck?> RestoreBrainFromCurrentArtifactReferencesAsync()
    {
        if (!TryBuildDefinitionArtifactReference(out var definitionRef, out var definitionError))
        {
            Status = definitionError ?? "Definition artifact reference is invalid.";
            return null;
        }

        Nbn.Proto.ArtifactRef? snapshotRef = null;
        if (!string.IsNullOrWhiteSpace(SnapshotArtifactShaText))
        {
            if (!TryBuildSnapshotArtifactReference(out var parsedSnapshotRef, out var snapshotError))
            {
                Status = snapshotError ?? "Snapshot artifact reference is invalid.";
                return null;
            }

            snapshotRef = parsedSnapshotRef;
        }

        if (!HasSpawnServiceReadiness())
        {
            Status = _connections.BuildSpawnReadinessGuidance();
            return null;
        }

        Guid brainId;
        if (snapshotRef is not null)
        {
            try
            {
                var snapshotBytes = await LoadArtifactBytesAsync(snapshotRef).ConfigureAwait(false);
                if (snapshotBytes is null)
                {
                    Status = BuildArtifactNotFoundStatus("Snapshot", snapshotRef);
                    return null;
                }

                var snapshotHeader = NbnBinary.ReadNbsHeader(snapshotBytes);
                var baseHash = snapshotHeader.BaseNbnSha256;
                if (baseHash is null || !baseHash.AsSpan().SequenceEqual(definitionRef.ToSha256Bytes()))
                {
                    Status = "Artifact restore failed: snapshot base hash does not match the selected definition artifact.";
                    return null;
                }

                brainId = snapshotHeader.BrainId;
            }
            catch (Exception ex)
            {
                Status = $"Artifact restore failed: {ex.Message}";
                return null;
            }
        }
        else
        {
            brainId = Brain?.BrainId ?? Guid.NewGuid();
        }

        Status = snapshotRef is null
            ? "Placing brain from artifact definition..."
            : "Restoring brain from artifact definition + snapshot...";

        try
        {
            var runtimeDefinitionRef = await PrepareRuntimeArtifactReferenceAsync(
                    definitionRef,
                    "Workbench Designer Restore Definition")
                .ConfigureAwait(false);
            var runtimeSnapshotRef = snapshotRef is null
                ? null
                : await PrepareRuntimeArtifactReferenceAsync(
                        snapshotRef,
                        "Workbench Designer Restore Snapshot")
                    .ConfigureAwait(false);

            var placementAck = await _client.RequestPlacementAsync(new ProtoControl.RequestPlacement
            {
                BrainId = brainId.ToProtoUuid(),
                BaseDef = runtimeDefinitionRef,
                LastSnapshot = runtimeSnapshotRef,
                RequestedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                IsRecovery = runtimeSnapshotRef is not null
            }).ConfigureAwait(false);

            if (placementAck is null)
            {
                Status = snapshotRef is null
                    ? "Artifact placement failed: HiveMind returned no placement acknowledgement."
                    : "Artifact restore failed: HiveMind returned no placement acknowledgement.";
                return null;
            }

            if (!placementAck.Accepted)
            {
                Status = snapshotRef is null
                    ? $"Artifact placement failed: {placementAck.Message}"
                    : $"Artifact restore failed: {placementAck.Message}";
                return placementAck;
            }

            Status = "Waiting for HiveMind placement readiness after artifact restore...";
            var awaitedPlacementAck = await _client
                .AwaitSpawnPlacementAsync(brainId, RuntimeDefaultPlacementWaitTimeoutMs)
                .ConfigureAwait(false);
            if (awaitedPlacementAck?.PlacementReady != true)
            {
                Status = SpawnFailureFormatter.Format(
                    prefix: snapshotRef is null ? "Artifact placement failed" : "Artifact restore failed",
                    ack: awaitedPlacementAck,
                    fallbackMessage: $"Artifact restore failed: brain {brainId:D} did not complete HiveMind placement.");
                return placementAck;
            }

            Status = "Waiting for brain visualization/controller readiness after artifact restore...";
            if (!await WaitForBrainRegistrationAsync(brainId).ConfigureAwait(false))
            {
                Status = $"Artifact restore failed: brain {brainId:D} did not become visualization-ready after HiveMind placement.";
                await _client.KillBrainAsync(brainId, "designer_artifact_restore_registration_timeout").ConfigureAwait(false);
                return placementAck;
            }

            _brainDiscovered?.Invoke(brainId);
            Status = snapshotRef is null
                ? $"Brain placed from artifact definition ({brainId:D})."
                : $"Brain restored from artifact refs ({brainId:D}).";
            return placementAck;
        }
        catch (Exception ex)
        {
            Status = $"Artifact restore failed: {ex.Message}";
            return null;
        }
    }

    private async Task SpawnBrainAsync()
    {
        ClearResetConfirmation();
        if (Brain is null || !IsDesignLoaded)
        {
            Status = "No design loaded.";
            return;
        }

        if (!HasSpawnServiceReadiness())
        {
            Status = _connections.BuildSpawnReadinessGuidance();
            return;
        }

        if (!_validationHasRun)
        {
            Validate();
        }

        if (!_validationPassed)
        {
            Status = "Spawn canceled: validation failed.";
            return;
        }

        Status = "Spawning brain...";

        try
        {
            var shardPlanMode = SelectedShardPlan.Value;

            int? shardCount = null;
            int? maxNeuronsPerShard = null;
            if (shardPlanMode == ShardPlanMode.FixedShardCount)
            {
                if (!TryParseOptionalNonNegativeInt(SpawnShardCountText, out shardCount))
                {
                    Status = "Invalid shard count.";
                    return;
                }
            }
            else if (shardPlanMode == ShardPlanMode.MaxNeuronsPerShard)
            {
                if (!TryParseOptionalNonNegativeInt(SpawnShardTargetNeuronsText, out maxNeuronsPerShard))
                {
                    Status = "Invalid shard target size.";
                    return;
                }
            }

            if (!TryBuildNbn(out var header, out var sections, out var error))
            {
                Status = error ?? "Spawn failed.";
                return;
            }

            var sharedPlanMode = ToSharedShardPlanMode(shardPlanMode);
            ShardPlanResult shardPlan;
            try
            {
                shardPlan = ShardPlanner.BuildPlan(header, sharedPlanMode, shardCount, maxNeuronsPerShard);
            }
            catch (Exception ex)
            {
                Status = $"Shard plan failed: {ex.Message}";
                return;
            }

            var plannedShardCount = shardPlan.Regions.Sum(entry => entry.Value.Count);
            if (plannedShardCount == 0)
            {
                Status = "Shard plan produced no shards.";
                return;
            }

            var designBrainId = Brain.BrainId;
            if (_spawnedBrains.TryGetValue(designBrainId, out var existing))
            {
                var response = await _client.ListBrainsAsync().ConfigureAwait(false);
                if (IsBrainRegistered(response, existing.RuntimeBrainId))
                {
                    Status = $"Brain already spawned ({existing.RuntimeBrainId:D}).";
                    return;
                }

                _spawnedBrains.Remove(designBrainId);
            }

            _ = await _client.GetPlacementWorkerInventoryAsync().ConfigureAwait(false);

            var nbnBytes = NbnBinary.WriteNbn(header, sections);
            var artifactRoot = string.IsNullOrWhiteSpace(SpawnArtifactRoot) ? BuildDefaultArtifactRoot() : SpawnArtifactRoot;
            var publishedArtifact = await _artifactPublisher
                .PublishAsync(
                    nbnBytes,
                    "application/x-nbn",
                    artifactRoot,
                    _connections.LocalBindHost,
                    advertisedHost: _connections.ResolveExplicitLocalAdvertiseHost(),
                    label: "Workbench Designer",
                    preferredPort: _connections.ResolveReachableArtifactPort())
                .ConfigureAwait(false);
            var artifactRef = publishedArtifact.ArtifactRef;
            var inputWidth = header.Regions.Length > NbnConstants.InputRegionId
                ? checked((int)header.Regions[NbnConstants.InputRegionId].NeuronSpan)
                : 0;
            var outputWidth = header.Regions.Length > NbnConstants.OutputRegionId
                ? checked((int)header.Regions[NbnConstants.OutputRegionId].NeuronSpan)
                : 0;
            if (!string.IsNullOrWhiteSpace(publishedArtifact.AttentionMessage))
            {
                WorkbenchLog.Warn($"Designer spawn artifact access: {publishedArtifact.AttentionMessage}");
            }
            if (LogSpawnDiagnostics && WorkbenchLog.Enabled)
            {
                var artifactSha = artifactRef.ToSha256Hex().ToLowerInvariant();
                var regionCount = sections.Count;
                var neuronCount = sections.Sum(section => (long)section.NeuronSpan);
                WorkbenchLog.Info(
                    $"SpawnDiag designBrain={designBrainId:D} artifactSha={artifactSha} artifactStore={artifactRef.StoreUri} backingRoot={artifactRoot} regions={regionCount} neurons={neuronCount} shardPlan={sharedPlanMode} plannedShards={plannedShardCount}");
            }

            Status = string.IsNullOrWhiteSpace(publishedArtifact.AttentionMessage)
                ? "Spawning brain via IO/HiveMind worker placement..."
                : $"Spawning brain via IO/HiveMind worker placement... {publishedArtifact.AttentionMessage}";
            var spawnAck = await _client.SpawnBrainViaIoAsync(new ProtoControl.SpawnBrain
            {
                BrainDef = artifactRef,
                InputWidth = (uint)Math.Max(0, inputWidth),
                OutputWidth = (uint)Math.Max(0, outputWidth)
            }).ConfigureAwait(false);

            if (spawnAck?.BrainId is null
                || !spawnAck.BrainId.TryToGuid(out var spawnedBrainId)
                || spawnedBrainId == Guid.Empty)
            {
                Status = SpawnFailureFormatter.Format(
                    prefix: "Spawn failed",
                    ack: spawnAck,
                    fallbackMessage: "Spawn failed: IO did not return a brain id.");
                return;
            }

            Status = "Waiting for IO/HiveMind placement readiness...";
            var awaitedPlacementAck = await _client
                .AwaitSpawnPlacementViaIoAsync(spawnedBrainId, RuntimeDefaultPlacementWaitTimeoutMs)
                .ConfigureAwait(false);
            if (awaitedPlacementAck?.PlacementReady != true)
            {
                Status = SpawnFailureFormatter.Format(
                    prefix: "Spawn failed",
                    ack: awaitedPlacementAck,
                    fallbackMessage: $"Spawn failed: brain {spawnedBrainId:D} did not complete IO/HiveMind worker placement.");
                await _client.KillBrainAsync(spawnedBrainId, "designer_managed_spawn_placement_timeout").ConfigureAwait(false);
                return;
            }

            Status = "Waiting for brain visualization/controller readiness after IO/HiveMind worker placement...";
            if (!await WaitForBrainRegistrationAsync(spawnedBrainId).ConfigureAwait(false))
            {
                Status = $"Spawn failed: brain {spawnedBrainId:D} did not become visualization-ready after IO/HiveMind worker placement.";
                await _client.KillBrainAsync(spawnedBrainId, "designer_managed_spawn_registration_timeout").ConfigureAwait(false);
                return;
            }

            _spawnedBrains[designBrainId] = DesignerSpawnState.Create(spawnedBrainId);
            _brainDiscovered?.Invoke(spawnedBrainId);
            if (LogSpawnDiagnostics && WorkbenchLog.Enabled)
            {
                WorkbenchLog.Info(
                    $"SpawnDiag runtimeBrain={spawnedBrainId:D} designBrain={designBrainId:D} status=registered");
            }
            Status = $"Brain spawned ({spawnedBrainId:D}). Spawned via IO; worker placement managed by HiveMind.";
            if (shardPlan.Warnings.Count > 0)
            {
                Status = $"{Status} Plan warnings: {string.Join(" ", shardPlan.Warnings)}";
            }
        }
        catch (Exception ex)
        {
            Status = $"Spawn failed: {ex.Message}";
        }
    }

    private void Validate()
    {
        if (_documentType == DesignerDocumentType.None)
        {
            Status = "Nothing to validate.";
            return;
        }

        NbnValidationResult result;
        switch (_documentType)
        {
            case DesignerDocumentType.Nbn:
                if (!TryBuildNbn(out var header, out var regions, out var error))
                {
                    Status = error ?? "Validation failed.";
                    return;
                }

                result = NbnBinaryValidator.ValidateNbn(header, regions);
                break;
            case DesignerDocumentType.Nbs:
                if (_nbsHeader is null || _nbsRegions is null)
                {
                    Status = "NBS not loaded.";
                    return;
                }

                result = NbnBinaryValidator.ValidateNbs(_nbsHeader, _nbsRegions, _nbsOverlay);
                break;
            default:
                Status = "Validation not available.";
                return;
        }

        ValidationIssues.Clear();
        foreach (var issue in result.Issues)
        {
            ValidationIssues.Add(issue.ToString());
        }

        _validationHasRun = true;
        _validationPassed = result.IsValid;
        ValidationSummary = result.IsValid
            ? "Validation passed."
            : $"Validation found {result.Issues.Count} issue(s).";
        Status = "Validation complete.";

    }

    private bool TryBuildSnapshot(out byte[] snapshotBytes, out string? error)
    {
        snapshotBytes = Array.Empty<byte>();
        error = null;

        if (!TryBuildNbn(out var header, out var sections, out var buildError))
        {
            error = buildError ?? "Unable to build base NBN.";
            return false;
        }

        if (Brain is null)
        {
            error = "No design loaded.";
            return false;
        }

        if (!ulong.TryParse(SnapshotTickText, out var tickId))
        {
            error = "Snapshot tick must be a number.";
            return false;
        }

        if (!long.TryParse(SnapshotEnergyText, out var energy))
        {
            error = "Snapshot energy must be a number.";
            return false;
        }

        var nbnBytes = NbnBinary.WriteNbn(header, sections);
        var hash = SHA256.HashData(nbnBytes);
        var flags = 0u;
        if (SnapshotIncludeEnabledBitset)
        {
            flags |= 0x1u;
        }

        var regions = new List<NbsRegionSection>();
        foreach (var region in Brain.Regions)
        {
            if (region.NeuronCount == 0)
            {
                continue;
            }

            var buffer = new short[region.NeuronCount];
            byte[]? enabledBitset = null;
            if (SnapshotIncludeEnabledBitset)
            {
                enabledBitset = new byte[(region.NeuronCount + 7) / 8];
                for (var i = 0; i < region.NeuronCount; i++)
                {
                    if (region.Neurons[i].Exists)
                    {
                        enabledBitset[i / 8] |= (byte)(1 << (i % 8));
                    }
                }
            }

            regions.Add(new NbsRegionSection((byte)region.RegionId, (uint)region.NeuronCount, buffer, enabledBitset));
        }

        var headerNbs = new NbsHeaderV2(
            "NBS2",
            2,
            1,
            9,
            Brain.BrainId,
            tickId,
            (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            energy,
            hash,
            flags,
            QuantizationSchemas.DefaultBuffer);

        snapshotBytes = NbnBinary.WriteNbs(headerNbs, regions);
        return true;
    }
    private static string BuildDefaultArtifactRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "designer-artifacts");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private bool CanLoadDefinitionArtifact()
        => !string.IsNullOrWhiteSpace(DefinitionArtifactShaText);

    private bool CanLoadSnapshotArtifact()
        => !string.IsNullOrWhiteSpace(SnapshotArtifactShaText);

    private bool CanRestoreArtifactBrain()
        => !string.IsNullOrWhiteSpace(DefinitionArtifactShaText);

    private async Task<Nbn.Proto.ArtifactRef> PrepareRuntimeArtifactReferenceAsync(
        Nbn.Proto.ArtifactRef artifactRef,
        string label)
    {
        var published = await _artifactPublisher
            .PromoteAsync(
                artifactRef,
                BuildDefaultArtifactRoot(),
                _connections.LocalBindHost,
                advertisedHost: _connections.ResolveExplicitLocalAdvertiseHost(),
                label: label,
                preferredPort: _connections.ResolveReachableArtifactPort())
            .ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(published.AttentionMessage))
        {
            WorkbenchLog.Warn($"{label}: {published.AttentionMessage}");
        }

        return published.ArtifactRef;
    }

    private bool TryResolveSnapshotBytesForArtifactStore(out byte[] snapshotBytes, out string? error)
    {
        if (IsSnapshotLoaded)
        {
            if (_snapshotBytes is null)
            {
                snapshotBytes = Array.Empty<byte>();
                error = "Snapshot data missing.";
                return false;
            }

            snapshotBytes = _snapshotBytes;
            error = null;
            return true;
        }

        return TryBuildSnapshot(out snapshotBytes, out error);
    }

    private async Task<Nbn.Proto.ArtifactRef?> StoreArtifactDocumentAsync(
        byte[] bytes,
        string mediaType,
        ArtifactReferenceSlot slot,
        string successStatusPrefix,
        bool clearDesignDirty,
        string failureStatusPrefix)
    {
        try
        {
            var artifactRef = await StoreArtifactAsync(bytes, mediaType, ArtifactStoreUri).ConfigureAwait(false);
            ApplyStoredArtifactReference(slot, artifactRef);
            if (clearDesignDirty)
            {
                SetDesignDirty(false);
            }

            var artifactSha = artifactRef.ToSha256Hex();
            Status = $"{successStatusPrefix} ({artifactSha[..8]}).";
            return artifactRef;
        }
        catch (Exception ex)
        {
            Status = $"{failureStatusPrefix}: {ex.Message}";
            return null;
        }
    }

    private async Task<Nbn.Proto.ArtifactRef> StoreArtifactAsync(byte[] bytes, string mediaType, string? storeUri)
    {
        if (bytes is null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        var resolvedStoreUri = ResolveArtifactStoreUriInput(storeUri, ArtifactStoreUri);
        var resolver = CreateWorkbenchArtifactStoreResolver(resolvedStoreUri);
        var store = resolver.Resolve(resolvedStoreUri);
        var manifest = await store.StoreAsync(new MemoryStream(bytes), mediaType).ConfigureAwait(false);
        return manifest.ArtifactId.ToHex().ToArtifactRef((ulong)Math.Max(0L, manifest.ByteLength), mediaType, resolvedStoreUri);
    }

    private async Task<byte[]?> LoadArtifactBytesAsync(Nbn.Proto.ArtifactRef artifactRef)
    {
        if (!artifactRef.TryToSha256Bytes(out var hashBytes))
        {
            throw new InvalidOperationException("ArtifactRef is missing sha256.");
        }

        var resolvedStoreUri = ResolveArtifactStoreUriInput(artifactRef.StoreUri, ArtifactStoreUri);
        var resolver = CreateWorkbenchArtifactStoreResolver(resolvedStoreUri);
        var store = resolver.Resolve(resolvedStoreUri);
        var hash = new Sha256Hash(hashBytes);
        return await TryReadArtifactBytesAsync(store, hash).ConfigureAwait(false);
    }

    private async Task<bool> LoadArtifactDocumentAsync(
        Nbn.Proto.ArtifactRef artifactRef,
        string artifactLabel,
        string displayLabel,
        string extension,
        string failureStatusPrefix,
        Func<byte[], string, string?, bool> importDocument)
    {
        try
        {
            var artifactBytes = await LoadArtifactBytesAsync(artifactRef).ConfigureAwait(false);
            if (artifactBytes is null)
            {
                Status = BuildArtifactNotFoundStatus(artifactLabel, artifactRef);
                return false;
            }

            return importDocument(
                artifactBytes,
                BuildArtifactDisplayName(displayLabel, artifactRef, extension),
                BuildArtifactDocumentPath(artifactRef));
        }
        catch (Exception ex)
        {
            Status = $"{failureStatusPrefix}: {ex.Message}";
            return false;
        }
    }

    private bool TryBuildDefinitionArtifactReference(out Nbn.Proto.ArtifactRef artifactRef, out string? error)
        => TryBuildArtifactReference(
            DefinitionArtifactShaText,
            DefinitionArtifactStoreUriText,
            "application/x-nbn",
            out artifactRef,
            out error);

    private bool TryBuildSnapshotArtifactReference(out Nbn.Proto.ArtifactRef artifactRef, out string? error)
        => TryBuildArtifactReference(
            SnapshotArtifactShaText,
            SnapshotArtifactStoreUriText,
            "application/x-nbs",
            out artifactRef,
            out error);

    private bool TryBuildArtifactReference(
        string shaText,
        string? storeUriText,
        string mediaType,
        out Nbn.Proto.ArtifactRef artifactRef,
        out string? error)
    {
        artifactRef = new Nbn.Proto.ArtifactRef();
        error = null;

        var normalizedSha = shaText?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(normalizedSha))
        {
            error = "Artifact sha256 is required.";
            return false;
        }

        try
        {
            var resolvedStoreUri = ResolveArtifactStoreUriInput(storeUriText, ArtifactStoreUri);
            artifactRef = normalizedSha.ToArtifactRef(0, mediaType, resolvedStoreUri);
            return true;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            error = $"Artifact reference is invalid: {ex.Message}";
            return false;
        }
    }

    private string ResolveArtifactStoreUriInput(string? storeUriText, string? fallbackStoreUri)
    {
        var candidate = string.IsNullOrWhiteSpace(storeUriText)
            ? fallbackStoreUri
            : storeUriText;
        var resolver = CreateWorkbenchArtifactStoreResolver(candidate);
        return resolver.ResolveStoreUriOrDefault(candidate);
    }

    private static ArtifactStoreResolver CreateWorkbenchArtifactStoreResolver(string? preferredStoreUri)
    {
        var defaultLocalRoot = BuildDefaultArtifactRoot();
        var localStoreRoot = ArtifactStoreResolver.TryGetLocalStoreRoot(preferredStoreUri, defaultLocalRoot, out var resolvedLocalRoot)
            ? resolvedLocalRoot
            : defaultLocalRoot;
        return new ArtifactStoreResolver(new ArtifactStoreResolverOptions(localStoreRoot));
    }

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

    private static string BuildArtifactReferenceSummary(string label, Nbn.Proto.ArtifactRef artifactRef)
    {
        var sha = artifactRef.TryToSha256Hex(out var shaHex) ? shaHex : "missing";
        var mediaType = string.IsNullOrWhiteSpace(artifactRef.MediaType) ? "unknown" : artifactRef.MediaType.Trim();
        var store = DescribeArtifactStore(artifactRef.StoreUri);
        return $"{label}: sha={sha} media={mediaType} size={artifactRef.SizeBytes} store={store}";
    }

    private void ApplyStoredArtifactReference(ArtifactReferenceSlot slot, Nbn.Proto.ArtifactRef artifactRef)
    {
        var sha = artifactRef.ToSha256Hex();
        var storeUri = artifactRef.StoreUri ?? string.Empty;
        var summaryLabel = slot switch
        {
            ArtifactReferenceSlot.Definition => "Definition",
            ArtifactReferenceSlot.Snapshot => "Snapshot",
            _ => throw new ArgumentOutOfRangeException(nameof(slot), slot, "Unknown artifact reference slot.")
        };
        var summary = BuildArtifactReferenceSummary(summaryLabel, artifactRef);
        switch (slot)
        {
            case ArtifactReferenceSlot.Definition:
                DefinitionArtifactShaText = sha;
                DefinitionArtifactStoreUriText = storeUri;
                DefinitionArtifactSummary = summary;
                break;
            case ArtifactReferenceSlot.Snapshot:
                SnapshotArtifactShaText = sha;
                SnapshotArtifactStoreUriText = storeUri;
                SnapshotArtifactSummary = summary;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(slot), slot, "Unknown artifact reference slot.");
        }
    }

    private static string BuildArtifactNotFoundStatus(string artifactLabel, Nbn.Proto.ArtifactRef artifactRef)
        => $"{artifactLabel} artifact {artifactRef.ToSha256Hex()[..8]} was not found in {DescribeArtifactStore(artifactRef.StoreUri)}.";

    private static string BuildArtifactDocumentPath(Nbn.Proto.ArtifactRef artifactRef)
    {
        var store = DescribeArtifactStore(artifactRef.StoreUri);
        var sha = artifactRef.TryToSha256Hex(out var shaHex) ? shaHex : "missing";
        return $"{store}#{sha}";
    }

    private static string BuildArtifactDisplayName(string label, Nbn.Proto.ArtifactRef artifactRef, string extension)
    {
        var sha = artifactRef.TryToSha256Hex(out var shaHex) ? shaHex[..8] : "missing";
        return $"{label}-{sha}.{extension}";
    }

    private static string DescribeArtifactStore(string? storeUri)
        => string.IsNullOrWhiteSpace(storeUri) ? "(default local store)" : storeUri.Trim();

    private async Task<bool> WaitForBrainRegistrationAsync(Guid brainId)
    {
        var deadline = DateTime.UtcNow + SpawnRegistrationTimeout;
        while (DateTime.UtcNow <= deadline)
        {
            var lifecycle = await _client.GetPlacementLifecycleAsync(brainId).ConfigureAwait(false);
            if (IsPlacementVisualizationReady(lifecycle, brainId))
            {
                return true;
            }

            await Task.Delay(SpawnRegistrationPollInterval).ConfigureAwait(false);
        }

        return false;
    }

    private static bool IsBrainRegistered(Nbn.Proto.Settings.BrainListResponse? response, Guid brainId)
    {
        if (response?.Brains is null)
        {
            return false;
        }

        foreach (var entry in response.Brains)
        {
            if (entry.BrainId is null || !entry.BrainId.TryToGuid(out var candidate) || candidate != brainId)
            {
                continue;
            }

            return !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static bool IsPlacementVisualizationReady(ProtoControl.PlacementLifecycleInfo? lifecycle, Guid brainId)
    {
        if (lifecycle?.BrainId is null
            || !lifecycle.BrainId.TryToGuid(out var candidate)
            || candidate != brainId)
        {
            return false;
        }

        return (lifecycle.LifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleAssigned
                || lifecycle.LifecycleState == ProtoControl.PlacementLifecycleState.PlacementLifecycleRunning)
               && lifecycle.RegisteredShards > 0;
    }

    private string SuggestedName(string extension)
    {
        if (!string.IsNullOrWhiteSpace(_documentPath))
        {
            var name = Path.GetFileNameWithoutExtension(_documentPath);
            return $"{name}.{extension}";
        }

        return $"brain.{extension}";
    }

    private static async Task<IStorageFile?> PickOpenFileAsync(string title, string filterName, string extension)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false,
            FileTypeFilter = new List<FilePickerFileType>
            {
                new(filterName) { Patterns = new List<string> { $"*.{extension}" } }
            }
        };

        var results = await provider.OpenFilePickerAsync(options);
        return results.FirstOrDefault();
    }

    private static async Task<IStorageFile?> PickSaveFileAsync(string title, string filterName, string extension, string? suggestedName)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerSaveOptions
        {
            Title = title,
            DefaultExtension = extension,
            SuggestedFileName = suggestedName,
            FileTypeChoices = new List<FilePickerFileType>
            {
                new(filterName) { Patterns = new List<string> { $"*.{extension}" } }
            }
        };

        return await provider.SaveFilePickerAsync(options);
    }

    private static IStorageProvider? GetStorageProvider()
    {
        var window = GetMainWindow();
        return window?.StorageProvider;
    }

    private static Window? GetMainWindow()
        => Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop
            ? desktop.MainWindow
            : null;

    private static async Task<byte[]> ReadAllBytesAsync(IStorageFile file)
    {
        await using var stream = await file.OpenReadAsync();
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static async Task WriteAllBytesAsync(IStorageFile file, byte[] bytes)
    {
        await using var stream = await file.OpenWriteAsync();
        stream.SetLength(0);
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    private static SharedShardPlanMode ToSharedShardPlanMode(ShardPlanMode mode)
        => mode switch
        {
            ShardPlanMode.FixedShardCount => SharedShardPlanMode.FixedShardCountPerRegion,
            ShardPlanMode.MaxNeuronsPerShard => SharedShardPlanMode.MaxNeuronsPerShard,
            _ => SharedShardPlanMode.SingleShardPerRegion
        };

    private static ProtoShardPlanMode ToProtoShardPlanMode(ShardPlanMode mode)
        => mode switch
        {
            ShardPlanMode.FixedShardCount => ProtoShardPlanMode.ShardPlanFixed,
            ShardPlanMode.MaxNeuronsPerShard => ProtoShardPlanMode.ShardPlanMaxNeurons,
            _ => ProtoShardPlanMode.ShardPlanSingle
        };

    private static ProtoControl.RequestPlacement BuildPlacementRequest(
        Guid brainId,
        int inputWidth,
        int outputWidth,
        string artifactSha,
        long artifactSize,
        ShardPlanMode shardPlanMode,
        int? shardCount,
        int? maxNeuronsPerShard,
        string artifactRoot)
    {
        var request = new ProtoControl.RequestPlacement
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = (uint)Math.Max(0, inputWidth),
            OutputWidth = (uint)Math.Max(0, outputWidth),
            ShardPlan = new ProtoControl.ShardPlan
            {
                Mode = ToProtoShardPlanMode(shardPlanMode)
            }
        };

        if (!string.IsNullOrWhiteSpace(artifactSha))
        {
            request.BaseDef = artifactSha.ToArtifactRef((ulong)Math.Max(0, artifactSize), "application/x-nbn", artifactRoot);
        }

        if (shardCount is { } count && count > 0)
        {
            request.ShardPlan.ShardCount = (uint)count;
        }

        if (maxNeuronsPerShard is { } max && max > 0)
        {
            request.ShardPlan.MaxNeuronsPerShard = (uint)max;
        }

        return request;
    }

    private static IReadOnlyList<ShardPlanOption> BuildShardPlanOptions()
        => new List<ShardPlanOption>
        {
            new("Single shard per region", ShardPlanMode.SingleShardPerRegion, "Use one shard per region (IO regions stay single)."),
            new("Fixed shard count", ShardPlanMode.FixedShardCount, "Split non-IO regions into N shards (stride-aligned)."),
            new("Max neurons per shard", ShardPlanMode.MaxNeuronsPerShard, "Split non-IO regions by target size (stride-aligned).")
        };

    private static bool TryParseOptionalNonNegativeInt(string value, out int? result)
    {
        result = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        if (!int.TryParse(value, out var parsed) || parsed < 0)
        {
            return false;
        }

        if (parsed == 0)
        {
            return true;
        }

        result = parsed;
        return true;
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private enum ArtifactReferenceSlot
    {
        Definition,
        Snapshot
    }
}
