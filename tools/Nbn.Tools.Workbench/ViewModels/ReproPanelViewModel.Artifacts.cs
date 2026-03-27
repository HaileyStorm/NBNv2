using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Nbn.Proto;
using Nbn.Proto.Repro;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class ReproPanelViewModel
{
    private bool ShouldUseArtifactParents()
        => !string.IsNullOrWhiteSpace(ParentADefPath) && !string.IsNullOrWhiteSpace(ParentBDefPath);

    private async Task<ReproduceResult?> RunByArtifactsAsync(ReproduceConfig config, ulong seed)
    {
        try
        {
            var parentADef = await StoreArtifactReferenceAsync(ParentADefPath, "application/x-nbn");
            var parentBDef = await StoreArtifactReferenceAsync(ParentBDefPath, "application/x-nbn");

            ArtifactRef? parentAState = null;
            ArtifactRef? parentBState = null;
            if (!string.IsNullOrWhiteSpace(ParentAStatePath))
            {
                parentAState = await StoreArtifactReferenceAsync(ParentAStatePath, "application/x-nbs");
            }

            if (!string.IsNullOrWhiteSpace(ParentBStatePath))
            {
                parentBState = await StoreArtifactReferenceAsync(ParentBStatePath, "application/x-nbs");
            }

            var request = new ReproduceByArtifactsRequest
            {
                ParentADef = parentADef,
                ParentBDef = parentBDef,
                ParentAState = parentAState,
                ParentBState = parentBState,
                StrengthSource = SelectedStrengthSource.Value,
                Config = config,
                Seed = seed
            };

            return await _client.ReproduceByArtifactsAsync(request);
        }
        catch (Exception ex)
        {
            Status = $"Artifact upload failed: {ex.Message}";
            return null;
        }
    }

    private async Task<ArtifactRef> StoreArtifactReferenceAsync(string filePath, string mediaType)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new InvalidOperationException("Artifact path is required.");
        }

        var fullPath = Path.GetFullPath(filePath);
        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException("Artifact file not found.", fullPath);
        }

        var artifactRoot = string.IsNullOrWhiteSpace(ArtifactStoreRoot)
            ? BuildDefaultArtifactRoot()
            : ArtifactStoreRoot;
        var bytes = await File.ReadAllBytesAsync(fullPath).ConfigureAwait(false);
        var published = await _artifactPublisher
            .PublishAsync(
                bytes,
                mediaType,
                artifactRoot,
                _connections?.LocalBindHost ?? NetworkAddressDefaults.DefaultBindHost,
                advertisedHost: _connections?.ResolveExplicitLocalAdvertiseHost(),
                label: "Workbench Reproduction",
                preferredPort: _connections?.ResolveReachableArtifactPort())
            .ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(published.AttentionMessage))
        {
            WorkbenchLog.Warn($"Workbench Reproduction: {published.AttentionMessage}");
        }

        return published.ArtifactRef;
    }

    private async Task BrowseParentFileAsync(ParentFileKind kind)
    {
        var extension = kind is ParentFileKind.ParentADef or ParentFileKind.ParentBDef ? "nbn" : "nbs";
        var filter = extension.ToUpperInvariant() + " files";
        var file = await WorkbenchStorageDialogs.PickOpenFileAsync($"Select .{extension} file", filter, extension).ConfigureAwait(false);
        if (file is null)
        {
            return;
        }

        var path = WorkbenchStorageDialogs.FormatPath(file);
        switch (kind)
        {
            case ParentFileKind.ParentADef:
                ParentADefPath = path;
                break;
            case ParentFileKind.ParentAState:
                ParentAStatePath = path;
                break;
            case ParentFileKind.ParentBDef:
                ParentBDefPath = path;
                break;
            case ParentFileKind.ParentBState:
                ParentBStatePath = path;
                break;
        }
    }

    private void ClearParentFiles()
    {
        ParentADefPath = string.Empty;
        ParentAStatePath = string.Empty;
        ParentBDefPath = string.Empty;
        ParentBStatePath = string.Empty;
    }

    private static string BuildDefaultArtifactRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "repro-artifacts");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private enum ParentFileKind
    {
        ParentADef,
        ParentAState,
        ParentBDef,
        ParentBState
    }
}
