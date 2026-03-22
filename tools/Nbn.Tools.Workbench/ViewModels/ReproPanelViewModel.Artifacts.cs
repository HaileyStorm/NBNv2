using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
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
        var file = await PickOpenFileAsync($"Select .{extension} file", filter, extension);
        if (file is null)
        {
            return;
        }

        var path = FormatPath(file);
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
            FileTypeFilter =
            [
                new FilePickerFileType(filterName)
                {
                    Patterns = [$"*.{extension}"]
                }
            ]
        };

        var results = await provider.OpenFilePickerAsync(options);
        return results.FirstOrDefault();
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

    private static string BuildDefaultArtifactRoot()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "Nbn.Workbench",
            "repro-artifacts");
        Directory.CreateDirectory(baseDir);
        return baseDir;
    }

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private enum ParentFileKind
    {
        ParentADef,
        ParentAState,
        ParentBDef,
        ParentBState
    }
}
