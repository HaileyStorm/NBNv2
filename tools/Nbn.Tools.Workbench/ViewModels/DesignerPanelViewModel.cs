using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Shared.Format;
using Nbn.Shared.Validation;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DesignerPanelViewModel : ViewModelBase
{
    private const string NoDocumentStatus = "No file loaded.";
    private string _status = "Designer ready.";
    private string _loadedSummary = NoDocumentStatus;
    private string _validationSummary = "Validation not run.";
    private DesignerDocumentType _documentType = DesignerDocumentType.None;
    private byte[]? _documentBytes;
    private string? _documentPath;
    private NbnHeaderV2? _nbnHeader;
    private IReadOnlyList<NbnRegionSection>? _nbnRegions;
    private NbsHeaderV2? _nbsHeader;
    private IReadOnlyList<NbsRegionSection>? _nbsRegions;
    private NbsOverlaySection? _nbsOverlay;

    public DesignerPanelViewModel()
    {
        ValidationIssues = new ObservableCollection<string>();
        NewBrainCommand = new RelayCommand(NewBrain);
        ImportNbnCommand = new AsyncRelayCommand(ImportNbnAsync);
        ImportNbsCommand = new AsyncRelayCommand(ImportNbsAsync);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => _documentBytes is not null);
        ValidateCommand = new RelayCommand(Validate);
    }

    public ObservableCollection<string> ValidationIssues { get; }

    public string Status
    {
        get => _status;
        set => SetProperty(ref _status, value);
    }

    public string LoadedSummary
    {
        get => _loadedSummary;
        set => SetProperty(ref _loadedSummary, value);
    }

    public string ValidationSummary
    {
        get => _validationSummary;
        set => SetProperty(ref _validationSummary, value);
    }

    public RelayCommand NewBrainCommand { get; }

    public AsyncRelayCommand ImportNbnCommand { get; }

    public AsyncRelayCommand ImportNbsCommand { get; }

    public AsyncRelayCommand ExportCommand { get; }

    public RelayCommand ValidateCommand { get; }

    private void NewBrain()
    {
        Status = "New brain creation is not implemented yet.";
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
            var header = NbnBinary.ReadNbnHeader(bytes);
            var regions = ReadNbnRegions(bytes, header);

            _documentType = DesignerDocumentType.Nbn;
            _documentBytes = bytes;
            _documentPath = FormatPath(file);
            _nbnHeader = header;
            _nbnRegions = regions;
            _nbsHeader = null;
            _nbsRegions = null;
            _nbsOverlay = null;

            LoadedSummary = BuildNbnSummary(file.Name, header, regions);
            Status = "NBN imported.";
            ResetValidation();
            ExportCommand.RaiseCanExecuteChanged();
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
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
            var header = NbnBinary.ReadNbsHeader(bytes);
            ReadNbsSections(bytes, header, out var regions, out var overlay);

            _documentType = DesignerDocumentType.Nbs;
            _documentBytes = bytes;
            _documentPath = FormatPath(file);
            _nbsHeader = header;
            _nbsRegions = regions;
            _nbsOverlay = overlay;
            _nbnHeader = null;
            _nbnRegions = null;

            LoadedSummary = BuildNbsSummary(file.Name, header, regions, overlay);
            Status = "NBS imported.";
            ResetValidation();
            ExportCommand.RaiseCanExecuteChanged();
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
        }
    }

    private async Task ExportAsync()
    {
        if (_documentBytes is null || _documentType == DesignerDocumentType.None)
        {
            Status = "Nothing to export.";
            return;
        }

        var extension = _documentType == DesignerDocumentType.Nbn ? "nbn" : "nbs";
        var title = _documentType == DesignerDocumentType.Nbn ? "Export .nbn" : "Export .nbs";
        var suggestedName = !string.IsNullOrWhiteSpace(_documentPath)
            ? Path.GetFileName(_documentPath)
            : $"brain.{extension}";

        var file = await PickSaveFileAsync(title, $"{extension.ToUpperInvariant()} files", extension, suggestedName);
        if (file is null)
        {
            Status = "Export canceled.";
            return;
        }

        try
        {
            await WriteAllBytesAsync(file, _documentBytes);
            Status = $"Exported to {FormatPath(file)}.";
        }
        catch (Exception ex)
        {
            Status = $"Export failed: {ex.Message}";
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
                if (_nbnHeader is null || _nbnRegions is null)
                {
                    Status = "NBN not loaded.";
                    return;
                }

                result = NbnBinaryValidator.ValidateNbn(_nbnHeader, _nbnRegions);
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

        ValidationSummary = result.IsValid
            ? "Validation passed."
            : $"Validation found {result.Issues.Count} issue(s).";
        Status = "Validation complete.";
    }

    private void ResetValidation()
    {
        ValidationIssues.Clear();
        ValidationSummary = "Validation not run.";
    }

    private static IReadOnlyList<NbnRegionSection> ReadNbnRegions(byte[] data, NbnHeaderV2 header)
    {
        var regions = new List<NbnRegionSection>();
        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            regions.Add(NbnBinary.ReadNbnRegionSection(data, entry.Offset));
        }

        return regions;
    }

    private static void ReadNbsSections(byte[] data, NbsHeaderV2 header, out IReadOnlyList<NbsRegionSection> regions, out NbsOverlaySection? overlay)
    {
        overlay = null;
        var list = new List<NbsRegionSection>();
        var offset = NbnBinary.NbsHeaderBytes;

        while (offset < data.Length)
        {
            if (header.AxonOverlayIncluded && data.Length - offset >= 4)
            {
                var overlayCount = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset, 4));
                var overlaySize = NbnBinary.GetNbsOverlaySectionSize((int)overlayCount);
                if (overlaySize > 0 && offset + overlaySize == data.Length)
                {
                    overlay = NbnBinary.ReadNbsOverlaySection(data, offset);
                    offset += overlay.ByteLength;
                    break;
                }
            }

            var region = NbnBinary.ReadNbsRegionSection(data, offset, header.EnabledBitsetIncluded);
            list.Add(region);
            offset += region.ByteLength;
        }

        regions = list;
    }

    private static string BuildNbnSummary(string fileName, NbnHeaderV2 header, IReadOnlyList<NbnRegionSection> regions)
    {
        var regionCount = regions.Count;
        var neuronTotal = regions.Sum(region => (long)region.NeuronSpan);
        return $"Loaded NBN: {fileName} • regions {regionCount} • neurons {neuronTotal} • stride {header.AxonStride}";
    }

    private static string BuildNbsSummary(string fileName, NbsHeaderV2 header, IReadOnlyList<NbsRegionSection> regions, NbsOverlaySection? overlay)
    {
        var overlayCount = overlay?.Records.Length ?? 0;
        return $"Loaded NBS: {fileName} • regions {regions.Count} • overlay {overlayCount} • tick {header.SnapshotTickId}";
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

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;
}

public enum DesignerDocumentType
{
    None,
    Nbn,
    Nbs
}
