using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Validation;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DesignerPanelViewModel : ViewModelBase
{
    private const string NoDocumentStatus = "No file loaded.";
    private const string NoDesignStatus = "Create or import a .nbn to edit.";
    private const double BaseCanvasNodeSize = 36;
    private const double BaseCanvasGap = 14;
    private const double CanvasPadding = 16;
    private string _status = "Designer ready.";
    private string _loadedSummary = NoDocumentStatus;
    private string _validationSummary = "Validation not run.";
    private DesignerDocumentType _documentType = DesignerDocumentType.None;
    private byte[]? _snapshotBytes;
    private string? _documentPath;
    private NbsHeaderV2? _nbsHeader;
    private IReadOnlyList<NbsRegionSection>? _nbsRegions;
    private NbsOverlaySection? _nbsOverlay;
    private DesignerBrainViewModel? _brain;
    private DesignerRegionViewModel? _selectedRegion;
    private DesignerNeuronViewModel? _selectedNeuron;
    private DesignerAxonViewModel? _selectedAxon;
    private DesignerNeuronViewModel? _pendingAxonSource;
    private DesignerNeuronViewModel? _hoveredNeuron;
    private bool _isAxonLinkMode;
    private int _defaultAxonStrength = 24;
    private int _regionPageSize = 64;
    private string _regionPageSizeText = "64";
    private int _regionPageIndex;
    private string _regionPageIndexText = "1";
    private int _regionPageCount = 1;
    private string _regionPageSummary = string.Empty;
    private string _regionSizeText = "0";
    private string _jumpNeuronIdText = "0";
    private string _axonTargetRegionText = "1";
    private string _axonTargetNeuronText = "0";
    private double _canvasZoom = 1;
    private double _canvasWidth;
    private double _canvasHeight;
    private string _edgeSummary = string.Empty;
    private string _snapshotTickText = "0";
    private string _snapshotEnergyText = "0";
    private bool _snapshotIncludeEnabledBitset = true;
    private bool _snapshotCostEnabled;
    private bool _snapshotEnergyEnabled;
    private bool _snapshotPlasticityEnabled;

    public DesignerPanelViewModel()
    {
        ValidationIssues = new ObservableCollection<string>();
        VisibleNeurons = new ObservableCollection<DesignerNeuronViewModel>();
        VisibleEdges = new ObservableCollection<DesignerEdgeViewModel>();

        ActivationFunctions = new ObservableCollection<DesignerFunctionOption>(BuildActivationFunctions());
        ResetFunctions = new ObservableCollection<DesignerFunctionOption>(BuildResetFunctions());
        AccumulationFunctions = new ObservableCollection<DesignerFunctionOption>(BuildAccumulationFunctions());

        NewBrainCommand = new RelayCommand(NewBrain);
        ImportNbnCommand = new AsyncRelayCommand(ImportNbnAsync);
        ImportNbsCommand = new AsyncRelayCommand(ImportNbsAsync);
        ExportCommand = new AsyncRelayCommand(ExportAsync, () => _documentType != DesignerDocumentType.None);
        ExportSnapshotCommand = new AsyncRelayCommand(ExportSnapshotAsync, () => CanExportSnapshot);
        ValidateCommand = new RelayCommand(Validate, () => _documentType != DesignerDocumentType.None);

        SelectRegionCommand = new RelayCommand<DesignerRegionViewModel>(SelectRegion);
        SelectNeuronCommand = new RelayCommand<DesignerNeuronViewModel>(HandleNeuronSelection);
        SelectAxonCommand = new RelayCommand<DesignerAxonViewModel>(SelectAxon);
        AddNeuronCommand = new RelayCommand(AddNeuron, () => CanAddNeuron);
        ToggleNeuronEnabledCommand = new RelayCommand(ToggleNeuronEnabled, () => CanToggleNeuron);
        ToggleAxonLinkModeCommand = new RelayCommand(ToggleAxonLinkMode, () => CanEditDesign);
        ClearSelectionCommand = new RelayCommand(ClearSelection, () => CanEditDesign);
        RemoveAxonCommand = new RelayCommand(RemoveSelectedAxon, () => CanRemoveAxon);
        RandomizeSeedCommand = new RelayCommand(RandomizeSeed, () => Brain is not null);
        RandomizeBrainIdCommand = new RelayCommand(RandomizeBrainId, () => Brain is not null);
        ApplyRegionSizeCommand = new RelayCommand(ApplyRegionSize, () => CanEditDesign && SelectedRegion is not null);
        PreviousRegionPageCommand = new RelayCommand(PreviousRegionPage, () => CanEditDesign && RegionPageIndex > 0);
        NextRegionPageCommand = new RelayCommand(NextRegionPage, () => CanEditDesign && RegionPageIndex + 1 < RegionPageCount);
        FirstRegionPageCommand = new RelayCommand(FirstRegionPage, () => CanEditDesign && RegionPageIndex > 0);
        LastRegionPageCommand = new RelayCommand(LastRegionPage, () => CanEditDesign && RegionPageIndex + 1 < RegionPageCount);
        JumpToNeuronCommand = new RelayCommand(JumpToNeuron, () => CanEditDesign && SelectedRegion is not null);
        AddAxonByIdCommand = new RelayCommand(AddAxonById, () => CanEditDesign && SelectedNeuron is not null);
    }

    public ObservableCollection<string> ValidationIssues { get; }

    public ObservableCollection<DesignerFunctionOption> ActivationFunctions { get; }
    public ObservableCollection<DesignerFunctionOption> ResetFunctions { get; }
    public ObservableCollection<DesignerFunctionOption> AccumulationFunctions { get; }
    public ObservableCollection<DesignerNeuronViewModel> VisibleNeurons { get; }
    public ObservableCollection<DesignerEdgeViewModel> VisibleEdges { get; }

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

    public DesignerBrainViewModel? Brain
    {
        get => _brain;
        private set
        {
            if (_brain is not null)
            {
                _brain.PropertyChanged -= OnBrainPropertyChanged;
            }

            if (SetProperty(ref _brain, value))
            {
                if (_brain is not null)
                {
                    _brain.PropertyChanged += OnBrainPropertyChanged;
                }

                OnPropertyChanged(nameof(IsDesignLoaded));
                OnPropertyChanged(nameof(CanEditDesign));
                OnPropertyChanged(nameof(IsDesignVisible));
                OnPropertyChanged(nameof(IsSnapshotVisible));
                OnPropertyChanged(nameof(DesignHint));
                OnPropertyChanged(nameof(CanExportSnapshot));
                UpdateCommandStates();
            }
        }
    }

    public DesignerRegionViewModel? SelectedRegion
    {
        get => _selectedRegion;
        private set
        {
            if (SetProperty(ref _selectedRegion, value))
            {
                OnPropertyChanged(nameof(SelectedRegionLabel));
                UpdateRegionSizeText();
                UpdateJumpNeuronText();
                if (SelectedRegion is not null)
                {
                    AxonTargetRegionText = SelectedRegion.RegionId == NbnConstants.InputRegionId
                        ? "1"
                        : SelectedRegion.RegionId.ToString();
                }
                SetRegionPageIndex(0);
                RefreshRegionView();
                UpdateCommandStates();
            }
        }
    }

    public DesignerNeuronViewModel? SelectedNeuron
    {
        get => _selectedNeuron;
        private set
        {
            if (_selectedNeuron is not null)
            {
                _selectedNeuron.PropertyChanged -= OnSelectedNeuronChanged;
            }

            if (SetProperty(ref _selectedNeuron, value))
            {
                if (_selectedNeuron is not null)
                {
                    _selectedNeuron.PropertyChanged += OnSelectedNeuronChanged;
                }

                OnPropertyChanged(nameof(SelectedNeuronLabel));
                OnPropertyChanged(nameof(HasNeuronSelection));
                UpdateJumpNeuronText();
                RefreshEdges();
                UpdateCommandStates();
            }
        }
    }

    public DesignerAxonViewModel? SelectedAxon
    {
        get => _selectedAxon;
        private set
        {
            if (_selectedAxon is not null)
            {
                _selectedAxon.PropertyChanged -= OnSelectedAxonChanged;
            }

            if (SetProperty(ref _selectedAxon, value))
            {
                if (_selectedAxon is not null)
                {
                    _selectedAxon.PropertyChanged += OnSelectedAxonChanged;
                }

                OnPropertyChanged(nameof(HasAxonSelection));
                RefreshEdges();
                UpdateCommandStates();
            }
        }
    }

    public bool IsAxonLinkMode
    {
        get => _isAxonLinkMode;
        set
        {
            if (SetProperty(ref _isAxonLinkMode, value))
            {
                if (!_isAxonLinkMode)
                {
                    ClearPendingAxonSource();
                }

                OnPropertyChanged(nameof(AxonLinkStatus));
            }
        }
    }

    public int DefaultAxonStrength
    {
        get => _defaultAxonStrength;
        set => SetProperty(ref _defaultAxonStrength, Clamp(value, 0, 31));
    }

    public string RegionPageSizeText
    {
        get => _regionPageSizeText;
        set
        {
            if (!SetProperty(ref _regionPageSizeText, value))
            {
                return;
            }

            if (int.TryParse(value, out var parsed) && parsed > 0)
            {
                _regionPageSize = parsed;
                SetRegionPageIndex(RegionPageIndex);
                RefreshRegionView();
            }
        }
    }

    public int RegionPageIndex
    {
        get => _regionPageIndex;
        private set
        {
            if (SetProperty(ref _regionPageIndex, value))
            {
                _regionPageIndexText = (_regionPageIndex + 1).ToString();
                OnPropertyChanged(nameof(RegionPageIndexText));
                UpdateRegionPageSummary();
                UpdateCommandStates();
            }
        }
    }

    public string RegionPageIndexText
    {
        get => _regionPageIndexText;
        set
        {
            if (!SetProperty(ref _regionPageIndexText, value))
            {
                return;
            }

            if (int.TryParse(value, out var parsed))
            {
                if (parsed - 1 != RegionPageIndex)
                {
                    SetRegionPageIndex(parsed - 1);
                }
            }
        }
    }

    public int RegionPageCount
    {
        get => _regionPageCount;
        private set => SetProperty(ref _regionPageCount, value);
    }

    public string RegionPageSummary
    {
        get => _regionPageSummary;
        private set => SetProperty(ref _regionPageSummary, value);
    }

    public string RegionSizeText
    {
        get => _regionSizeText;
        set => SetProperty(ref _regionSizeText, value);
    }

    public string JumpNeuronIdText
    {
        get => _jumpNeuronIdText;
        set => SetProperty(ref _jumpNeuronIdText, value);
    }

    public string AxonTargetRegionText
    {
        get => _axonTargetRegionText;
        set => SetProperty(ref _axonTargetRegionText, value);
    }

    public string AxonTargetNeuronText
    {
        get => _axonTargetNeuronText;
        set => SetProperty(ref _axonTargetNeuronText, value);
    }

    public double CanvasZoom
    {
        get => _canvasZoom;
        set
        {
            if (SetProperty(ref _canvasZoom, Clamp(value, 0.5, 2.5)))
            {
                OnPropertyChanged(nameof(CanvasNodeSize));
                OnPropertyChanged(nameof(CanvasNodeRadius));
                OnPropertyChanged(nameof(CanvasNodeGap));
                RefreshRegionView();
            }
        }
    }

    public double CanvasNodeSize => BaseCanvasNodeSize * CanvasZoom;
    public double CanvasNodeRadius => CanvasNodeSize / 2;
    public double CanvasNodeGap => BaseCanvasGap * CanvasZoom;

    public double CanvasWidth
    {
        get => _canvasWidth;
        private set => SetProperty(ref _canvasWidth, value);
    }

    public double CanvasHeight
    {
        get => _canvasHeight;
        private set => SetProperty(ref _canvasHeight, value);
    }

    public string EdgeSummary
    {
        get => _edgeSummary;
        private set => SetProperty(ref _edgeSummary, value);
    }

    public string SnapshotTickText
    {
        get => _snapshotTickText;
        set => SetProperty(ref _snapshotTickText, value);
    }

    public string SnapshotEnergyText
    {
        get => _snapshotEnergyText;
        set => SetProperty(ref _snapshotEnergyText, value);
    }

    public bool SnapshotIncludeEnabledBitset
    {
        get => _snapshotIncludeEnabledBitset;
        set => SetProperty(ref _snapshotIncludeEnabledBitset, value);
    }

    public bool SnapshotCostEnabled
    {
        get => _snapshotCostEnabled;
        set => SetProperty(ref _snapshotCostEnabled, value);
    }

    public bool SnapshotEnergyEnabled
    {
        get => _snapshotEnergyEnabled;
        set => SetProperty(ref _snapshotEnergyEnabled, value);
    }

    public bool SnapshotPlasticityEnabled
    {
        get => _snapshotPlasticityEnabled;
        set => SetProperty(ref _snapshotPlasticityEnabled, value);
    }

    public bool IsDesignLoaded => _documentType == DesignerDocumentType.Nbn && Brain is not null;
    public bool IsSnapshotLoaded => _documentType == DesignerDocumentType.Nbs;
    public bool CanEditDesign => IsDesignLoaded;
    public bool CanAddNeuron => CanEditDesign && SelectedRegion is not null;
    public bool CanToggleNeuron => CanEditDesign && SelectedNeuron is not null && !SelectedNeuron.IsRequired;
    public bool CanRemoveAxon => CanEditDesign && SelectedAxon is not null && SelectedNeuron is not null;
    public bool CanExportSnapshot => IsDesignLoaded;

    public bool IsDesignVisible => IsDesignLoaded;
    public bool IsSnapshotVisible => IsSnapshotLoaded;
    public bool HasNeuronSelection => SelectedNeuron is not null;
    public bool HasAxonSelection => SelectedAxon is not null;

    public string DesignHint => IsDesignLoaded ? string.Empty : NoDesignStatus;

    public string SelectedRegionLabel => SelectedRegion is null ? "No region selected" : SelectedRegion.Label;
    public string SelectedNeuronLabel => SelectedNeuron is null
        ? "No neuron selected"
        : $"Region {SelectedNeuron.RegionId} / Neuron {SelectedNeuron.NeuronId}";

    public string AxonLinkStatus
    {
        get
        {
            if (!IsAxonLinkMode)
            {
                return "Axon link tool idle.";
            }

            if (_pendingAxonSource is null)
            {
                return "Axon link mode: click a source neuron.";
            }

            return $"Axon link mode: source N{_pendingAxonSource.NeuronId} set. Click a target neuron.";
        }
    }

    public RelayCommand NewBrainCommand { get; }
    public AsyncRelayCommand ImportNbnCommand { get; }
    public AsyncRelayCommand ImportNbsCommand { get; }
    public AsyncRelayCommand ExportCommand { get; }
    public AsyncRelayCommand ExportSnapshotCommand { get; }
    public RelayCommand ValidateCommand { get; }
    public RelayCommand<DesignerRegionViewModel> SelectRegionCommand { get; }
    public RelayCommand<DesignerNeuronViewModel> SelectNeuronCommand { get; }
    public RelayCommand<DesignerAxonViewModel> SelectAxonCommand { get; }
    public RelayCommand AddNeuronCommand { get; }
    public RelayCommand ToggleNeuronEnabledCommand { get; }
    public RelayCommand ToggleAxonLinkModeCommand { get; }
    public RelayCommand ClearSelectionCommand { get; }
    public RelayCommand RemoveAxonCommand { get; }
    public RelayCommand RandomizeSeedCommand { get; }
    public RelayCommand RandomizeBrainIdCommand { get; }
    public RelayCommand ApplyRegionSizeCommand { get; }
    public RelayCommand PreviousRegionPageCommand { get; }
    public RelayCommand NextRegionPageCommand { get; }
    public RelayCommand FirstRegionPageCommand { get; }
    public RelayCommand LastRegionPageCommand { get; }
    public RelayCommand JumpToNeuronCommand { get; }
    public RelayCommand AddAxonByIdCommand { get; }

    private void NewBrain()
    {
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

        SetDocumentType(DesignerDocumentType.Nbn);
        Brain = brain;
        _snapshotBytes = null;
        _documentPath = null;
        _nbsHeader = null;
        _nbsRegions = null;
        _nbsOverlay = null;

        SelectRegion(inputRegion);
        SelectNeuron(inputRegion.Neurons.FirstOrDefault());
        ResetValidation();
        UpdateLoadedSummary();
        Status = "New brain created.";
        RefreshRegionView();
        ExportCommand.RaiseCanExecuteChanged();
        ValidateCommand.RaiseCanExecuteChanged();
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
            var brain = BuildDesignerBrainFromNbn(header, regions, Path.GetFileNameWithoutExtension(file.Name));

            SetDocumentType(DesignerDocumentType.Nbn);
            Brain = brain;
            _snapshotBytes = null;
            _documentPath = FormatPath(file);
            _nbsHeader = null;
            _nbsRegions = null;
            _nbsOverlay = null;

            var region0 = brain.Regions[NbnConstants.InputRegionId];
            SelectRegion(region0);
            SelectNeuron(region0.Neurons.FirstOrDefault());

            LoadedSummary = BuildDesignSummary(brain, file.Name);
            Status = "NBN imported.";
            ResetValidation();
            RefreshRegionView();
            ExportCommand.RaiseCanExecuteChanged();
            ValidateCommand.RaiseCanExecuteChanged();
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

            SetDocumentType(DesignerDocumentType.Nbs);
            _snapshotBytes = bytes;
            _documentPath = FormatPath(file);
            _nbsHeader = header;
            _nbsRegions = regions;
            _nbsOverlay = overlay;
            Brain = null;
            SelectRegion(null);
            ClearSelection();

            LoadedSummary = BuildNbsSummary(file.Name, header, regions, overlay);
            Status = "NBS imported.";
            ResetValidation();
            ExportCommand.RaiseCanExecuteChanged();
            ValidateCommand.RaiseCanExecuteChanged();
        }
        catch (Exception ex)
        {
            Status = $"Import failed: {ex.Message}";
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

        ValidationSummary = result.IsValid
            ? "Validation passed."
            : $"Validation found {result.Issues.Count} issue(s).";
        Status = "Validation complete.";
    }

    private void AddNeuron()
    {
        if (SelectedRegion is null || Brain is null)
        {
            return;
        }

        var neuron = CreateDefaultNeuron(SelectedRegion, SelectedRegion.Neurons.Count);
        SelectedRegion.Neurons.Add(neuron);
        SelectedRegion.UpdateCounts();
        UpdateRegionSizeText();
        Brain.UpdateTotals();
        EnsureNeuronVisible(neuron);
        SelectNeuron(neuron);
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = $"Neuron {neuron.NeuronId} added to region {SelectedRegion.RegionId}.";
    }

    private void ToggleNeuronEnabled()
    {
        if (SelectedNeuron is null || SelectedRegion is null)
        {
            return;
        }

        if (SelectedNeuron.IsRequired)
        {
            Status = "Input and output neurons cannot be disabled.";
            return;
        }

        SelectedNeuron.Exists = !SelectedNeuron.Exists;
        if (!SelectedNeuron.Exists)
        {
            RemoveInboundAxons(SelectedRegion.RegionId, neuronId => neuronId == SelectedNeuron.NeuronId);
            SelectedNeuron.Axons.Clear();
            SelectedNeuron.UpdateAxonCount();
            SelectedAxon = null;
        }

        SelectedRegion.UpdateCounts();
        Brain?.UpdateTotals();
        RefreshRegionView();
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = SelectedNeuron.Exists
            ? $"Neuron {SelectedNeuron.NeuronId} re-enabled."
            : $"Neuron {SelectedNeuron.NeuronId} disabled.";
    }

    private void ToggleAxonLinkMode()
    {
        IsAxonLinkMode = !IsAxonLinkMode;
        Status = IsAxonLinkMode
            ? "Axon link mode enabled."
            : "Axon link mode disabled.";
        RefreshEdges();
    }

    private void ClearSelection()
    {
        ClearPendingAxonSource();
        SetHoveredNeuron(null);
        SelectNeuron(null);
        SelectAxon(null);
        RefreshEdges();
    }

    private void RemoveSelectedAxon()
    {
        if (SelectedNeuron is null || SelectedAxon is null || SelectedRegion is null)
        {
            return;
        }

        SelectedNeuron.Axons.Remove(SelectedAxon);
        SelectedNeuron.UpdateAxonCount();
        SelectedRegion.UpdateCounts();
        Brain?.UpdateTotals();
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = "Axon removed.";
        SelectAxon(null);
        RefreshEdges();
    }

    private void RandomizeSeed()
    {
        if (Brain is null)
        {
            return;
        }

        Brain.SetSeed(GenerateSeed());
        UpdateLoadedSummary();
        MarkDesignDirty();
        Status = "Brain seed randomized.";
    }

    private void RandomizeBrainId()
    {
        if (Brain is null)
        {
            return;
        }

        Brain.SetBrainId(Guid.NewGuid());
        MarkDesignDirty();
        Status = "Brain ID randomized.";
    }

    private void SelectRegion(DesignerRegionViewModel? region)
    {
        if (region is null)
        {
            if (SelectedRegion is not null)
            {
                SelectedRegion.IsSelected = false;
                SelectedRegion = null;
            }

            return;
        }

        if (SelectedRegion == region)
        {
            return;
        }

        if (SelectedRegion is not null)
        {
            SelectedRegion.IsSelected = false;
        }

        SelectedRegion = region;
        SelectedRegion.IsSelected = true;
        SelectNeuron(null);
    }

    private void SelectNeuron(DesignerNeuronViewModel? neuron)
    {
        if (SelectedNeuron == neuron)
        {
            return;
        }

        if (SelectedNeuron is not null)
        {
            SelectedNeuron.IsSelected = false;
        }

        SelectedNeuron = neuron;
        if (SelectedNeuron is not null)
        {
            SelectedNeuron.IsSelected = true;
            EnsureNeuronVisible(SelectedNeuron);
        }

        SelectAxon(null);
    }

    private void SelectAxon(DesignerAxonViewModel? axon)
    {
        if (SelectedAxon == axon)
        {
            return;
        }

        if (SelectedAxon is not null)
        {
            SelectedAxon.IsSelected = false;
        }

        SelectedAxon = axon;
        if (SelectedAxon is not null)
        {
            SelectedAxon.IsSelected = true;
        }

        RefreshEdges();
    }

    private void HandleNeuronSelection(DesignerNeuronViewModel? neuron)
    {
        if (neuron is null)
        {
            return;
        }

        if (IsAxonLinkMode)
        {
            if (_pendingAxonSource is null)
            {
                SetPendingAxonSource(neuron);
                SelectNeuron(neuron);
                return;
            }

            if (_pendingAxonSource == neuron)
            {
                ClearPendingAxonSource();
                Status = "Axon source cleared.";
                return;
            }

            if (TryAddAxon(_pendingAxonSource, neuron, out var message))
            {
                Status = message ?? "Axon added.";
            }
            else
            {
                Status = message ?? "Unable to add axon.";
            }

            return;
        }

        SelectNeuron(neuron);
    }

    private bool TryAddAxon(DesignerNeuronViewModel source, DesignerNeuronViewModel target, out string? message)
    {
        message = null;
        if (SelectedRegion is null)
        {
            message = "Select a region first.";
            return false;
        }

        if (!source.Exists)
        {
            message = "Source neuron is disabled.";
            return false;
        }

        if (!target.Exists)
        {
            message = "Target neuron is disabled.";
            return false;
        }

        if (target.RegionId == NbnConstants.InputRegionId)
        {
            message = "Axons cannot target the input region.";
            return false;
        }

        if (source.RegionId == NbnConstants.OutputRegionId && target.RegionId == NbnConstants.OutputRegionId)
        {
            message = "Output region neurons cannot target output region.";
            return false;
        }

        if (source.Axons.Count >= NbnConstants.MaxAxonsPerNeuron)
        {
            message = "Source neuron already has max axons.";
            return false;
        }

        if (source.Axons.Any(axon => axon.TargetRegionId == target.RegionId && axon.TargetNeuronId == target.NeuronId))
        {
            message = "Duplicate axon not allowed.";
            return false;
        }

        var axon = new DesignerAxonViewModel(target.RegionId, target.NeuronId, DefaultAxonStrength);
        source.Axons.Add(axon);
        source.UpdateAxonCount();

        var region = Brain?.Regions.FirstOrDefault(r => r.RegionId == source.RegionId);
        region?.UpdateCounts();
        Brain?.UpdateTotals();
        UpdateLoadedSummary();
        MarkDesignDirty();
        SelectNeuron(source);
        SelectAxon(axon);
        message = $"Axon added: R{source.RegionId} N{source.NeuronId} -> R{target.RegionId} N{target.NeuronId}.";
        RefreshEdges();
        return true;
    }

    private void SetPendingAxonSource(DesignerNeuronViewModel neuron)
    {
        ClearPendingAxonSource();
        _pendingAxonSource = neuron;
        _pendingAxonSource.IsPendingSource = true;
        OnPropertyChanged(nameof(AxonLinkStatus));
        RefreshEdges();
    }

    private void ClearPendingAxonSource()
    {
        if (_pendingAxonSource is not null)
        {
            _pendingAxonSource.IsPendingSource = false;
        }

        _pendingAxonSource = null;
        OnPropertyChanged(nameof(AxonLinkStatus));
        RefreshEdges();
    }

    public void SetHoveredNeuron(DesignerNeuronViewModel? neuron)
    {
        if (_hoveredNeuron == neuron)
        {
            return;
        }

        if (_hoveredNeuron is not null)
        {
            _hoveredNeuron.IsHovered = false;
        }

        _hoveredNeuron = neuron;
        if (_hoveredNeuron is not null)
        {
            _hoveredNeuron.IsHovered = true;
        }

        RefreshEdges();
    }

    private void ApplyRegionSize()
    {
        if (SelectedRegion is null || Brain is null)
        {
            return;
        }

        if (!int.TryParse(RegionSizeText, out var targetCount) || targetCount < 0)
        {
            Status = "Region size must be a non-negative integer.";
            return;
        }

        if ((SelectedRegion.IsInput || SelectedRegion.IsOutput) && targetCount < 1)
        {
            Status = "Input/output regions must have at least one neuron.";
            return;
        }

        var current = SelectedRegion.Neurons.Count;
        if (targetCount == current)
        {
            Status = "Region size already set.";
            return;
        }

        if (targetCount > current)
        {
            for (var i = current; i < targetCount; i++)
            {
                SelectedRegion.Neurons.Add(CreateDefaultNeuron(SelectedRegion, i));
            }

            SelectedRegion.UpdateCounts();
            RegionSizeText = targetCount.ToString();
            Brain.UpdateTotals();
            UpdateLoadedSummary();
            MarkDesignDirty();
            EnsureNeuronVisible(SelectedRegion.Neurons.Last());
            Status = $"Region {SelectedRegion.RegionId} expanded to {targetCount} neurons.";
            RefreshRegionView();
            return;
        }

        RemoveInboundAxons(SelectedRegion.RegionId, neuronId => neuronId >= targetCount);
        for (var i = current - 1; i >= targetCount; i--)
        {
            SelectedRegion.Neurons.RemoveAt(i);
        }

        if (SelectedNeuron is not null && SelectedNeuron.RegionId == SelectedRegion.RegionId && SelectedNeuron.NeuronId >= targetCount)
        {
            SelectNeuron(null);
        }

        if (_pendingAxonSource is not null && _pendingAxonSource.RegionId == SelectedRegion.RegionId && _pendingAxonSource.NeuronId >= targetCount)
        {
            ClearPendingAxonSource();
        }

        SelectedRegion.UpdateCounts();
        RegionSizeText = targetCount.ToString();
        Brain.UpdateTotals();
        UpdateLoadedSummary();
        MarkDesignDirty();
        RefreshRegionView();
        Status = targetCount == 0
            ? $"Region {SelectedRegion.RegionId} cleared."
            : $"Region {SelectedRegion.RegionId} trimmed to {targetCount} neurons.";
    }

    private void PreviousRegionPage()
    {
        SetRegionPageIndex(RegionPageIndex - 1);
    }

    private void NextRegionPage()
    {
        SetRegionPageIndex(RegionPageIndex + 1);
    }

    private void FirstRegionPage()
    {
        SetRegionPageIndex(0);
    }

    private void LastRegionPage()
    {
        SetRegionPageIndex(RegionPageCount - 1);
    }

    private void JumpToNeuron()
    {
        if (SelectedRegion is null)
        {
            Status = "Select a region first.";
            return;
        }

        if (!int.TryParse(JumpNeuronIdText, out var neuronId))
        {
            Status = "Neuron ID must be a number.";
            return;
        }

        if (neuronId < 0 || neuronId >= SelectedRegion.Neurons.Count)
        {
            Status = "Neuron ID is out of range.";
            return;
        }

        var neuron = SelectedRegion.Neurons[neuronId];
        EnsureNeuronVisible(neuron);
        SelectNeuron(neuron);
        Status = $"Focused neuron {neuron.NeuronId} in region {SelectedRegion.RegionId}.";
    }

    private void AddAxonById()
    {
        if (Brain is null || SelectedNeuron is null)
        {
            return;
        }

        if (!int.TryParse(AxonTargetRegionText, out var targetRegionId))
        {
            Status = "Target region must be a number.";
            return;
        }

        if (!int.TryParse(AxonTargetNeuronText, out var targetNeuronId))
        {
            Status = "Target neuron must be a number.";
            return;
        }

        var region = Brain.Regions.FirstOrDefault(r => r.RegionId == targetRegionId);
        if (region is null || region.NeuronCount == 0)
        {
            Status = "Target region is empty or missing.";
            return;
        }

        if (targetNeuronId < 0 || targetNeuronId >= region.Neurons.Count)
        {
            Status = "Target neuron is out of range.";
            return;
        }

        var targetNeuron = region.Neurons[targetNeuronId];
        if (!targetNeuron.Exists)
        {
            Status = "Target neuron is disabled.";
            return;
        }

        if (TryAddAxon(SelectedNeuron, targetNeuron, out var message))
        {
            Status = message ?? "Axon added.";
        }
        else
        {
            Status = message ?? "Unable to add axon.";
        }
    }

    private void MarkDesignDirty()
    {
        ValidationSummary = "Validation not run.";
        ValidationIssues.Clear();
    }

    private void SetRegionPageIndex(int index)
    {
        var clamped = Math.Clamp(index, 0, Math.Max(RegionPageCount - 1, 0));
        if (RegionPageIndex == clamped)
        {
            RegionPageIndexText = (RegionPageIndex + 1).ToString();
            UpdateRegionPageSummary();
            return;
        }

        RegionPageIndex = clamped;
        RefreshRegionView();
    }

    private void UpdateRegionPageSummary()
    {
        if (SelectedRegion is null)
        {
            RegionPageSummary = "No region selected.";
            return;
        }

        var total = SelectedRegion.Neurons.Count;
        if (total == 0)
        {
            RegionPageSummary = "No neurons in region.";
            return;
        }

        var start = RegionPageIndex * _regionPageSize;
        var end = Math.Min(total, start + _regionPageSize);
        RegionPageSummary = $"Showing {start}-{end - 1} of {total}";
    }

    private void RefreshRegionView()
    {
        VisibleNeurons.Clear();

        if (SelectedRegion is null)
        {
            CanvasWidth = 0;
            CanvasHeight = 0;
            EdgeSummary = string.Empty;
            return;
        }

        var total = SelectedRegion.Neurons.Count;
        RegionPageCount = Math.Max(1, (int)Math.Ceiling(total / (double)_regionPageSize));
        UpdateCommandStates();
        if (RegionPageIndex >= RegionPageCount)
        {
            RegionPageIndex = Math.Max(RegionPageCount - 1, 0);
        }

        var startIndex = RegionPageIndex * _regionPageSize;
        var endIndex = Math.Min(total, startIndex + _regionPageSize);

        for (var i = startIndex; i < endIndex; i++)
        {
            VisibleNeurons.Add(SelectedRegion.Neurons[i]);
        }

        UpdateCanvasLayout();
        UpdateRegionPageSummary();
        RefreshEdges();
    }

    private void UpdateCanvasLayout()
    {
        var count = VisibleNeurons.Count;
        if (count == 0)
        {
            CanvasWidth = 0;
            CanvasHeight = 0;
            return;
        }

        var columns = Math.Max(1, (int)Math.Ceiling(Math.Sqrt(count)));
        var nodeSize = CanvasNodeSize;
        var gap = CanvasNodeGap;
        var rows = (int)Math.Ceiling(count / (double)columns);

        CanvasWidth = (columns * nodeSize) + ((columns - 1) * gap) + (CanvasPadding * 2);
        CanvasHeight = (rows * nodeSize) + ((rows - 1) * gap) + (CanvasPadding * 2);

        for (var i = 0; i < count; i++)
        {
            var row = i / columns;
            var col = i % columns;
            var x = CanvasPadding + (col * (nodeSize + gap));
            var y = CanvasPadding + (row * (nodeSize + gap));
            VisibleNeurons[i].CanvasX = x;
            VisibleNeurons[i].CanvasY = y;
        }
    }

    private void RefreshEdges()
    {
        VisibleEdges.Clear();
        EdgeSummary = string.Empty;

        if (SelectedRegion is null || VisibleNeurons.Count == 0)
        {
            return;
        }

        var source = IsAxonLinkMode && _pendingAxonSource is not null
            ? _pendingAxonSource
            : SelectedNeuron;

        if (source is null || source.RegionId != SelectedRegion.RegionId)
        {
            return;
        }

        var positions = new Dictionary<int, Point>();
        foreach (var neuron in VisibleNeurons)
        {
            positions[neuron.NeuronId] = new Point(neuron.CanvasX + CanvasNodeRadius, neuron.CanvasY + CanvasNodeRadius);
        }

        if (!positions.TryGetValue(source.NeuronId, out var start))
        {
            return;
        }

        var visible = 0;
        var offPage = 0;

        foreach (var axon in source.Axons)
        {
            if (axon.TargetRegionId != SelectedRegion.RegionId)
            {
                offPage++;
                continue;
            }

            if (!positions.TryGetValue(axon.TargetNeuronId, out var end))
            {
                offPage++;
                continue;
            }

            var isSelected = SelectedAxon is not null
                && SelectedAxon.TargetRegionId == axon.TargetRegionId
                && SelectedAxon.TargetNeuronId == axon.TargetNeuronId;

            VisibleEdges.Add(new DesignerEdgeViewModel(start, end, false, isSelected));
            visible++;
        }

        if (IsAxonLinkMode && _pendingAxonSource is not null && _hoveredNeuron is not null && _hoveredNeuron != _pendingAxonSource)
        {
            if (positions.TryGetValue(_hoveredNeuron.NeuronId, out var hoverEnd))
            {
                VisibleEdges.Add(new DesignerEdgeViewModel(start, hoverEnd, true, false));
            }
        }

        EdgeSummary = source.Axons.Count == 0
            ? "No outgoing axons."
            : $"Edges shown: {visible} (off-page {offPage})";
    }

    private void EnsureNeuronVisible(DesignerNeuronViewModel neuron)
    {
        if (SelectedRegion is null)
        {
            return;
        }

        if (neuron.RegionId != SelectedRegion.RegionId)
        {
            var region = Brain?.Regions.FirstOrDefault(r => r.RegionId == neuron.RegionId);
            if (region is not null)
            {
                SelectRegion(region);
            }
        }

        var targetPage = _regionPageSize == 0 ? 0 : neuron.NeuronId / _regionPageSize;
        if (targetPage != RegionPageIndex)
        {
            SetRegionPageIndex(targetPage);
        }
    }

    private void UpdateRegionSizeText()
    {
        RegionSizeText = SelectedRegion?.NeuronCount.ToString() ?? "0";
    }

    private void UpdateJumpNeuronText()
    {
        if (SelectedNeuron is not null)
        {
            JumpNeuronIdText = SelectedNeuron.NeuronId.ToString();
        }
        else if (SelectedRegion is not null)
        {
            JumpNeuronIdText = "0";
        }
    }

    private void RemoveInboundAxons(int targetRegionId, Func<int, bool> neuronPredicate)
    {
        if (Brain is null)
        {
            return;
        }

        foreach (var region in Brain.Regions)
        {
            foreach (var neuron in region.Neurons)
            {
                if (neuron.Axons.Count == 0)
                {
                    continue;
                }

                var removed = 0;
                for (var i = neuron.Axons.Count - 1; i >= 0; i--)
                {
                    var axon = neuron.Axons[i];
                    if (axon.TargetRegionId == targetRegionId && neuronPredicate(axon.TargetNeuronId))
                    {
                        if (SelectedAxon == axon)
                        {
                            SelectedAxon = null;
                        }

                        neuron.Axons.RemoveAt(i);
                        removed++;
                    }
                }

                if (removed > 0)
                {
                    neuron.UpdateAxonCount();
                }
            }

            region.UpdateCounts();
        }

        Brain.UpdateTotals();
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

        if (SnapshotCostEnabled)
        {
            flags |= 0x4u;
        }

        if (SnapshotEnergyEnabled)
        {
            flags |= 0x8u;
        }

        if (SnapshotPlasticityEnabled)
        {
            flags |= 0x10u;
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

    private void OnBrainPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(DesignerBrainViewModel.Name)
            or nameof(DesignerBrainViewModel.BrainSeed)
            or nameof(DesignerBrainViewModel.BrainSeedText)
            or nameof(DesignerBrainViewModel.AxonStride)
            or nameof(DesignerBrainViewModel.AxonStrideText)
            or nameof(DesignerBrainViewModel.BrainId)
            or nameof(DesignerBrainViewModel.BrainIdText))
        {
            UpdateLoadedSummary();
            MarkDesignDirty();
        }
    }

    private void OnSelectedNeuronChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(DesignerNeuronViewModel.ActivationFunctionId)
            or nameof(DesignerNeuronViewModel.ResetFunctionId)
            or nameof(DesignerNeuronViewModel.AccumulationFunctionId)
            or nameof(DesignerNeuronViewModel.ParamACode)
            or nameof(DesignerNeuronViewModel.ParamBCode)
            or nameof(DesignerNeuronViewModel.ActivationThresholdCode)
            or nameof(DesignerNeuronViewModel.PreActivationThresholdCode)
            or nameof(DesignerNeuronViewModel.Exists))
        {
            MarkDesignDirty();
        }
    }

    private void OnSelectedAxonChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(DesignerAxonViewModel.StrengthCode))
        {
            MarkDesignDirty();
            RefreshEdges();
        }
    }

    private void UpdateCommandStates()
    {
        AddNeuronCommand.RaiseCanExecuteChanged();
        ToggleNeuronEnabledCommand.RaiseCanExecuteChanged();
        ToggleAxonLinkModeCommand.RaiseCanExecuteChanged();
        ClearSelectionCommand.RaiseCanExecuteChanged();
        RemoveAxonCommand.RaiseCanExecuteChanged();
        RandomizeSeedCommand.RaiseCanExecuteChanged();
        RandomizeBrainIdCommand.RaiseCanExecuteChanged();
        ApplyRegionSizeCommand.RaiseCanExecuteChanged();
        PreviousRegionPageCommand.RaiseCanExecuteChanged();
        NextRegionPageCommand.RaiseCanExecuteChanged();
        FirstRegionPageCommand.RaiseCanExecuteChanged();
        LastRegionPageCommand.RaiseCanExecuteChanged();
        JumpToNeuronCommand.RaiseCanExecuteChanged();
        AddAxonByIdCommand.RaiseCanExecuteChanged();
        ExportSnapshotCommand.RaiseCanExecuteChanged();
    }

    private void SetDocumentType(DesignerDocumentType documentType)
    {
        if (_documentType == documentType)
        {
            return;
        }

        _documentType = documentType;
        OnPropertyChanged(nameof(IsDesignLoaded));
        OnPropertyChanged(nameof(IsSnapshotLoaded));
        OnPropertyChanged(nameof(IsDesignVisible));
        OnPropertyChanged(nameof(IsSnapshotVisible));
        OnPropertyChanged(nameof(DesignHint));
        OnPropertyChanged(nameof(CanExportSnapshot));
    }

    private void ResetValidation()
    {
        ValidationIssues.Clear();
        ValidationSummary = "Validation not run.";
    }

    private void UpdateLoadedSummary()
    {
        if (_documentType == DesignerDocumentType.Nbn && Brain is not null)
        {
            LoadedSummary = BuildDesignSummary(Brain, _documentPath ?? Brain.Name);
        }
        else if (_documentType == DesignerDocumentType.Nbs && _nbsHeader is not null && _nbsRegions is not null)
        {
            LoadedSummary = BuildNbsSummary(_documentPath ?? "Snapshot", _nbsHeader, _nbsRegions, _nbsOverlay);
        }
        else
        {
            LoadedSummary = NoDocumentStatus;
        }
    }

    private bool TryBuildNbn(out NbnHeaderV2 header, out List<NbnRegionSection> sections, out string? error)
    {
        header = null!;
        sections = new List<NbnRegionSection>();
        error = null;

        if (Brain is null)
        {
            error = "No design loaded.";
            return false;
        }

        if (Brain.AxonStride == 0)
        {
            error = "Axon stride must be greater than zero.";
            return false;
        }

        foreach (var region in Brain.Regions)
        {
            region.UpdateCounts();
        }
        Brain.UpdateTotals();

        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        var regionCounts = Brain.Regions.ToDictionary(region => region.RegionId, region => region.NeuronCount);
        var regionMap = Brain.Regions.ToDictionary(region => region.RegionId, region => region);
        ulong offset = NbnBinary.NbnHeaderBytes;

        for (var i = 0; i < Brain.Regions.Count; i++)
        {
            var region = Brain.Regions[i];
            var neuronSpan = region.NeuronCount;
            if ((region.IsInput || region.IsOutput) && neuronSpan == 0)
            {
                error = "Input and output regions must contain neurons.";
                return false;
            }

            if (neuronSpan == 0)
            {
                directory[region.RegionId] = new NbnRegionDirectoryEntry(0, 0, 0, 0);
                continue;
            }

            if (neuronSpan > NbnConstants.MaxAxonTargetNeuronId)
            {
                error = $"Region {region.RegionId} exceeds max neuron span.";
                return false;
            }

            var neuronRecords = new NeuronRecord[neuronSpan];
            var axonRecords = new List<AxonRecord>();

            for (var neuronIndex = 0; neuronIndex < neuronSpan; neuronIndex++)
            {
                var neuron = region.Neurons[neuronIndex];
                if ((region.IsInput || region.IsOutput) && !neuron.Exists)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} must exist.";
                    return false;
                }

                if (!neuron.Exists && neuron.Axons.Count > 0)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} is disabled but has axons.";
                    return false;
                }

                if (neuron.Axons.Count > NbnConstants.MaxAxonsPerNeuron)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} exceeds max axons.";
                    return false;
                }

                var targets = new HashSet<(int regionId, int neuronId)>();
                var orderedAxons = neuron.Axons
                    .OrderBy(axon => axon.TargetRegionId)
                    .ThenBy(axon => axon.TargetNeuronId)
                    .ToList();

                foreach (var axon in orderedAxons)
                {
                    if (axon.TargetRegionId == NbnConstants.InputRegionId)
                    {
                        error = "Axons cannot target input region.";
                        return false;
                    }

                    if (region.RegionId == NbnConstants.OutputRegionId && axon.TargetRegionId == NbnConstants.OutputRegionId)
                    {
                        error = "Output region neurons cannot target output region.";
                        return false;
                    }

                    if (!regionCounts.TryGetValue(axon.TargetRegionId, out var targetSpan) || targetSpan == 0)
                    {
                        error = $"Target region {axon.TargetRegionId} is missing.";
                        return false;
                    }

                    if (axon.TargetNeuronId < 0 || axon.TargetNeuronId >= targetSpan)
                    {
                        error = $"Target neuron {axon.TargetNeuronId} is out of range for region {axon.TargetRegionId}.";
                        return false;
                    }

                    if (regionMap.TryGetValue(axon.TargetRegionId, out var targetRegion)
                        && !targetRegion.Neurons[axon.TargetNeuronId].Exists)
                    {
                        error = $"Target neuron {axon.TargetNeuronId} in region {axon.TargetRegionId} is disabled.";
                        return false;
                    }

                    if (!targets.Add((axon.TargetRegionId, axon.TargetNeuronId)))
                    {
                        error = $"Duplicate axon from neuron {neuron.NeuronId} in region {region.RegionId}.";
                        return false;
                    }

                    axonRecords.Add(new AxonRecord((byte)axon.StrengthCode, axon.TargetNeuronId, (byte)axon.TargetRegionId));
                }

                neuronRecords[neuronIndex] = new NeuronRecord(
                    (ushort)orderedAxons.Count,
                    (byte)neuron.ParamBCode,
                    (byte)neuron.ParamACode,
                    (byte)neuron.ActivationThresholdCode,
                    (byte)neuron.PreActivationThresholdCode,
                    (byte)neuron.ResetFunctionId,
                    (byte)neuron.ActivationFunctionId,
                    (byte)neuron.AccumulationFunctionId,
                    neuron.Exists);
            }

            var checkpointCount = (uint)((neuronSpan + Brain.AxonStride - 1) / Brain.AxonStride + 1);
            var checkpoints = NbnBinary.BuildCheckpoints(neuronRecords, Brain.AxonStride);
            var section = new NbnRegionSection(
                (byte)region.RegionId,
                (uint)neuronSpan,
                (ulong)axonRecords.Count,
                Brain.AxonStride,
                checkpointCount,
                checkpoints,
                neuronRecords,
                axonRecords.ToArray());

            sections.Add(section);
            directory[region.RegionId] = new NbnRegionDirectoryEntry((uint)neuronSpan, (ulong)axonRecords.Count, offset, 0);
            offset += (ulong)section.ByteLength;
        }

        header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            Brain.BrainSeed,
            Brain.AxonStride,
            0,
            QuantizationSchemas.DefaultNbn,
            directory);

        return true;
    }

    private static DesignerBrainViewModel BuildDesignerBrainFromNbn(NbnHeaderV2 header, IReadOnlyList<NbnRegionSection> regions, string? name)
    {
        var brain = new DesignerBrainViewModel(name ?? "Imported Brain", Guid.NewGuid(), header.BrainSeed, header.AxonStride);
        var regionMap = regions.ToDictionary(region => (int)region.RegionId, region => region);

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var regionVm = new DesignerRegionViewModel(i);
            if (regionMap.TryGetValue(i, out var section))
            {
                for (var n = 0; n < section.NeuronRecords.Length; n++)
                {
                    var record = section.NeuronRecords[n];
                    var neuronVm = new DesignerNeuronViewModel(i, n, record.Exists, i == 0 || i == NbnConstants.OutputRegionId)
                    {
                        ActivationFunctionId = record.ActivationFunctionId,
                        ResetFunctionId = record.ResetFunctionId,
                        AccumulationFunctionId = record.AccumulationFunctionId,
                        ParamACode = record.ParamACode,
                        ParamBCode = record.ParamBCode,
                        ActivationThresholdCode = record.ActivationThresholdCode,
                        PreActivationThresholdCode = record.PreActivationThresholdCode
                    };

                    regionVm.Neurons.Add(neuronVm);
                }

                var axonIndex = 0;
                for (var n = 0; n < section.NeuronRecords.Length; n++)
                {
                    var neuronVm = regionVm.Neurons[n];
                    var axonCount = section.NeuronRecords[n].AxonCount;
                    for (var a = 0; a < axonCount; a++)
                    {
                        var axonRecord = section.AxonRecords[axonIndex++];
                        neuronVm.Axons.Add(new DesignerAxonViewModel(axonRecord.TargetRegionId, axonRecord.TargetNeuronId, axonRecord.StrengthCode));
                    }

                    neuronVm.UpdateAxonCount();
                }
            }

            regionVm.UpdateCounts();
            brain.Regions.Add(regionVm);
        }

        brain.UpdateTotals();
        return brain;
    }

    private static DesignerNeuronViewModel CreateDefaultNeuron(DesignerRegionViewModel region, int neuronId)
    {
        var isRequired = region.IsInput || region.IsOutput;
        return new DesignerNeuronViewModel(region.RegionId, neuronId, true, isRequired)
        {
            ActivationFunctionId = 1,
            ResetFunctionId = 0,
            AccumulationFunctionId = 0,
            ParamACode = 0,
            ParamBCode = 0,
            ActivationThresholdCode = 0,
            PreActivationThresholdCode = 0
        };
    }

    private static void AddDefaultNeuron(DesignerRegionViewModel region)
    {
        var neuron = CreateDefaultNeuron(region, 0);
        region.Neurons.Add(neuron);
        region.UpdateCounts();
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

    private static string BuildDesignSummary(DesignerBrainViewModel brain, string? label)
    {
        var regionCount = brain.Regions.Count(region => region.NeuronCount > 0);
        var name = string.IsNullOrWhiteSpace(label) ? brain.Name : label;
        return $"Design: {name} - regions {regionCount} - neurons {brain.TotalNeurons} - axons {brain.TotalAxons} - stride {brain.AxonStride}";
    }

    private static string BuildNbsSummary(string fileName, NbsHeaderV2 header, IReadOnlyList<NbsRegionSection> regions, NbsOverlaySection? overlay)
    {
        var overlayCount = overlay?.Records.Length ?? 0;
        return $"Loaded NBS: {fileName} - regions {regions.Count} - overlay {overlayCount} - tick {header.SnapshotTickId}";
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildActivationFunctions()
    {
        var list = new List<DesignerFunctionOption>
        {
            new(0, "ACT_NONE (0)"),
            new(1, "ACT_IDENTITY (1)"),
            new(2, "ACT_STEP_UP (2)"),
            new(3, "ACT_STEP_MID (3)"),
            new(4, "ACT_STEP_DOWN (4)"),
            new(5, "ACT_ABS (5)"),
            new(6, "ACT_CLAMP (6)"),
            new(7, "ACT_RELU (7)"),
            new(8, "ACT_NRELU (8)"),
            new(9, "ACT_SIN (9)"),
            new(10, "ACT_TAN (10)"),
            new(11, "ACT_TANH (11)"),
            new(12, "ACT_ELU (12)"),
            new(13, "ACT_EXP (13)"),
            new(14, "ACT_PRELU (14)"),
            new(15, "ACT_LOG (15)"),
            new(16, "ACT_MULT (16)"),
            new(17, "ACT_ADD (17)"),
            new(18, "ACT_SIG (18)"),
            new(19, "ACT_SILU (19)"),
            new(20, "ACT_PCLAMP (20)"),
            new(21, "ACT_MODL (21)"),
            new(22, "ACT_MODR (22)"),
            new(23, "ACT_SOFTP (23)"),
            new(24, "ACT_SELU (24)"),
            new(25, "ACT_LIN (25)"),
            new(26, "ACT_LOGB (26)"),
            new(27, "ACT_POW (27)"),
            new(28, "ACT_GAUSS (28)"),
            new(29, "ACT_QUAD (29)")
        };

        for (var i = 30; i < 64; i++)
        {
            list.Add(new DesignerFunctionOption(i, $"RESERVED ({i})"));
        }

        return list;
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildResetFunctions()
    {
        var names = new Dictionary<int, string>
        {
            { 0, "RESET_ZERO" },
            { 1, "RESET_HOLD" },
            { 2, "RESET_CLAMP_POTENTIAL" },
            { 3, "RESET_CLAMP1" },
            { 4, "RESET_POTENTIAL_CLAMP_BUFFER" },
            { 5, "RESET_NEG_POTENTIAL_CLAMP_BUFFER" },
            { 6, "RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER" },
            { 7, "RESET_TENTH_POTENTIAL_CLAMP_BUFFER" },
            { 8, "RESET_HALF_POTENTIAL_CLAMP_BUFFER" },
            { 9, "RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER" },
            { 10, "RESET_FIVEX_POTENTIAL_CLAMP_BUFFER" },
            { 11, "RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER" },
            { 12, "RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER" },
            { 13, "RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER" },
            { 14, "RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER" },
            { 15, "RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER" },
            { 16, "RESET_INVERSE_POTENTIAL_CLAMP_BUFFER" },
            { 17, "RESET_POTENTIAL_CLAMP1" },
            { 18, "RESET_NEG_POTENTIAL_CLAMP1" },
            { 19, "RESET_HUNDREDTHS_POTENTIAL_CLAMP1" },
            { 20, "RESET_TENTH_POTENTIAL_CLAMP1" },
            { 21, "RESET_HALF_POTENTIAL_CLAMP1" },
            { 22, "RESET_DOUBLE_POTENTIAL_CLAMP1" },
            { 23, "RESET_FIVEX_POTENTIAL_CLAMP1" },
            { 24, "RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1" },
            { 25, "RESET_NEG_TENTH_POTENTIAL_CLAMP1" },
            { 26, "RESET_NEG_HALF_POTENTIAL_CLAMP1" },
            { 27, "RESET_NEG_DOUBLE_POTENTIAL_CLAMP1" },
            { 28, "RESET_NEG_FIVEX_POTENTIAL_CLAMP1" },
            { 29, "RESET_INVERSE_POTENTIAL_CLAMP1" },
            { 30, "RESET_POTENTIAL" },
            { 31, "RESET_NEG_POTENTIAL" },
            { 32, "RESET_HUNDREDTHS_POTENTIAL" },
            { 33, "RESET_TENTH_POTENTIAL" },
            { 34, "RESET_HALF_POTENTIAL" },
            { 35, "RESET_DOUBLE_POTENTIAL" },
            { 36, "RESET_FIVEX_POTENTIAL" },
            { 37, "RESET_NEG_HUNDREDTHS_POTENTIAL" },
            { 38, "RESET_NEG_TENTH_POTENTIAL" },
            { 39, "RESET_NEG_HALF_POTENTIAL" },
            { 40, "RESET_NEG_DOUBLE_POTENTIAL" },
            { 41, "RESET_NEG_FIVEX_POTENTIAL" },
            { 42, "RESET_INVERSE_POTENTIAL" },
            { 43, "RESET_HALF" },
            { 44, "RESET_TENTH" },
            { 45, "RESET_HUNDREDTH" },
            { 46, "RESET_NEGATIVE" },
            { 47, "RESET_NEG_HALF" },
            { 48, "RESET_NEG_TENTH" },
            { 49, "RESET_NEG_HUNDREDTH" },
            { 50, "RESET_DOUBLE_CLAMP1" },
            { 51, "RESET_FIVEX_CLAMP1" },
            { 52, "RESET_NEG_DOUBLE_CLAMP1" },
            { 53, "RESET_NEG_FIVEX_CLAMP1" },
            { 54, "RESET_DOUBLE" },
            { 55, "RESET_FIVEX" },
            { 56, "RESET_NEG_DOUBLE" },
            { 57, "RESET_NEG_FIVEX" },
            { 58, "RESET_DIVIDE_AXON_CT" },
            { 59, "RESET_INVERSE_CLAMP1" },
            { 60, "RESET_INVERSE" }
        };

        var list = new List<DesignerFunctionOption>();
        for (var i = 0; i < 64; i++)
        {
            if (names.TryGetValue(i, out var name))
            {
                list.Add(new DesignerFunctionOption(i, $"{name} ({i})"));
            }
            else
            {
                list.Add(new DesignerFunctionOption(i, $"RESERVED ({i})"));
            }
        }

        return list;
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildAccumulationFunctions()
    {
        return new List<DesignerFunctionOption>
        {
            new(0, "ACCUM_SUM (0)"),
            new(1, "ACCUM_PRODUCT (1)"),
            new(2, "ACCUM_MAX (2)"),
            new(3, "ACCUM_NONE (3)")
        };
    }

    private static ulong GenerateSeed()
    {
        Span<byte> buffer = stackalloc byte[8];
        RandomNumberGenerator.Fill(buffer);
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
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

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private static int Clamp(int value, int min, int max)
        => value < min ? min : value > max ? max : value;

    private static double Clamp(double value, double min, double max)
        => value < min ? min : value > max ? max : value;
}

public enum DesignerDocumentType
{
    None,
    Nbn,
    Nbs
}
