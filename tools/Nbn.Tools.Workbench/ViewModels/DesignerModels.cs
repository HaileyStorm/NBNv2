using System;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Avalonia;
using Avalonia.Media;
using Avalonia.Media.Immutable;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class DesignerBrainViewModel : ViewModelBase
{
    private string _name;
    private Guid _brainId;
    private string _brainIdText;
    private ulong _brainSeed;
    private string _brainSeedText;
    private uint _axonStride;
    private string _axonStrideText;
    private int _totalNeurons;
    private int _totalAxons;

    public DesignerBrainViewModel(string name, Guid brainId, ulong brainSeed, uint axonStride)
    {
        _name = name;
        _brainId = brainId;
        _brainIdText = brainId.ToString();
        _brainSeed = brainSeed;
        _brainSeedText = brainSeed.ToString();
        _axonStride = axonStride;
        _axonStrideText = axonStride.ToString();
        Regions = new ObservableCollection<DesignerRegionViewModel>();
    }

    public ObservableCollection<DesignerRegionViewModel> Regions { get; }

    public string Name
    {
        get => _name;
        set => SetProperty(ref _name, value);
    }

    public Guid BrainId
    {
        get => _brainId;
        private set => SetProperty(ref _brainId, value);
    }

    public string BrainIdText
    {
        get => _brainIdText;
        set
        {
            if (SetProperty(ref _brainIdText, value) && Guid.TryParse(value, out var parsed))
            {
                BrainId = parsed;
            }
        }
    }

    public ulong BrainSeed
    {
        get => _brainSeed;
        private set => SetProperty(ref _brainSeed, value);
    }

    public string BrainSeedText
    {
        get => _brainSeedText;
        set
        {
            if (SetProperty(ref _brainSeedText, value) && ulong.TryParse(value, out var parsed))
            {
                BrainSeed = parsed;
            }
        }
    }

    public uint AxonStride
    {
        get => _axonStride;
        private set => SetProperty(ref _axonStride, value);
    }

    public string AxonStrideText
    {
        get => _axonStrideText;
        set
        {
            if (SetProperty(ref _axonStrideText, value) && uint.TryParse(value, out var parsed))
            {
                AxonStride = parsed;
            }
        }
    }

    public int TotalNeurons
    {
        get => _totalNeurons;
        private set => SetProperty(ref _totalNeurons, value);
    }

    public int TotalAxons
    {
        get => _totalAxons;
        private set => SetProperty(ref _totalAxons, value);
    }

    public void SetSeed(ulong seed)
    {
        BrainSeed = seed;
        BrainSeedText = seed.ToString();
    }

    public void SetBrainId(Guid brainId)
    {
        BrainId = brainId;
        BrainIdText = brainId.ToString();
    }

    public void SetStride(uint stride)
    {
        AxonStride = stride;
        AxonStrideText = stride.ToString();
    }

    public void UpdateTotals()
    {
        TotalNeurons = Regions.Sum(region => region.NeuronCount);
        TotalAxons = Regions.Sum(region => region.AxonCount);
    }
}

public sealed class DesignerRegionViewModel : ViewModelBase
{
    private bool _isSelected;
    private int _neuronCount;
    private int _axonCount;

    public DesignerRegionViewModel(int regionId)
    {
        RegionId = regionId;
        Label = regionId switch
        {
            0 => "Region 0 (Input)",
            31 => "Region 31 (Output)",
            _ => $"Region {regionId}"
        };
        Neurons = new ObservableCollection<DesignerNeuronViewModel>();
    }

    public int RegionId { get; }
    public string Label { get; }
    public ObservableCollection<DesignerNeuronViewModel> Neurons { get; }

    public int NeuronCount
    {
        get => _neuronCount;
        private set => SetProperty(ref _neuronCount, value);
    }

    public int AxonCount
    {
        get => _axonCount;
        private set => SetProperty(ref _axonCount, value);
    }

    public bool IsSelected
    {
        get => _isSelected;
        set
        {
            if (SetProperty(ref _isSelected, value))
            {
                OnPropertyChanged(nameof(SelectionBackground));
                OnPropertyChanged(nameof(SelectionForeground));
                OnPropertyChanged(nameof(SelectionBorder));
            }
        }
    }

    public bool IsInput => RegionId == 0;
    public bool IsOutput => RegionId == 31;
    public bool HasNeurons => NeuronCount > 0;

    public string Detail => HasNeurons
        ? $"{NeuronCount} neurons, {AxonCount} axons"
        : "Empty";

    public IBrush SelectionBackground => IsSelected
        ? DesignerBrushes.Accent
        : DesignerBrushes.SurfaceAlt;

    public IBrush SelectionForeground => IsSelected
        ? DesignerBrushes.OnAccent
        : DesignerBrushes.Ink;

    public IBrush SelectionBorder => IsSelected
        ? DesignerBrushes.Accent
        : DesignerBrushes.Border;

    public double TileOpacity => HasNeurons ? 1 : 0.75;

    public void UpdateCounts()
    {
        NeuronCount = Neurons.Count;
        AxonCount = Neurons.Sum(neuron => neuron.AxonCount);
        OnPropertyChanged(nameof(HasNeurons));
        OnPropertyChanged(nameof(Detail));
        OnPropertyChanged(nameof(TileOpacity));
    }
}

public sealed class DesignerNeuronViewModel : ViewModelBase
{
    private bool _isSelected;
    private bool _isPendingSource;
    private bool _isHovered;
    private bool _exists;
    private int _activationFunctionId;
    private int _resetFunctionId;
    private int _accumulationFunctionId;
    private int _paramACode;
    private int _paramBCode;
    private int _activationThresholdCode;
    private int _preActivationThresholdCode;
    private int _axonCount;
    private double _canvasX;
    private double _canvasY;

    public DesignerNeuronViewModel(int regionId, int neuronId, bool exists, bool isRequired)
    {
        RegionId = regionId;
        NeuronId = neuronId;
        _exists = exists;
        IsRequired = isRequired;
        Axons = new ObservableCollection<DesignerAxonViewModel>();
    }

    public int RegionId { get; }
    public int NeuronId { get; }
    public bool IsRequired { get; }
    public ObservableCollection<DesignerAxonViewModel> Axons { get; }

    public bool Exists
    {
        get => _exists;
        set
        {
            if (IsRequired && !value)
            {
                return;
            }

            if (SetProperty(ref _exists, value))
            {
                OnPropertyChanged(nameof(TileBackground));
                OnPropertyChanged(nameof(TileForeground));
                OnPropertyChanged(nameof(TileBorder));
                OnPropertyChanged(nameof(TileOpacity));
            }
        }
    }

    public int ActivationFunctionId
    {
        get => _activationFunctionId;
        set => SetProperty(ref _activationFunctionId, Clamp(value, 0, 29));
    }

    public int ResetFunctionId
    {
        get => _resetFunctionId;
        set => SetProperty(ref _resetFunctionId, Clamp(value, 0, 60));
    }

    public int AccumulationFunctionId
    {
        get => _accumulationFunctionId;
        set => SetProperty(ref _accumulationFunctionId, Clamp(value, 0, 3));
    }

    public int ParamACode
    {
        get => _paramACode;
        set => SetProperty(ref _paramACode, Clamp(value, 0, 63));
    }

    public int ParamBCode
    {
        get => _paramBCode;
        set => SetProperty(ref _paramBCode, Clamp(value, 0, 63));
    }

    public int ActivationThresholdCode
    {
        get => _activationThresholdCode;
        set => SetProperty(ref _activationThresholdCode, Clamp(value, 0, 63));
    }

    public int PreActivationThresholdCode
    {
        get => _preActivationThresholdCode;
        set => SetProperty(ref _preActivationThresholdCode, Clamp(value, 0, 63));
    }

    public int AxonCount
    {
        get => _axonCount;
        private set => SetProperty(ref _axonCount, value);
    }

    public bool IsSelected
    {
        get => _isSelected;
        set
        {
            if (SetProperty(ref _isSelected, value))
            {
                OnPropertyChanged(nameof(TileBackground));
                OnPropertyChanged(nameof(TileForeground));
                OnPropertyChanged(nameof(TileBorder));
            }
        }
    }

    public bool IsPendingSource
    {
        get => _isPendingSource;
        set
        {
            if (SetProperty(ref _isPendingSource, value))
            {
                OnPropertyChanged(nameof(TileBackground));
                OnPropertyChanged(nameof(TileForeground));
                OnPropertyChanged(nameof(TileBorder));
            }
        }
    }

    public bool IsHovered
    {
        get => _isHovered;
        set
        {
            if (SetProperty(ref _isHovered, value))
            {
                OnPropertyChanged(nameof(TileBackground));
                OnPropertyChanged(nameof(TileForeground));
                OnPropertyChanged(nameof(TileBorder));
            }
        }
    }

    public double CanvasX
    {
        get => _canvasX;
        set => SetProperty(ref _canvasX, value);
    }

    public double CanvasY
    {
        get => _canvasY;
        set => SetProperty(ref _canvasY, value);
    }

    public string TileLabel => $"N{NeuronId}";

    public string TileDetail => AxonCount > 0 ? $"{AxonCount} axons" : "No axons";

    public IBrush TileBackground
    {
        get
        {
            if (!Exists)
            {
                return DesignerBrushes.Disabled;
            }

            if (IsPendingSource)
            {
                return DesignerBrushes.Teal;
            }

            if (IsSelected)
            {
                return DesignerBrushes.Accent;
            }

            return IsHovered ? DesignerBrushes.SurfaceAlt : DesignerBrushes.Surface;
        }
    }

    public IBrush TileForeground
    {
        get
        {
            if (!Exists)
            {
                return DesignerBrushes.Muted;
            }

            return IsSelected || IsPendingSource ? DesignerBrushes.OnAccent : DesignerBrushes.Ink;
        }
    }

    public IBrush TileBorder
    {
        get
        {
            if (IsSelected || IsPendingSource)
            {
                return DesignerBrushes.Accent;
            }

            return IsHovered ? DesignerBrushes.Teal : DesignerBrushes.Border;
        }
    }

    public double TileOpacity => Exists ? 1 : 0.6;

    public void UpdateAxonCount()
    {
        AxonCount = Axons.Count;
        OnPropertyChanged(nameof(TileDetail));
    }

    private static int Clamp(int value, int min, int max)
        => value < min ? min : value > max ? max : value;
}

public sealed class DesignerAxonViewModel : ViewModelBase
{
    private bool _isSelected;
    private int _strengthCode;

    public DesignerAxonViewModel(int targetRegionId, int targetNeuronId, int strengthCode)
    {
        TargetRegionId = targetRegionId;
        TargetNeuronId = targetNeuronId;
        _strengthCode = strengthCode;
    }

    public int TargetRegionId { get; }
    public int TargetNeuronId { get; }

    public int StrengthCode
    {
        get => _strengthCode;
        set
        {
            if (SetProperty(ref _strengthCode, Clamp(value, 0, 31)))
            {
                OnPropertyChanged(nameof(StrengthLabel));
            }
        }
    }

    public bool IsSelected
    {
        get => _isSelected;
        set
        {
            if (SetProperty(ref _isSelected, value))
            {
                OnPropertyChanged(nameof(RowBackground));
                OnPropertyChanged(nameof(RowForeground));
            }
        }
    }

    public string TargetLabel => $"R{TargetRegionId} N{TargetNeuronId}";
    public string StrengthLabel => $"S{StrengthCode}";

    public IBrush RowBackground => IsSelected ? DesignerBrushes.Accent : DesignerBrushes.SurfaceAlt;
    public IBrush RowForeground => IsSelected ? DesignerBrushes.OnAccent : DesignerBrushes.Ink;

    private static int Clamp(int value, int min, int max)
        => value < min ? min : value > max ? max : value;
}

public sealed class DesignerEdgeViewModel : ViewModelBase
{
    private const double InternalBundleBase = 12;
    private const double ExternalBundleBase = 18;
    private const double BundleSpread = 8;
    private const double MinVectorLength = 0.001;
    private const double ArrowLengthBase = 8;
    private const double ArrowWidthBase = 4.5;
    private Point _start;
    private Point _end;
    private bool _isPreview;
    private bool _isSelected;
    private DesignerEdgeKind _kind;
    private IBrush _stroke;
    private double _thickness;
    private string? _label;
    private Point _labelPosition;
    private Geometry _pathGeometry;
    private Geometry _arrowHeadGeometry;
    private bool _showArrowHead;
    private readonly int _bundleIndex;
    private readonly int _bundleCount;
    private readonly int? _navigationRegionId;
    private readonly int? _navigationNeuronId;

    public DesignerEdgeViewModel(
        Point start,
        Point end,
        bool isPreview,
        bool isSelected,
        DesignerEdgeKind kind = DesignerEdgeKind.OutboundInternal,
        string? label = null,
        Point? labelPosition = null,
        int bundleIndex = 0,
        int bundleCount = 1,
        int? navigationRegionId = null,
        int? navigationNeuronId = null)
    {
        _start = start;
        _end = end;
        _isPreview = isPreview;
        _isSelected = isSelected;
        _kind = kind;
        _label = label;
        _labelPosition = labelPosition ?? default;
        _stroke = DesignerBrushes.Border;
        _thickness = 1;
        _pathGeometry = Geometry.Parse("M 0 0");
        _arrowHeadGeometry = Geometry.Parse("M 0 0");
        _bundleIndex = Math.Max(0, bundleIndex);
        _bundleCount = Math.Max(1, bundleCount);
        _navigationRegionId = navigationRegionId;
        _navigationNeuronId = navigationNeuronId;
        UpdateAppearance();
    }

    public Point Start
    {
        get => _start;
        set
        {
            if (SetProperty(ref _start, value))
            {
                UpdateGeometry();
            }
        }
    }

    public Point End
    {
        get => _end;
        set
        {
            if (SetProperty(ref _end, value))
            {
                UpdateGeometry();
            }
        }
    }

    public bool IsPreview
    {
        get => _isPreview;
        set
        {
            if (SetProperty(ref _isPreview, value))
            {
                UpdateAppearance();
            }
        }
    }

    public bool IsSelected
    {
        get => _isSelected;
        set
        {
            if (SetProperty(ref _isSelected, value))
            {
                UpdateAppearance();
            }
        }
    }

    public DesignerEdgeKind Kind
    {
        get => _kind;
        set
        {
            if (SetProperty(ref _kind, value))
            {
                UpdateAppearance();
            }
        }
    }

    public IBrush Stroke
    {
        get => _stroke;
        private set => SetProperty(ref _stroke, value);
    }

    public double Thickness
    {
        get => _thickness;
        private set => SetProperty(ref _thickness, value);
    }

    public string? Label
    {
        get => _label;
        set
        {
            if (SetProperty(ref _label, value))
            {
                OnPropertyChanged(nameof(HasLabel));
            }
        }
    }

    public bool HasLabel => !string.IsNullOrWhiteSpace(_label);

    public Point LabelPosition
    {
        get => _labelPosition;
        set => SetProperty(ref _labelPosition, value);
    }

    public Geometry PathGeometry
    {
        get => _pathGeometry;
        private set => SetProperty(ref _pathGeometry, value);
    }

    public Geometry ArrowHeadGeometry
    {
        get => _arrowHeadGeometry;
        private set => SetProperty(ref _arrowHeadGeometry, value);
    }

    public bool ShowArrowHead
    {
        get => _showArrowHead;
        private set => SetProperty(ref _showArrowHead, value);
    }

    public bool CanNavigate => _navigationRegionId.HasValue && _navigationNeuronId.HasValue;

    public int? NavigationRegionId => _navigationRegionId;

    public int? NavigationNeuronId => _navigationNeuronId;

    public string NavigationHint => CanNavigate
        ? $"Focus R{_navigationRegionId} N{_navigationNeuronId}"
        : string.Empty;

    private void UpdateAppearance()
    {
        if (_isPreview)
        {
            Stroke = DesignerBrushes.Teal;
            Thickness = 2;
        }
        else if (_isSelected)
        {
            Stroke = DesignerBrushes.Accent;
            Thickness = 2;
        }
        else
        {
            switch (_kind)
            {
                case DesignerEdgeKind.OutboundInternal:
                    Stroke = DesignerBrushes.Accent;
                    Thickness = 1.8;
                    break;
                case DesignerEdgeKind.InboundInternal:
                    Stroke = DesignerBrushes.Teal;
                    Thickness = 1.6;
                    break;
                case DesignerEdgeKind.OutboundExternal:
                    Stroke = DesignerBrushes.Gold;
                    Thickness = 1.4;
                    break;
                case DesignerEdgeKind.InboundExternal:
                    Stroke = DesignerBrushes.Muted;
                    Thickness = 1.3;
                    break;
                default:
                    Stroke = DesignerBrushes.Border;
                    Thickness = 1;
                    break;
            }
        }

        UpdateGeometry();
    }

    private void UpdateGeometry()
    {
        var dx = _end.X - _start.X;
        var dy = _end.Y - _start.Y;
        var length = Math.Sqrt((dx * dx) + (dy * dy));
        var safeDx = length < MinVectorLength ? 1.0 : dx;
        var safeDy = length < MinVectorLength ? 0.0 : dy;
        var safeLength = length < MinVectorLength ? 1.0 : length;

        var unitPerpX = -safeDy / safeLength;
        var unitPerpY = safeDx / safeLength;

        var midpoint = new Point((_start.X + _end.X) / 2.0, (_start.Y + _end.Y) / 2.0);
        var bundleOffset = ComputeBundleOffset();
        var control = new Point(
            midpoint.X + (unitPerpX * bundleOffset),
            midpoint.Y + (unitPerpY * bundleOffset));

        var pathData = $"M {FormatPoint(_start)} Q {FormatPoint(control)} {FormatPoint(_end)}";
        PathGeometry = Geometry.Parse(pathData);

        var tangentX = _end.X - control.X;
        var tangentY = _end.Y - control.Y;
        var tangentLength = Math.Sqrt((tangentX * tangentX) + (tangentY * tangentY));
        if (tangentLength < MinVectorLength)
        {
            tangentX = safeDx;
            tangentY = safeDy;
            tangentLength = safeLength;
        }

        if (length < 4)
        {
            ArrowHeadGeometry = Geometry.Parse("M 0 0");
            ShowArrowHead = false;
            return;
        }

        var unitX = tangentX / tangentLength;
        var unitY = tangentY / tangentLength;
        var arrowLength = ArrowLengthBase + Math.Min(4, Thickness * 1.5);
        var arrowWidth = ArrowWidthBase + Math.Min(2.5, Thickness * 0.8);

        var tip = _end;
        var back = new Point(tip.X - (unitX * arrowLength), tip.Y - (unitY * arrowLength));
        var left = new Point(back.X - (unitY * arrowWidth), back.Y + (unitX * arrowWidth));
        var right = new Point(back.X + (unitY * arrowWidth), back.Y - (unitX * arrowWidth));

        var arrowData = $"M {FormatPoint(left)} L {FormatPoint(tip)} L {FormatPoint(right)}";
        ArrowHeadGeometry = Geometry.Parse(arrowData);
        ShowArrowHead = true;
    }

    private double ComputeBundleOffset()
    {
        var baseOffset = _kind is DesignerEdgeKind.OutboundExternal or DesignerEdgeKind.InboundExternal
            ? ExternalBundleBase
            : InternalBundleBase;
        var orientedBase = _kind is DesignerEdgeKind.InboundInternal or DesignerEdgeKind.InboundExternal
            ? -baseOffset
            : baseOffset;
        var centeredIndex = _bundleIndex - ((_bundleCount - 1) / 2.0);
        var spread = centeredIndex * BundleSpread;

        var offset = orientedBase + spread;
        if (_isPreview)
        {
            offset *= 1.15;
        }
        else if (_isSelected)
        {
            offset *= 1.1;
        }

        return offset;
    }

    private static string FormatPoint(Point point)
        => $"{FormatDouble(point.X)} {FormatDouble(point.Y)}";

    private static string FormatDouble(double value)
        => value.ToString("0.###", CultureInfo.InvariantCulture);
}

public enum DesignerEdgeKind
{
    OutboundInternal,
    InboundInternal,
    OutboundExternal,
    InboundExternal
}

public sealed class DesignerFunctionOption
{
    public DesignerFunctionOption(int id, string label, string description = "", bool usesParamA = false, bool usesParamB = false)
    {
        Id = id;
        Label = label;
        Description = description;
        UsesParamA = usesParamA;
        UsesParamB = usesParamB;
    }

    public int Id { get; }
    public string Label { get; }
    public string Description { get; }
    public bool UsesParamA { get; }
    public bool UsesParamB { get; }

    public override string ToString() => Label;
}

internal static class DesignerBrushes
{
    public static readonly IBrush Surface = new ImmutableSolidColorBrush(Color.Parse("#FFFFFF"));
    public static readonly IBrush SurfaceAlt = new ImmutableSolidColorBrush(Color.Parse("#EDE7D7"));
    public static readonly IBrush Ink = new ImmutableSolidColorBrush(Color.Parse("#101B22"));
    public static readonly IBrush Muted = new ImmutableSolidColorBrush(Color.Parse("#6F7D86"));
    public static readonly IBrush Border = new ImmutableSolidColorBrush(Color.Parse("#D3C9B6"));
    public static readonly IBrush Accent = new ImmutableSolidColorBrush(Color.Parse("#F16D3A"));
    public static readonly IBrush Teal = new ImmutableSolidColorBrush(Color.Parse("#2F9C8A"));
    public static readonly IBrush Gold = new ImmutableSolidColorBrush(Color.Parse("#E2B548"));
    public static readonly IBrush Disabled = new ImmutableSolidColorBrush(Color.Parse("#F0EBDD"));
    public static readonly IBrush OnAccent = new ImmutableSolidColorBrush(Color.Parse("#FFFFFF"));
}
