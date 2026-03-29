using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Nbn.Tools.Workbench.ViewModels;

/// <summary>
/// Represents a species row in the current population summary.
/// </summary>
public sealed record SpeciationSpeciesCountItem(
    string SpeciesId,
    string SpeciesDisplayName,
    int Count,
    string PercentLabel,
    string BarLabel);

/// <summary>
/// Represents a summary row for a speciation epoch.
/// </summary>
public sealed record SpeciationEpochSummaryItem(
    long EpochId,
    int MembershipCount,
    int SpeciesCount,
    string FirstAssigned,
    string LastAssigned);

/// <summary>
/// Represents a selectable epoch in the speciation pane.
/// </summary>
public sealed record SpeciationEpochOptionItem(
    long EpochId,
    string Label,
    bool IsActive);

/// <summary>
/// Represents a rendered chart series for a species.
/// </summary>
public sealed record SpeciationLineChartSeriesItem(
    string SpeciesId,
    string Label,
    string Stroke,
    string PathData,
    string LatestCountLabel);

/// <summary>
/// Represents a filled area in the speciation flow chart.
/// </summary>
public sealed record SpeciationFlowChartAreaItem(
    string SpeciesId,
    string Label,
    string Fill,
    string Stroke,
    string PathData,
    string LatestShareLabel,
    IReadOnlyList<SpeciationFlowChartSampleItem> Samples);

/// <summary>
/// Represents a hover sample along a flow-chart area.
/// </summary>
public sealed record SpeciationFlowChartSampleItem(
    string RowLabel,
    int PopulationCount,
    int TotalCount,
    double Share,
    double CenterY,
    IReadOnlyList<SpeciationFlowChartSampleBand> Bands);

/// <summary>
/// Represents a single horizontal band in a flow-chart sample.
/// </summary>
public sealed record SpeciationFlowChartSampleBand(
    double StartX,
    double EndX);

/// <summary>
/// Represents a legend entry shared by the speciation charts.
/// </summary>
public sealed record SpeciationChartLegendItem(
    string SpeciesId,
    string Label,
    string SwatchColor,
    double SwatchHeight,
    string ValueLabel,
    bool IsColorEditable);

/// <summary>
/// Represents a selectable color swatch for species overrides.
/// </summary>
public sealed record SpeciationColorPickerSwatchItem(string ColorHex);

/// <summary>
/// Represents a node in the speciation cladogram tree.
/// </summary>
public sealed class SpeciationCladogramItem : ViewModelBase
{
    private bool _isExpanded;

    /// <summary>
    /// Initializes a cladogram node.
    /// </summary>
    public SpeciationCladogramItem(
        string speciesId,
        string speciesDisplayName,
        string detailLabel,
        string color,
        bool isRoot,
        IReadOnlyList<SpeciationCladogramItem>? children = null,
        bool isExpanded = true)
    {
        SpeciesId = speciesId;
        SpeciesDisplayName = speciesDisplayName;
        DetailLabel = detailLabel;
        Color = color;
        IsRoot = isRoot;
        Children = new ObservableCollection<SpeciationCladogramItem>(children ?? Array.Empty<SpeciationCladogramItem>());
        _isExpanded = isExpanded;
    }

    /// <summary>
    /// Gets the stable species identifier.
    /// </summary>
    public string SpeciesId { get; }

    /// <summary>
    /// Gets the display name shown in the UI.
    /// </summary>
    public string SpeciesDisplayName { get; }

    /// <summary>
    /// Gets the supporting detail text shown with the node.
    /// </summary>
    public string DetailLabel { get; }

    /// <summary>
    /// Gets the UI color assigned to the node.
    /// </summary>
    public string Color { get; }

    /// <summary>
    /// Gets whether the node is a root lineage.
    /// </summary>
    public bool IsRoot { get; }

    /// <summary>
    /// Gets the lineage role label used by the UI.
    /// </summary>
    public string RoleLabel => IsRoot ? "Root lineage" : "Derived lineage";

    /// <summary>
    /// Gets the full line text rendered in simple list contexts.
    /// </summary>
    public string LineText => $"{SpeciesDisplayName} [{SpeciesId}]";

    /// <summary>
    /// Gets the child lineage nodes.
    /// </summary>
    public ObservableCollection<SpeciationCladogramItem> Children { get; }

    /// <summary>
    /// Gets whether the node has any children.
    /// </summary>
    public bool HasChildren => Children.Count > 0;

    /// <summary>
    /// Gets whether the node has no children.
    /// </summary>
    public bool IsLeaf => Children.Count == 0;

    /// <summary>
    /// Gets or sets whether the node is expanded in the UI.
    /// </summary>
    public bool IsExpanded
    {
        get => _isExpanded;
        set => SetProperty(ref _isExpanded, value);
    }
}

/// <summary>
/// Represents an active-brain option for the simulator controls.
/// </summary>
public sealed record SpeciationSimulatorBrainOption(Guid BrainId, string Label)
{
    /// <summary>
    /// Gets the display label for the backing brain identifier.
    /// </summary>
    public string BrainIdLabel => BrainId.ToString("D");
}

/// <summary>
/// Represents a seed-parent selection in the simulator controls.
/// </summary>
public sealed record SpeciationSimulatorSeedParentItem(Guid BrainId, string Label, string Source)
{
    /// <summary>
    /// Gets the display label for the backing brain identifier.
    /// </summary>
    public string BrainIdLabel => BrainId.ToString("D");
}
