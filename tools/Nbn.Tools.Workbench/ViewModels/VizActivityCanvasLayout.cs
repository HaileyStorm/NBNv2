using System;
using System.Collections.Generic;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

/// <summary>
/// Represents a render-ready activity canvas snapshot.
/// </summary>
public sealed record VizActivityCanvasLayout(
    double Width,
    double Height,
    string Legend,
    IReadOnlyList<VizActivityCanvasNode> Nodes,
    IReadOnlyList<VizActivityCanvasEdge> Edges);

/// <summary>
/// Tracks selection, hover, and pin state applied to canvas nodes and routes.
/// </summary>
public sealed record VizActivityCanvasInteractionState(
    string? SelectedNodeKey,
    string? SelectedRouteLabel,
    string? HoverNodeKey,
    string? HoverRouteLabel,
    IReadOnlySet<string> PinnedNodeKeys,
    IReadOnlySet<string> PinnedRouteLabels)
{
    /// <summary>
    /// Gets the empty interaction state.
    /// </summary>
    public static VizActivityCanvasInteractionState Empty { get; } = new(
        null,
        null,
        null,
        null,
        new HashSet<string>(StringComparer.OrdinalIgnoreCase),
        new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    /// <summary>
    /// Determines whether the node key is currently selected.
    /// </summary>
    public bool IsSelectedNode(string? nodeKey) => KeyEquals(SelectedNodeKey, nodeKey);

    /// <summary>
    /// Determines whether the node key is currently hovered.
    /// </summary>
    public bool IsHoveredNode(string? nodeKey) => KeyEquals(HoverNodeKey, nodeKey);

    /// <summary>
    /// Determines whether the route label is currently selected.
    /// </summary>
    public bool IsSelectedRoute(string? routeLabel) => KeyEquals(SelectedRouteLabel, routeLabel);

    /// <summary>
    /// Determines whether the route label is currently hovered.
    /// </summary>
    public bool IsHoveredRoute(string? routeLabel) => KeyEquals(HoverRouteLabel, routeLabel);

    /// <summary>
    /// Determines whether the node key is pinned.
    /// </summary>
    public bool IsNodePinned(string? nodeKey)
    {
        if (string.IsNullOrWhiteSpace(nodeKey))
        {
            return false;
        }

        foreach (var pinned in PinnedNodeKeys)
        {
            if (KeyEquals(pinned, nodeKey))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Determines whether the route label is pinned.
    /// </summary>
    public bool IsRoutePinned(string? routeLabel)
    {
        if (string.IsNullOrWhiteSpace(routeLabel))
        {
            return false;
        }

        foreach (var pinned in PinnedRouteLabels)
        {
            if (KeyEquals(pinned, routeLabel))
            {
                return true;
            }
        }

        return false;
    }

    private static bool KeyEquals(string? left, string? right)
        => string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Identifies a route between two regions for canvas topology hints.
/// </summary>
public readonly record struct VizActivityCanvasRegionRoute(uint SourceRegionId, uint TargetRegionId);

/// <summary>
/// Identifies a route between two neuron addresses for focused topology hints.
/// </summary>
public readonly record struct VizActivityCanvasNeuronRoute(uint SourceAddress, uint TargetAddress);

/// <summary>
/// Provides optional topology hints used to render inactive or focus-only nodes and routes.
/// </summary>
public sealed record VizActivityCanvasTopology(
    IReadOnlySet<uint> Regions,
    IReadOnlySet<VizActivityCanvasRegionRoute> RegionRoutes,
    IReadOnlySet<uint> NeuronAddresses,
    IReadOnlySet<VizActivityCanvasNeuronRoute> NeuronRoutes)
{
    /// <summary>
    /// Gets an empty topology snapshot.
    /// </summary>
    public static VizActivityCanvasTopology Empty { get; } = new(
        new HashSet<uint>(),
        new HashSet<VizActivityCanvasRegionRoute>(),
        new HashSet<uint>(),
        new HashSet<VizActivityCanvasNeuronRoute>());
}

/// <summary>
/// Represents a render-ready canvas node.
/// </summary>
public sealed record VizActivityCanvasNode(
    string NodeKey,
    uint RegionId,
    uint? NeuronId,
    uint NavigateRegionId,
    string Label,
    string Detail,
    double Left,
    double Top,
    double Diameter,
    string Fill,
    string Stroke,
    double FillOpacity,
    double PulseOpacity,
    double StrokeThickness,
    bool IsFocused,
    ulong LastTick,
    int EventCount,
    bool IsSelected,
    bool IsHovered,
    bool IsPinned);

/// <summary>
/// Represents a render-ready canvas edge.
/// </summary>
public sealed record VizActivityCanvasEdge(
    string RouteLabel,
    string Detail,
    string PathData,
    double SourceX,
    double SourceY,
    double ControlX,
    double ControlY,
    double TargetX,
    double TargetY,
    string Stroke,
    string DirectionDashArray,
    string ActivityStroke,
    double StrokeThickness,
    double ActivityStrokeThickness,
    double HitTestThickness,
    double Opacity,
    double ActivityOpacity,
    bool IsFocused,
    ulong LastTick,
    int EventCount,
    uint? SourceRegionId,
    uint? TargetRegionId,
    bool IsSelected,
    bool IsHovered,
    bool IsPinned);

/// <summary>
/// Selects the value family used to color the activity canvas.
/// </summary>
public enum VizActivityCanvasColorMode
{
    StateValue = 0,
    Activity = 1,
    Topology = 2,
    EnergyReserve = 3,
    EnergyCostPressure = 4
}

/// <summary>
/// Selects the transfer curve applied before color encoding.
/// </summary>
public enum VizActivityCanvasTransferCurve
{
    Linear = 0,
    PerceptualLog = 1
}

/// <summary>
/// Selects the spatial layout mode used for region placement.
/// </summary>
public enum VizActivityCanvasLayoutMode
{
    Axial2D = 0,
    Axial3DExperimental = 1
}

/// <summary>
/// Configures route level-of-detail behavior for focused canvases.
/// </summary>
public sealed record VizActivityCanvasLodOptions(
    bool Enabled,
    int LowZoomRouteBudget,
    int MediumZoomRouteBudget,
    int HighZoomRouteBudget);

/// <summary>
/// Configures canvas layout mode, scale, route LOD, and color transfer behavior.
/// </summary>
public sealed record VizActivityCanvasRenderOptions(
    VizActivityCanvasLayoutMode LayoutMode,
    double ViewportScale,
    VizActivityCanvasLodOptions Lod,
    VizActivityCanvasTransferCurve ColorCurve = VizActivityCanvasTransferCurve.Linear)
{
    /// <summary>
    /// Gets the default canvas render options.
    /// </summary>
    public static VizActivityCanvasRenderOptions Default { get; } = new(
        VizActivityCanvasLayoutMode.Axial2D,
        1.0,
        new VizActivityCanvasLodOptions(
            Enabled: false,
            LowZoomRouteBudget: 160,
            MediumZoomRouteBudget: 280,
            HighZoomRouteBudget: 420),
        VizActivityCanvasTransferCurve.Linear);
}

/// <summary>
/// Builds render-ready activity canvas snapshots from projection and topology data.
/// </summary>
public static partial class VizActivityCanvasLayoutBuilder
{
    /// <summary>
    /// Gets the baseline canvas width used by the visualizer.
    /// </summary>
    public const double CanvasWidth = 860;

    /// <summary>
    /// Gets the baseline canvas height used by the visualizer.
    /// </summary>
    public const double CanvasHeight = 420;

    private const double CenterX = CanvasWidth / 2.0;
    private const double CenterY = CanvasHeight / 2.0;
    private const double CanvasPadding = 26;
    private const double MinRegionNodeRadius = 16;
    private const double MaxRegionNodeRadius = 29;
    private const double MinFocusNeuronNodeRadius = 11;
    private const double MaxFocusNeuronNodeRadius = 19;
    private const double MinGatewayNodeRadius = 11;
    private const double MaxGatewayNodeRadius = 16;
    private const double RegionNodePositionPadding = CanvasPadding + MaxRegionNodeRadius + 4;
    private const double EdgeControlPadding = CanvasPadding;
    private const double EdgeNodeClearance = 4;
    private const double BaseEdgeStroke = 1.1;
    private const double MaxEdgeStrokeBoost = 2.8;
    private const double PerceptualLogGain = 63.0;
    private const int MinAdaptiveRouteBudget = 16;
    private const int MaxAdaptiveRouteBudget = 4096;
    private const int EdgeCurveCacheMaxEntries = 4096;
    private static readonly object EdgeCurveCacheGate = new();
    private static readonly Dictionary<CanvasEdgeCurveKey, CanvasEdgeCurve> EdgeCurveCache = new();
    private static readonly Queue<CanvasEdgeCurveKey> EdgeCurveCacheOrder = new();
    private static readonly double OverlayAvoidUpperRightCenterRadians = -Math.PI / 4.0;
    private static readonly double OverlayAvoidUpperRightSpreadRadians = Math.PI / 3.1;
    private static readonly double OverlayAvoidUpperRightTransitionRadians = Math.PI / 18.0;
    private static readonly double OverlayAvoidUpperRightMaxShiftRadians = Math.PI / 48.0;

    /// <summary>
    /// Builds either a region canvas or a focused-neuron canvas for the supplied projection.
    /// </summary>
    public static VizActivityCanvasLayout Build(
        VizActivityProjection projection,
        VizActivityProjectionOptions options,
        VizActivityCanvasInteractionState? interaction = null,
        VizActivityCanvasTopology? topology = null,
        VizActivityCanvasColorMode colorMode = VizActivityCanvasColorMode.StateValue,
        VizActivityCanvasRenderOptions? renderOptions = null)
    {
        interaction ??= VizActivityCanvasInteractionState.Empty;
        topology ??= VizActivityCanvasTopology.Empty;
        renderOptions ??= VizActivityCanvasRenderOptions.Default;

        var latestTick = ResolveLatestTick(projection);
        if (options.FocusRegionId is uint focusRegionId)
        {
            return BuildFocusedNeuronCanvas(
                projection,
                options,
                topology,
                interaction,
                latestTick,
                focusRegionId,
                colorMode,
                renderOptions);
        }

        return BuildRegionCanvas(
            projection,
            options,
            topology,
            interaction,
            latestTick,
            colorMode,
            renderOptions);
    }
}
