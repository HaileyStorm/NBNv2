using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public sealed class VizActivityCanvasSurface : Control
{
    public static readonly StyledProperty<IReadOnlyList<VizActivityCanvasNode>?> NodesProperty =
        AvaloniaProperty.Register<VizActivityCanvasSurface, IReadOnlyList<VizActivityCanvasNode>?>(nameof(Nodes));

    public static readonly StyledProperty<IReadOnlyList<VizActivityCanvasEdge>?> EdgesProperty =
        AvaloniaProperty.Register<VizActivityCanvasSurface, IReadOnlyList<VizActivityCanvasEdge>?>(nameof(Edges));

    private readonly Dictionary<string, CachedEdgeGeometry> _edgeGeometryCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IBrush> _brushCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, DashStyle?> _dashStyleCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, FormattedText> _labelTextCache = new(StringComparer.Ordinal);
    private FormattedText? _pinnedGlyphText;
    private INotifyCollectionChanged? _nodesNotifier;
    private INotifyCollectionChanged? _edgesNotifier;

    public IReadOnlyList<VizActivityCanvasNode>? Nodes
    {
        get => GetValue(NodesProperty);
        set => SetValue(NodesProperty, value);
    }

    public IReadOnlyList<VizActivityCanvasEdge>? Edges
    {
        get => GetValue(EdgesProperty);
        set => SetValue(EdgesProperty, value);
    }

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);

        if (change.Property == NodesProperty)
        {
            AttachCollectionNotifier(ref _nodesNotifier, change.NewValue);
            InvalidateVisual();
        }
        else if (change.Property == EdgesProperty)
        {
            AttachCollectionNotifier(ref _edgesNotifier, change.NewValue);
            InvalidateVisual();
        }
    }

    public override void Render(DrawingContext context)
    {
        base.Render(context);

        var edges = Edges;
        var nodes = Nodes;
        if ((edges?.Count ?? 0) == 0 && (nodes?.Count ?? 0) == 0)
        {
            return;
        }

        var pinnedBrush = ResolveResourceBrush("NbnGoldBrush", Colors.Gold);
        var hoverBrush = ResolveResourceBrush("NbnTealBrush", Colors.Teal);
        var selectedBrush = ResolveResourceBrush("NbnAccentBrush", Colors.DeepSkyBlue);
        var labelBrush = ResolveResourceBrush("NbnInkBrush", Color.FromRgb(0x10, 0x1B, 0x22));

        var seenRoutes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (edges is not null)
        {
            foreach (var edge in edges)
            {
                if (!string.IsNullOrWhiteSpace(edge.RouteLabel))
                {
                    seenRoutes.Add(edge.RouteLabel);
                }

                var geometry = ResolveEdgeGeometry(edge);
                var dashStyle = ResolveDashStyle(edge.DirectionDashArray);
                var directionPen = CreatePen(edge.Stroke, edge.Opacity, edge.StrokeThickness, dashStyle);
                var activityPen = CreatePen(edge.ActivityStroke, edge.ActivityOpacity, edge.ActivityStrokeThickness, dashStyle: null);

                context.DrawGeometry(null, directionPen, geometry);
                context.DrawGeometry(null, activityPen, geometry);

                if (edge.IsPinned)
                {
                    var pinnedPen = new Pen(pinnedBrush, Math.Max(1.0, edge.StrokeThickness), dashStyle: dashStyle, lineCap: PenLineCap.Round);
                    context.DrawGeometry(null, pinnedPen, geometry);
                }

                if (edge.IsHovered)
                {
                    var hoverPen = new Pen(hoverBrush, Math.Max(1.0, edge.StrokeThickness), dashStyle: dashStyle, lineCap: PenLineCap.Round);
                    context.DrawGeometry(null, hoverPen, geometry);
                }

                if (edge.IsSelected)
                {
                    var selectedPen = new Pen(selectedBrush, Math.Max(1.0, edge.StrokeThickness), dashStyle: dashStyle, lineCap: PenLineCap.Round);
                    context.DrawGeometry(null, selectedPen, geometry);
                }
            }
        }

        PruneEdgeGeometryCache(seenRoutes);

        if (nodes is null)
        {
            return;
        }

        foreach (var node in nodes)
        {
            var radius = Math.Max(1.0, node.Diameter / 2.0);
            var center = new Point(node.Left + radius, node.Top + radius);
            context.DrawEllipse(ResolveColorBrush(node.Fill, node.FillOpacity), null, center, radius, radius);

            var baseStroke = CreatePen(node.Stroke, node.PulseOpacity, node.StrokeThickness, dashStyle: null);
            context.DrawEllipse(null, baseStroke, center, radius, radius);

            if (node.IsPinned)
            {
                var pinStroke = new Pen(pinnedBrush, 2.2, lineCap: PenLineCap.Round);
                context.DrawEllipse(null, pinStroke, center, radius, radius);
            }

            if (node.IsHovered)
            {
                var hoverStroke = new Pen(hoverBrush, 2.2, lineCap: PenLineCap.Round);
                context.DrawEllipse(null, hoverStroke, center, radius, radius);
            }

            if (node.IsSelected)
            {
                var selectedStroke = new Pen(selectedBrush, 2.8, lineCap: PenLineCap.Round);
                context.DrawEllipse(null, selectedStroke, center, radius, radius);
            }

            if (node.IsPinned)
            {
                var glyph = ResolvePinnedGlyphText(pinnedBrush);
                context.DrawText(glyph, new Point(node.Left + 2.0, node.Top - 0.5));
            }

            if (!string.IsNullOrWhiteSpace(node.Label))
            {
                var text = ResolveNodeLabelText(node.Label, labelBrush);
                var textOrigin = new Point(
                    center.X - (text.Width / 2.0),
                    center.Y - (text.Height / 2.0));
                context.DrawText(text, textOrigin);
            }
        }
    }

    private void AttachCollectionNotifier(
        ref INotifyCollectionChanged? existing,
        object? newValue)
    {
        if (existing is not null)
        {
            existing.CollectionChanged -= HandleBoundCollectionChanged;
            existing = null;
        }

        if (newValue is INotifyCollectionChanged replacement)
        {
            existing = replacement;
            existing.CollectionChanged += HandleBoundCollectionChanged;
        }
    }

    private void HandleBoundCollectionChanged(object? sender, NotifyCollectionChangedEventArgs e)
        => InvalidateVisual();

    private Geometry ResolveEdgeGeometry(VizActivityCanvasEdge edge)
    {
        var routeLabel = string.IsNullOrWhiteSpace(edge.RouteLabel) ? "<unnamed>" : edge.RouteLabel;
        var key = new EdgeGeometryKey(
            Quantize(edge.SourceX),
            Quantize(edge.SourceY),
            Quantize(edge.ControlX),
            Quantize(edge.ControlY),
            Quantize(edge.TargetX),
            Quantize(edge.TargetY));
        if (_edgeGeometryCache.TryGetValue(routeLabel, out var cached) && cached.Key == key)
        {
            return cached.Geometry;
        }

        var geometry = new StreamGeometry();
        using (var stream = geometry.Open())
        {
            stream.BeginFigure(new Point(edge.SourceX, edge.SourceY), isFilled: false);
            stream.QuadraticBezierTo(new Point(edge.ControlX, edge.ControlY), new Point(edge.TargetX, edge.TargetY));
            stream.EndFigure(isClosed: false);
        }

        _edgeGeometryCache[routeLabel] = new CachedEdgeGeometry(key, geometry);
        return geometry;
    }

    private void PruneEdgeGeometryCache(IReadOnlySet<string> seenRoutes)
    {
        if (_edgeGeometryCache.Count == 0)
        {
            return;
        }

        var stale = new List<string>();
        foreach (var key in _edgeGeometryCache.Keys)
        {
            if (!seenRoutes.Contains(key))
            {
                stale.Add(key);
            }
        }

        foreach (var key in stale)
        {
            _edgeGeometryCache.Remove(key);
        }
    }

    private Pen CreatePen(string colorCode, double opacity, double thickness, DashStyle? dashStyle)
    {
        var brush = ResolveColorBrush(colorCode, opacity);
        var safeThickness = Math.Max(0.6, thickness);
        return new Pen(brush, safeThickness, dashStyle: dashStyle, lineCap: PenLineCap.Round);
    }

    private IBrush ResolveColorBrush(string colorCode, double opacity)
    {
        var boundedOpacity = Math.Clamp(opacity, 0.0, 1.0);
        var cacheKey = $"{colorCode}|{boundedOpacity:0.###}";
        if (_brushCache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        if (!Color.TryParse(colorCode, out var parsed))
        {
            parsed = Colors.Gray;
        }

        var alpha = (byte)Math.Clamp(Math.Round(parsed.A * boundedOpacity), 0, 255);
        var withOpacity = Color.FromArgb(alpha, parsed.R, parsed.G, parsed.B);
        var brush = new SolidColorBrush(withOpacity);
        _brushCache[cacheKey] = brush;
        return brush;
    }

    private DashStyle? ResolveDashStyle(string dashPattern)
    {
        if (string.IsNullOrWhiteSpace(dashPattern))
        {
            return null;
        }

        if (_dashStyleCache.TryGetValue(dashPattern, out var cached))
        {
            return cached;
        }

        var segments = new List<double>();
        var tokens = dashPattern.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        foreach (var token in tokens)
        {
            if (!double.TryParse(token, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0.0)
            {
                continue;
            }

            segments.Add(parsed);
        }

        DashStyle? dash = segments.Count == 0 ? null : new DashStyle(segments, 0.0);
        _dashStyleCache[dashPattern] = dash;
        return dash;
    }

    private IBrush ResolveResourceBrush(string resourceName, Color fallbackColor)
    {
        var cacheKey = $"res:{resourceName}";
        if (_brushCache.TryGetValue(cacheKey, out var cached))
        {
            return cached;
        }

        var fallback = new SolidColorBrush(fallbackColor);
        _brushCache[cacheKey] = fallback;
        return fallback;
    }

    private FormattedText ResolveNodeLabelText(string label, IBrush foreground)
    {
        if (_labelTextCache.TryGetValue(label, out var cached))
        {
            return cached;
        }

        var layout = new FormattedText(
            label,
            CultureInfo.InvariantCulture,
            FlowDirection.LeftToRight,
            new Typeface(FontFamily.Default, FontStyle.Normal, FontWeight.SemiBold),
            10,
            foreground);
        _labelTextCache[label] = layout;
        return layout;
    }

    private FormattedText ResolvePinnedGlyphText(IBrush foreground)
    {
        if (_pinnedGlyphText is not null)
        {
            return _pinnedGlyphText;
        }

        _pinnedGlyphText = new FormattedText(
            "*",
            CultureInfo.InvariantCulture,
            FlowDirection.LeftToRight,
            new Typeface(FontFamily.Default, FontStyle.Normal, FontWeight.Bold),
            10,
            foreground);
        return _pinnedGlyphText;
    }

    private static int Quantize(double value)
        => (int)Math.Round(value * 1000.0, MidpointRounding.AwayFromZero);

    private readonly record struct EdgeGeometryKey(
        int SourceX,
        int SourceY,
        int ControlX,
        int ControlY,
        int TargetX,
        int TargetY);

    private readonly record struct CachedEdgeGeometry(EdgeGeometryKey Key, Geometry Geometry);
}
