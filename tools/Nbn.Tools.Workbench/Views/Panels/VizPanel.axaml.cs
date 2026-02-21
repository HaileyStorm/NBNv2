using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Media;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class VizPanel : UserControl
{
    private const double PressProbeDistancePx = 5.0;
    private const double MinCanvasScale = 0.35;
    private const double MaxCanvasScale = 4.0;
    private const double ButtonZoomFactor = 1.2;
    private const double WheelZoomFactor = 1.12;
    private const double PanButtonStepPx = 96.0;
    private static readonly Point[] HoverProbeOffsets =
    {
        new(0, 0)
    };
    private static readonly Point[] PressProbeOffsets =
    {
        new(0, 0),
        new(PressProbeDistancePx, 0),
        new(-PressProbeDistancePx, 0),
        new(0, PressProbeDistancePx),
        new(0, -PressProbeDistancePx),
        new(PressProbeDistancePx * 0.6, PressProbeDistancePx * 0.6),
        new(-PressProbeDistancePx * 0.6, PressProbeDistancePx * 0.6),
        new(PressProbeDistancePx * 0.6, -PressProbeDistancePx * 0.6),
        new(-PressProbeDistancePx * 0.6, -PressProbeDistancePx * 0.6)
    };

    private double _canvasScale = 1.0;
    private bool _isPanning;
    private IPointer? _panPointer;
    private int _panPointerId = -1;
    private Point _panStartPoint;
    private Vector _panStartOffset;
    private readonly Dictionary<int, Point> _activeTouchPoints = new();
    private bool _isTouchTransforming;
    private double _touchStartDistance;
    private double _touchStartScale;
    private Point _touchStartWorldCenter;
    private readonly ScaleTransform _canvasScaleTransform = new(1, 1);
    private INotifyPropertyChanged? _viewModelNotifier;

    public VizPanel()
    {
        InitializeComponent();
        AddHandler(
            InputElement.PointerMovedEvent,
            VizRootPointerMoved,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
        AddHandler(
            InputElement.PointerExitedEvent,
            VizRootPointerExited,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
        AddHandler(
            InputElement.PointerReleasedEvent,
            VizRootPointerReleased,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
        AddHandler(
            InputElement.PointerCaptureLostEvent,
            VizRootPointerCaptureLost,
            RoutingStrategies.Bubble,
            handledEventsToo: true);
        AddHandler(
            InputElement.PointerWheelChangedEvent,
            VizRootPointerWheelChanged,
            RoutingStrategies.Bubble,
            handledEventsToo: true);

        if (ActivityCanvasScaleRoot is not null)
        {
            ActivityCanvasScaleRoot.RenderTransform = _canvasScaleTransform;
        }

        DataContextChanged += VizPanelDataContextChanged;
        AttachedToVisualTree += (_, _) => SyncCanvasScaleVisuals();
        DetachedFromVisualTree += (_, _) => DetachViewModelNotifier();
    }

    private VizPanelViewModel? ViewModel => DataContext as VizPanelViewModel;

    private void VizPanelDataContextChanged(object? sender, EventArgs e)
    {
        DetachViewModelNotifier();
        _viewModelNotifier = DataContext as INotifyPropertyChanged;
        if (_viewModelNotifier is not null)
        {
            _viewModelNotifier.PropertyChanged += ViewModelPropertyChanged;
        }

        SyncCanvasScaleVisuals();
    }

    private void ViewModelPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(VizPanelViewModel.ActivityCanvasWidth)
            or nameof(VizPanelViewModel.ActivityCanvasHeight))
        {
            SyncCanvasScaleVisuals();
        }
    }

    private void DetachViewModelNotifier()
    {
        if (_viewModelNotifier is not null)
        {
            _viewModelNotifier.PropertyChanged -= ViewModelPropertyChanged;
            _viewModelNotifier = null;
        }
    }

    private void VizRootPointerMoved(object? sender, PointerEventArgs e)
    {
        if (TryHandleTouchGestureMove(e))
        {
            e.Handled = true;
            return;
        }

        if (TryHandlePanMove(e))
        {
            e.Handled = true;
            return;
        }

        if (e.Pointer.Type == PointerType.Touch)
        {
            return;
        }

        if (ViewModel is null)
        {
            return;
        }

        if (!TryGetCanvasPointerPoint(e, out var point, out var isInsideCanvas))
        {
            return;
        }

        if (!isInsideCanvas)
        {
            ViewModel.ClearCanvasHover();
            return;
        }

        UpdateCanvasHover(point);
    }

    private void VizRootPointerExited(object? sender, PointerEventArgs e)
    {
        if (!_isPanning && e.Pointer.Type != PointerType.Touch)
        {
            ViewModel?.ClearCanvasHover();
        }
    }

    private void VizRootPointerReleased(object? sender, PointerReleasedEventArgs e)
    {
        HandleTouchPointerReleased(e.Pointer);
        EndPan(e.Pointer);
    }

    private void VizRootPointerCaptureLost(object? sender, PointerCaptureLostEventArgs e)
    {
        HandleTouchPointerReleased(e.Pointer);
        EndPan(e.Pointer);
    }

    private void VizRootPointerWheelChanged(object? sender, PointerWheelEventArgs e)
    {
        if (ViewModel is null || !TryGetCanvasPointerPoint(e, out _, out var isInsideCanvas) || !isInsideCanvas)
        {
            return;
        }

        if (e.KeyModifiers.HasFlag(KeyModifiers.Control))
        {
            if (!TryGetViewportPointerPoint(e, out var viewportPoint))
            {
                return;
            }

            var zoomFactor = e.Delta.Y >= 0 ? WheelZoomFactor : 1.0 / WheelZoomFactor;
            SetCanvasScale(_canvasScale * zoomFactor, viewportPoint);
            e.Handled = true;
            return;
        }

        if (e.KeyModifiers.HasFlag(KeyModifiers.Shift))
        {
            PanCanvasBy(-e.Delta.Y * (PanButtonStepPx * 0.45), 0);
            e.Handled = true;
        }
    }

    private void ActivityCanvasPointerPressed(object? sender, PointerPressedEventArgs e)
    {
        if (ViewModel is null || sender is not Visual visual || e.Handled)
        {
            return;
        }

        if (HandleTouchPointerPressed(e))
        {
            return;
        }

        var pointer = e.GetCurrentPoint(visual).Properties;
        var hasPanModifier = e.KeyModifiers.HasFlag(KeyModifiers.Shift);
        if (pointer.IsMiddleButtonPressed || (hasPanModifier && pointer.IsLeftButtonPressed))
        {
            BeginPan(e);
            e.Handled = true;
            return;
        }

        if (!TryGetCanvasPointerPoint(e, out var point, out _))
        {
            return;
        }

        var isPrimaryDoubleClick = pointer.IsLeftButtonPressed && e.ClickCount >= 2;
        var hasHit = TryResolveCanvasHitWithProbe(point, PressProbeOffsets, hoverMode: false, out var node, out var edge);
        if (!hasHit)
        {
            if (isPrimaryDoubleClick)
            {
                FitCanvasToViewport();
                e.Handled = true;
                return;
            }

            if (ViewModel.TrySelectHoveredCanvasItem(pointer.IsRightButtonPressed))
            {
                e.Handled = true;
            }

            return;
        }

        if (node is not null)
        {
            ViewModel.SetCanvasNodeHover(node, point.X, point.Y);
            if (isPrimaryDoubleClick && node.NeuronId is null)
            {
                if (ViewModel.ZoomToRegion(node.NavigateRegionId))
                {
                    FitCanvasToViewport();
                }

                e.Handled = true;
                return;
            }

            if (pointer.IsRightButtonPressed)
            {
                ViewModel.TogglePinCanvasNode(node);
            }
            else
            {
                ViewModel.SelectCanvasNode(node);
            }

            e.Handled = true;
            return;
        }

        if (edge is null)
        {
            return;
        }

        if (pointer.IsRightButtonPressed)
        {
            ViewModel.SetCanvasEdgeHover(edge, point.X, point.Y);
            ViewModel.TogglePinCanvasEdge(edge);
        }
        else
        {
            ViewModel.SetCanvasEdgeHover(edge, point.X, point.Y);
            ViewModel.SelectCanvasEdge(edge);
        }

        e.Handled = true;
    }

    private void CanvasZoomOutClicked(object? sender, RoutedEventArgs e)
    {
        SetCanvasScale(_canvasScale / ButtonZoomFactor);
        e.Handled = true;
    }

    private void CanvasZoomInClicked(object? sender, RoutedEventArgs e)
    {
        SetCanvasScale(_canvasScale * ButtonZoomFactor);
        e.Handled = true;
    }

    private void CanvasFitViewClicked(object? sender, RoutedEventArgs e)
    {
        FitCanvasToViewport();
        e.Handled = true;
    }

    private void CanvasPanLeftClicked(object? sender, RoutedEventArgs e)
    {
        PanCanvasBy(-PanButtonStepPx, 0);
        e.Handled = true;
    }

    private void CanvasPanRightClicked(object? sender, RoutedEventArgs e)
    {
        PanCanvasBy(PanButtonStepPx, 0);
        e.Handled = true;
    }

    private void CanvasPanUpClicked(object? sender, RoutedEventArgs e)
    {
        PanCanvasBy(0, -PanButtonStepPx);
        e.Handled = true;
    }

    private void CanvasPanDownClicked(object? sender, RoutedEventArgs e)
    {
        PanCanvasBy(0, PanButtonStepPx);
        e.Handled = true;
    }

    private void UpdateCanvasHover(Point point)
    {
        if (ViewModel is null)
        {
            return;
        }

        if (!TryResolveCanvasHitWithProbe(point, HoverProbeOffsets, hoverMode: true, out var node, out var edge))
        {
            ViewModel.ClearCanvasHover();
            return;
        }

        if (node is not null)
        {
            ViewModel.SetCanvasNodeHover(node, point.X, point.Y);
            return;
        }

        if (edge is not null)
        {
            ViewModel.SetCanvasEdgeHover(edge, point.X, point.Y);
            return;
        }

        ViewModel.ClearCanvasHover();
    }

    private bool TryResolveCanvasHitWithProbe(
        Point point,
        IReadOnlyList<Point> probeOffsets,
        bool hoverMode,
        out VizActivityCanvasNode? node,
        out VizActivityCanvasEdge? edge)
    {
        node = null;
        edge = null;
        if (ViewModel is null)
        {
            return false;
        }

        foreach (var offset in probeOffsets)
        {
            var hasHit = hoverMode
                ? ViewModel.TryResolveCanvasHoverHit(point.X + offset.X, point.Y + offset.Y, out node, out edge)
                : ViewModel.TryResolveCanvasHit(point.X + offset.X, point.Y + offset.Y, out node, out edge);
            if (hasHit)
            {
                return true;
            }
        }

        return false;
    }

    private bool TryGetCanvasPointerPoint(PointerEventArgs e, out Point point, out bool inside)
    {
        point = default;
        inside = false;
        var canvas = ActivityCanvasSurface;
        if (canvas is null || !canvas.IsVisible || canvas.Bounds.Width <= 0 || canvas.Bounds.Height <= 0)
        {
            return false;
        }

        point = e.GetPosition(canvas);
        inside = point.X >= 0
            && point.Y >= 0
            && point.X <= canvas.Bounds.Width
            && point.Y <= canvas.Bounds.Height;
        return true;
    }

    private bool TryGetViewportPointerPoint(PointerEventArgs e, out Point point)
    {
        point = default;
        var scrollViewer = ActivityCanvasScrollViewer;
        if (scrollViewer is null || scrollViewer.Viewport.Width <= 0 || scrollViewer.Viewport.Height <= 0)
        {
            return false;
        }

        point = e.GetPosition(scrollViewer);
        return true;
    }

    private void SetCanvasScale(double targetScale, Point? viewportAnchor = null)
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (ViewModel is null || scrollViewer is null)
        {
            return;
        }

        var clampedScale = ClampScale(targetScale);
        var oldScale = Math.Max(0.0001, _canvasScale);
        if (Math.Abs(clampedScale - _canvasScale) <= 0.0001)
        {
            SyncCanvasScaleVisuals();
            return;
        }

        var anchor = viewportAnchor ?? new Point(scrollViewer.Viewport.Width / 2.0, scrollViewer.Viewport.Height / 2.0);
        var worldX = (scrollViewer.Offset.X + anchor.X) / oldScale;
        var worldY = (scrollViewer.Offset.Y + anchor.Y) / oldScale;

        _canvasScale = clampedScale;
        SyncCanvasScaleVisuals();

        var targetOffset = new Vector((worldX * _canvasScale) - anchor.X, (worldY * _canvasScale) - anchor.Y);
        scrollViewer.Offset = ClampOffset(targetOffset, scrollViewer);
    }

    private void FitCanvasToViewport()
    {
        var viewModel = ViewModel;
        var scrollViewer = ActivityCanvasScrollViewer;
        if (viewModel is null || scrollViewer is null)
        {
            return;
        }

        if (scrollViewer.Viewport.Width <= 0 || scrollViewer.Viewport.Height <= 0)
        {
            SetCanvasScale(1.0);
            return;
        }

        var fitScaleX = scrollViewer.Viewport.Width / viewModel.ActivityCanvasWidth;
        var fitScaleY = scrollViewer.Viewport.Height / viewModel.ActivityCanvasHeight;
        var fitScale = Math.Min(1.0, Math.Min(fitScaleX, fitScaleY));
        if (!double.IsFinite(fitScale) || fitScale <= 0)
        {
            fitScale = 1.0;
        }

        _canvasScale = ClampScale(fitScale);
        SyncCanvasScaleVisuals();

        var scaledWidth = viewModel.ActivityCanvasWidth * _canvasScale;
        var scaledHeight = viewModel.ActivityCanvasHeight * _canvasScale;
        var centeredOffset = new Vector(
            Math.Max(0.0, (scaledWidth - scrollViewer.Viewport.Width) / 2.0),
            Math.Max(0.0, (scaledHeight - scrollViewer.Viewport.Height) / 2.0));
        scrollViewer.Offset = ClampOffset(centeredOffset, scrollViewer);
    }

    private void PanCanvasBy(double deltaX, double deltaY)
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (scrollViewer is null)
        {
            return;
        }

        var targetOffset = new Vector(scrollViewer.Offset.X + deltaX, scrollViewer.Offset.Y + deltaY);
        scrollViewer.Offset = ClampOffset(targetOffset, scrollViewer);
    }

    private void BeginPan(PointerPressedEventArgs e)
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (scrollViewer is null)
        {
            return;
        }

        _isPanning = true;
        _panPointer = e.Pointer;
        _panPointerId = e.Pointer.Id;
        _panStartPoint = e.GetPosition(scrollViewer);
        _panStartOffset = scrollViewer.Offset;
        _panPointer.Capture(ActivityCanvasSurface);
    }

    private bool TryHandlePanMove(PointerEventArgs e)
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (!_isPanning || _panPointer is null || scrollViewer is null || e.Pointer.Id != _panPointerId)
        {
            return false;
        }

        var currentPoint = e.GetPosition(scrollViewer);
        var delta = currentPoint - _panStartPoint;
        var targetOffset = new Vector(_panStartOffset.X - delta.X, _panStartOffset.Y - delta.Y);
        scrollViewer.Offset = ClampOffset(targetOffset, scrollViewer);
        return true;
    }

    private void EndPan(IPointer? pointer = null)
    {
        if (!_isPanning)
        {
            return;
        }

        if (pointer is not null && _panPointer is not null && pointer.Id != _panPointerId)
        {
            return;
        }

        _panPointer?.Capture(null);
        _panPointer = null;
        _panPointerId = -1;
        _isPanning = false;
    }

    private bool HandleTouchPointerPressed(PointerPressedEventArgs e)
    {
        if (e.Pointer.Type != PointerType.Touch || !TryGetViewportPointerPoint(e, out var viewportPoint))
        {
            return false;
        }

        _activeTouchPoints[e.Pointer.Id] = viewportPoint;
        if (_activeTouchPoints.Count >= 2)
        {
            BeginTouchTransformGesture();
            e.Handled = true;
            return true;
        }

        return false;
    }

    private bool TryHandleTouchGestureMove(PointerEventArgs e)
    {
        if (e.Pointer.Type != PointerType.Touch
            || !_activeTouchPoints.ContainsKey(e.Pointer.Id)
            || !TryGetViewportPointerPoint(e, out var viewportPoint))
        {
            return false;
        }

        _activeTouchPoints[e.Pointer.Id] = viewportPoint;
        if (_activeTouchPoints.Count < 2)
        {
            return false;
        }

        if (!_isTouchTransforming)
        {
            BeginTouchTransformGesture();
        }

        return UpdateTouchTransformGesture();
    }

    private void HandleTouchPointerReleased(IPointer pointer)
    {
        if (pointer.Type != PointerType.Touch)
        {
            return;
        }

        _activeTouchPoints.Remove(pointer.Id);
        if (_activeTouchPoints.Count >= 2)
        {
            BeginTouchTransformGesture();
            return;
        }

        _isTouchTransforming = false;
    }

    private void BeginTouchTransformGesture()
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (scrollViewer is null || _activeTouchPoints.Count < 2)
        {
            _isTouchTransforming = false;
            return;
        }

        var points = _activeTouchPoints.Values.Take(2).ToArray();
        var center = Midpoint(points[0], points[1]);
        _touchStartDistance = Distance(points[0], points[1]);
        _touchStartScale = _canvasScale;
        var safeScale = Math.Max(0.0001, _touchStartScale);
        _touchStartWorldCenter = new Point(
            (scrollViewer.Offset.X + center.X) / safeScale,
            (scrollViewer.Offset.Y + center.Y) / safeScale);
        _isTouchTransforming = true;
    }

    private bool UpdateTouchTransformGesture()
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (scrollViewer is null || !_isTouchTransforming || _activeTouchPoints.Count < 2)
        {
            return false;
        }

        var points = _activeTouchPoints.Values.Take(2).ToArray();
        var center = Midpoint(points[0], points[1]);
        var currentDistance = Distance(points[0], points[1]);
        var scaleFactor = _touchStartDistance > 1.0 ? currentDistance / _touchStartDistance : 1.0;

        _canvasScale = ClampScale(_touchStartScale * scaleFactor);
        SyncCanvasScaleVisuals();

        var targetOffset = new Vector(
            (_touchStartWorldCenter.X * _canvasScale) - center.X,
            (_touchStartWorldCenter.Y * _canvasScale) - center.Y);
        scrollViewer.Offset = ClampOffset(targetOffset, scrollViewer);
        return true;
    }

    private void SyncCanvasScaleVisuals()
    {
        var viewModel = ViewModel;
        if (viewModel is not null && ActivityCanvasExtentHost is not null)
        {
            ActivityCanvasExtentHost.Width = viewModel.ActivityCanvasWidth * _canvasScale;
            ActivityCanvasExtentHost.Height = viewModel.ActivityCanvasHeight * _canvasScale;
        }

        _canvasScaleTransform.ScaleX = _canvasScale;
        _canvasScaleTransform.ScaleY = _canvasScale;

        if (CanvasZoomLabel is not null)
        {
            CanvasZoomLabel.Text = $"Zoom {_canvasScale * 100.0:0}%";
        }

        if (ActivityCanvasScrollViewer is { } scrollViewer)
        {
            scrollViewer.Offset = ClampOffset(scrollViewer.Offset, scrollViewer);
        }
    }

    private Vector ClampOffset(Vector offset, ScrollViewer scrollViewer)
    {
        var width = ViewModel?.ActivityCanvasWidth ?? scrollViewer.Extent.Width;
        var height = ViewModel?.ActivityCanvasHeight ?? scrollViewer.Extent.Height;
        var scaledWidth = width * _canvasScale;
        var scaledHeight = height * _canvasScale;
        if (!double.IsFinite(scaledWidth) || scaledWidth <= 0)
        {
            scaledWidth = scrollViewer.Extent.Width;
        }

        if (!double.IsFinite(scaledHeight) || scaledHeight <= 0)
        {
            scaledHeight = scrollViewer.Extent.Height;
        }

        var maxX = Math.Max(0.0, scaledWidth - scrollViewer.Viewport.Width);
        var maxY = Math.Max(0.0, scaledHeight - scrollViewer.Viewport.Height);
        return new Vector(
            Math.Clamp(offset.X, 0.0, maxX),
            Math.Clamp(offset.Y, 0.0, maxY));
    }

    private static double ClampScale(double scale) => Math.Clamp(scale, MinCanvasScale, MaxCanvasScale);

    private static Point Midpoint(Point left, Point right)
        => new((left.X + right.X) / 2.0, (left.Y + right.Y) / 2.0);

    private static double Distance(Point left, Point right)
    {
        var dx = right.X - left.X;
        var dy = right.Y - left.Y;
        return Math.Sqrt((dx * dx) + (dy * dy));
    }
}
