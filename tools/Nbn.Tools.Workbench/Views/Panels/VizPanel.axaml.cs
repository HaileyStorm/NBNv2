using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.Media;
using Avalonia.Threading;
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
    private const double MinPanTranslationLimitPx = 320.0;
    private const int MaxPendingCanvasViewAttempts = 8;
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
    private Point _panLastPoint;
    private readonly Dictionary<int, Point> _activeTouchPoints = new();
    private bool _isTouchTransforming;
    private double _touchStartDistance;
    private double _touchStartScale;
    private Point _touchStartWorldCenter;
    private Vector _canvasPan;
    private readonly ScaleTransform _canvasScaleTransform = new(1, 1);
    private readonly TranslateTransform _canvasTranslateTransform = new(0, 0);
    private readonly TransformGroup _canvasTransformGroup = new();
    private INotifyPropertyChanged? _viewModelNotifier;
    private VizPanelViewModel? _boundViewModel;
    private PendingCanvasViewMode _pendingCanvasViewMode;
    private int _pendingCanvasViewAttempts;

    private enum PendingCanvasViewMode
    {
        None = 0,
        DefaultCenter = 1,
        Fit = 2
    }

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
            _canvasTransformGroup.Children.Add(_canvasScaleTransform);
            _canvasTransformGroup.Children.Add(_canvasTranslateTransform);
            ActivityCanvasScaleRoot.RenderTransform = _canvasTransformGroup;
        }

        if (ActivityCanvasScrollViewer is not null)
        {
            ActivityCanvasScrollViewer.SizeChanged += ActivityCanvasScrollViewerSizeChanged;
        }

        DataContextChanged += VizPanelDataContextChanged;
        AttachedToVisualTree += (_, _) => SyncCanvasScaleVisuals();
        DetachedFromVisualTree += (_, _) => DetachViewModelNotifier();
    }

    private VizPanelViewModel? ViewModel => DataContext as VizPanelViewModel;

    private void VizPanelDataContextChanged(object? sender, EventArgs e)
    {
        DetachViewModelNotifier();
        _boundViewModel = DataContext as VizPanelViewModel;
        _viewModelNotifier = _boundViewModel as INotifyPropertyChanged;
        if (_viewModelNotifier is not null)
        {
            _viewModelNotifier.PropertyChanged += ViewModelPropertyChanged;
        }

        if (_boundViewModel is not null)
        {
            _boundViewModel.VisualizationSelectionChanged += ViewModelVisualizationSelectionChanged;
        }

        SyncCanvasScaleVisuals();
        RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
    }

    private void ViewModelPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName is nameof(VizPanelViewModel.ActivityCanvasWidth)
            or nameof(VizPanelViewModel.ActivityCanvasHeight))
        {
            SyncCanvasScaleVisuals();
            if (_pendingCanvasViewMode != PendingCanvasViewMode.None)
            {
                TryApplyPendingCanvasView();
            }
        }
    }

    private void ViewModelVisualizationSelectionChanged()
        => RequestCanvasView(PendingCanvasViewMode.DefaultCenter);

    private void ActivityCanvasScrollViewerSizeChanged(object? sender, SizeChangedEventArgs e)
    {
        if (_pendingCanvasViewMode != PendingCanvasViewMode.None)
        {
            TryApplyPendingCanvasView();
        }
    }

    private void DetachViewModelNotifier()
    {
        if (_viewModelNotifier is not null)
        {
            _viewModelNotifier.PropertyChanged -= ViewModelPropertyChanged;
            _viewModelNotifier = null;
        }

        if (_boundViewModel is not null)
        {
            _boundViewModel.VisualizationSelectionChanged -= ViewModelVisualizationSelectionChanged;
            _boundViewModel = null;
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

        if (e.KeyModifiers.HasFlag(KeyModifiers.Shift))
        {
            if (!TryGetViewportPointerPoint(e, out var viewportPoint))
            {
                e.Handled = true;
                return;
            }

            var zoomDelta = Math.Abs(e.Delta.Y) >= Math.Abs(e.Delta.X) ? e.Delta.Y : e.Delta.X;
            if (Math.Abs(zoomDelta) <= double.Epsilon)
            {
                e.Handled = true;
                return;
            }

            var zoomFactor = zoomDelta >= 0 ? WheelZoomFactor : 1.0 / WheelZoomFactor;
            SetCanvasScale(_canvasScale * zoomFactor, viewportPoint);
            e.Handled = true;
            return;
        }

        // Block wheel/touchpad panning and pinch-to-zoom unless Shift is held.
        e.Handled = true;
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
                    RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
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
        var worldX = (scrollViewer.Offset.X + anchor.X - _canvasPan.X) / oldScale;
        var worldY = (scrollViewer.Offset.Y + anchor.Y - _canvasPan.Y) / oldScale;

        _canvasScale = clampedScale;
        SyncCanvasScaleVisuals();

        var targetOffset = new Vector(
            (worldX * _canvasScale) + _canvasPan.X - anchor.X,
            (worldY * _canvasScale) + _canvasPan.Y - anchor.Y);
        ApplyCanvasOffset(targetOffset, scrollViewer, absorbResidualIntoPan: true);
    }

    private void FitCanvasToViewport()
        => RequestCanvasView(PendingCanvasViewMode.Fit);

    private void RequestCanvasView(PendingCanvasViewMode mode)
    {
        if (mode == PendingCanvasViewMode.None)
        {
            return;
        }

        _pendingCanvasViewMode = _pendingCanvasViewMode == PendingCanvasViewMode.Fit
            ? PendingCanvasViewMode.Fit
            : mode;
        _pendingCanvasViewAttempts = 0;
        TryApplyPendingCanvasView();
        if (_pendingCanvasViewMode != PendingCanvasViewMode.None)
        {
            Dispatcher.UIThread.Post(TryApplyPendingCanvasView, DispatcherPriority.Render);
        }
    }

    private void TryApplyPendingCanvasView()
    {
        if (_pendingCanvasViewMode == PendingCanvasViewMode.None)
        {
            return;
        }

        var viewModel = ViewModel;
        var scrollViewer = ActivityCanvasScrollViewer;
        var ready = viewModel is not null
            && scrollViewer is not null
            && viewModel.ActivityCanvasWidth > 0
            && viewModel.ActivityCanvasHeight > 0
            && scrollViewer.Viewport.Width > 0
            && scrollViewer.Viewport.Height > 0;
        if (!ready)
        {
            if (_pendingCanvasViewAttempts < MaxPendingCanvasViewAttempts)
            {
                _pendingCanvasViewAttempts++;
                Dispatcher.UIThread.Post(TryApplyPendingCanvasView, DispatcherPriority.Background);
            }

            return;
        }

        var mode = _pendingCanvasViewMode;
        _pendingCanvasViewMode = PendingCanvasViewMode.None;
        _pendingCanvasViewAttempts = 0;
        if (mode == PendingCanvasViewMode.Fit)
        {
            ApplyFitCanvasView(scrollViewer!, viewModel!);
        }
        else
        {
            ApplyDefaultCanvasView(scrollViewer!, viewModel!);
        }
    }

    private void ApplyDefaultCanvasView(ScrollViewer scrollViewer, VizPanelViewModel viewModel)
        => CenterCanvasAtScale(scrollViewer, viewModel, 1.0);

    private void ApplyFitCanvasView(ScrollViewer scrollViewer, VizPanelViewModel viewModel)
    {
        var fitScaleX = scrollViewer.Viewport.Width / viewModel.ActivityCanvasWidth;
        var fitScaleY = scrollViewer.Viewport.Height / viewModel.ActivityCanvasHeight;
        var fitScale = Math.Min(1.0, Math.Min(fitScaleX, fitScaleY));
        if (!double.IsFinite(fitScale) || fitScale <= 0)
        {
            fitScale = 1.0;
        }

        CenterCanvasAtScale(scrollViewer, viewModel, fitScale);
    }

    private void CenterCanvasAtScale(ScrollViewer scrollViewer, VizPanelViewModel viewModel, double targetScale)
    {
        _canvasScale = ClampScale(targetScale);
        _canvasPan = default;
        scrollViewer.Offset = default;
        SyncCanvasScaleVisuals();

        var scaledWidth = viewModel.ActivityCanvasWidth * _canvasScale;
        var scaledHeight = viewModel.ActivityCanvasHeight * _canvasScale;
        var centeredOffset = new Vector(
            Math.Max(0.0, (scaledWidth - scrollViewer.Viewport.Width) / 2.0),
            Math.Max(0.0, (scaledHeight - scrollViewer.Viewport.Height) / 2.0));
        ApplyCanvasOffset(centeredOffset, scrollViewer, absorbResidualIntoPan: false);
    }

    private void ApplyCanvasOffset(Vector targetOffset, ScrollViewer scrollViewer, bool absorbResidualIntoPan)
    {
        var clampedOffset = ClampOffset(targetOffset, scrollViewer);
        scrollViewer.Offset = clampedOffset;
        if (!absorbResidualIntoPan)
        {
            return;
        }

        var residual = clampedOffset - targetOffset;
        if (Math.Abs(residual.X) > 0.0001 || Math.Abs(residual.Y) > 0.0001)
        {
            SetCanvasPan(_canvasPan + residual, scrollViewer);
        }
    }

    private void PanCanvasBy(double deltaX, double deltaY)
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        if (scrollViewer is null)
        {
            return;
        }

        var startOffset = scrollViewer.Offset;
        var targetOffset = ClampOffset(new Vector(startOffset.X + deltaX, startOffset.Y + deltaY), scrollViewer);
        scrollViewer.Offset = targetOffset;
        var consumed = targetOffset - startOffset;
        var remaining = new Vector(deltaX - consumed.X, deltaY - consumed.Y);
        if (Math.Abs(remaining.X) > 0.0001 || Math.Abs(remaining.Y) > 0.0001)
        {
            SetCanvasPan(_canvasPan + remaining, scrollViewer);
        }
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
        _panLastPoint = e.GetPosition(scrollViewer);
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
        var delta = currentPoint - _panLastPoint;
        if (Math.Abs(delta.X) > 0.0001 || Math.Abs(delta.Y) > 0.0001)
        {
            PanCanvasBy(delta.X, delta.Y);
            _panLastPoint = currentPoint;
        }

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
            (scrollViewer.Offset.X + center.X - _canvasPan.X) / safeScale,
            (scrollViewer.Offset.Y + center.Y - _canvasPan.Y) / safeScale);
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
            (_touchStartWorldCenter.X * _canvasScale) + _canvasPan.X - center.X,
            (_touchStartWorldCenter.Y * _canvasScale) + _canvasPan.Y - center.Y);
        ApplyCanvasOffset(targetOffset, scrollViewer, absorbResidualIntoPan: true);
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
        _canvasPan = ClampPan(_canvasPan, ActivityCanvasScrollViewer);
        _canvasTranslateTransform.X = _canvasPan.X;
        _canvasTranslateTransform.Y = _canvasPan.Y;

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

    private void SetCanvasPan(Vector pan, ScrollViewer? scrollViewer)
    {
        _canvasPan = ClampPan(pan, scrollViewer);
        _canvasTranslateTransform.X = _canvasPan.X;
        _canvasTranslateTransform.Y = _canvasPan.Y;
    }

    private static Vector ClampPan(Vector pan, ScrollViewer? scrollViewer)
    {
        var viewport = scrollViewer?.Viewport ?? default;
        var limitX = Math.Max(MinPanTranslationLimitPx, viewport.Width * 0.55);
        var limitY = Math.Max(MinPanTranslationLimitPx, viewport.Height * 0.55);
        return new Vector(
            Math.Clamp(pan.X, -limitX, limitX),
            Math.Clamp(pan.Y, -limitY, limitY));
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
