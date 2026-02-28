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
    private const double FitContentPaddingPx = 18.0;
    private const int MaxPendingCanvasViewAttempts = 8;
    private const double CanvasViewportHeightRatio = 0.74;
    private const double CanvasViewportMinHeightPx = 460.0;
    private const double CanvasViewportMaxHeightPx = 1800.0;
    private const double DefaultCanvasViewOffsetTolerancePx = 1.5;
    private const double MiniChartCenterBiasRatio = 1.0;
    private const double MiniChartCenterBiasMaxPx = 340.0;
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
    private PendingCanvasViewMode _pendingCanvasViewMode;
    private int _pendingCanvasViewAttempts;
    private uint? _lastNavigationFocusRegionId;
    private Guid? _lastSelectedBrainId;
    private TopLevel? _canvasTopLevel;
    private bool _defaultCanvasViewApplied;

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
            RoutingStrategies.Tunnel | RoutingStrategies.Bubble,
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
            ActivityCanvasScrollViewer.PropertyChanged += ActivityCanvasScrollViewerPropertyChanged;
        }

        SizeChanged += VizPanelSizeChanged;
        DataContextChanged += VizPanelDataContextChanged;
        AttachedToVisualTree += (_, _) =>
        {
            AttachCanvasTopLevelEvents();
            SyncCanvasScaleVisuals();
            UpdateCanvasViewportHeight();
        };
        DetachedFromVisualTree += (_, _) =>
        {
            DetachCanvasTopLevelEvents();
            DetachViewModelNotifier();
        };
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

        CaptureNavigationContext();
        _lastSelectedBrainId = ViewModel?.SelectedBrain?.BrainId;
        SyncCanvasScaleVisuals();
        RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
        UpdateCanvasViewportHeight();
    }

    private void ViewModelPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName == nameof(VizPanelViewModel.SelectedBrain))
        {
            var selectedBrainId = ViewModel?.SelectedBrain?.BrainId;
            if (!selectedBrainId.HasValue)
            {
                CaptureNavigationContext();
                return;
            }

            if (_lastSelectedBrainId == selectedBrainId)
            {
                CaptureNavigationContext();
                return;
            }

            _lastSelectedBrainId = selectedBrainId;
            CaptureNavigationContext();
            RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
            return;
        }

        if (e.PropertyName == nameof(VizPanelViewModel.ShowMiniActivityChart))
        {
            if (_defaultCanvasViewApplied)
            {
                RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
            }

            return;
        }

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

    private void ActivityCanvasScrollViewerSizeChanged(object? sender, SizeChangedEventArgs e)
    {
        UpdateCanvasViewportHeight();
        if (_pendingCanvasViewMode != PendingCanvasViewMode.None)
        {
            TryApplyPendingCanvasView();
        }
    }

    private void VizPanelSizeChanged(object? sender, SizeChangedEventArgs e)
        => UpdateCanvasViewportHeight();

    private void DetachViewModelNotifier()
    {
        if (_viewModelNotifier is not null)
        {
            _viewModelNotifier.PropertyChanged -= ViewModelPropertyChanged;
            _viewModelNotifier = null;
        }

        _lastNavigationFocusRegionId = null;
        _lastSelectedBrainId = null;
    }

    private void ActivityCanvasScrollViewerPropertyChanged(object? sender, AvaloniaPropertyChangedEventArgs e)
    {
        if (e.Property == ScrollViewer.OffsetProperty)
        {
            InvalidateDefaultCanvasViewState();
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
        if (e.Handled)
        {
            return;
        }

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
        var isPrimarySingleClick = pointer.IsLeftButtonPressed && e.ClickCount == 1;
        var hasHit = TryResolveCanvasHitWithProbe(point, PressProbeOffsets, hoverMode: false, out var node, out var edge);
        if (!hasHit)
        {
            if (isPrimaryDoubleClick)
            {
                HandleEmptyCanvasDoubleClick();
                e.Handled = true;
                return;
            }

            if (isPrimarySingleClick && ViewModel.TryClearCanvasSelectionFromEmptyClick())
            {
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

    private void CanvasRegionZoomClicked(object? sender, RoutedEventArgs e)
        => QueueDefaultCenterAfterNavigation();

    private void CanvasShowFullBrainClicked(object? sender, RoutedEventArgs e)
        => QueueDefaultCenterAfterNavigation();

    private void CanvasNavigateClicked(object? sender, RoutedEventArgs e)
        => QueueDefaultCenterAfterNavigation();

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
        InvalidateDefaultCanvasViewState();
        SyncCanvasScaleVisuals();

        var targetOffset = new Vector(
            (worldX * _canvasScale) + _canvasPan.X - anchor.X,
            (worldY * _canvasScale) + _canvasPan.Y - anchor.Y);
        ApplyCanvasOffset(targetOffset, scrollViewer, absorbResidualIntoPan: true);
    }

    private void FitCanvasToViewport()
        => RequestCanvasView(PendingCanvasViewMode.Fit);

    private void HandleEmptyCanvasDoubleClick()
    {
        var viewModel = ViewModel;
        if (viewModel is null)
        {
            RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
            return;
        }

        var action = viewModel.HandleEmptyCanvasDoubleClick(IsCanvasAtDefaultView());
        if (action == VizPanelViewModel.EmptyCanvasDoubleClickAction.ShowFullBrain)
        {
            QueueDefaultCenterAfterNavigation();
            return;
        }

        RequestCanvasView(PendingCanvasViewMode.DefaultCenter);
    }

    private bool IsCanvasAtDefaultView()
    {
        var viewModel = ViewModel;
        var scrollViewer = ActivityCanvasScrollViewer;
        if (viewModel is null
            || scrollViewer is null
            || scrollViewer.Viewport.Width <= 0
            || scrollViewer.Viewport.Height <= 0)
        {
            return false;
        }

        if (_defaultCanvasViewApplied)
        {
            return true;
        }

        if (Math.Abs(_canvasScale - 1.0) > 0.01)
        {
            return false;
        }

        if (Math.Abs(_canvasPan.X) > 0.75 || Math.Abs(_canvasPan.Y) > 0.75)
        {
            return false;
        }

        var expectedOffset = ClampOffset(ComputeCenteredOffsetForScale(scrollViewer, viewModel, 1.0), scrollViewer);
        var isDefault =
            Math.Abs(scrollViewer.Offset.X - expectedOffset.X) <= DefaultCanvasViewOffsetTolerancePx
            && Math.Abs(scrollViewer.Offset.Y - expectedOffset.Y) <= DefaultCanvasViewOffsetTolerancePx;
        if (isDefault)
        {
            _defaultCanvasViewApplied = true;
        }

        return isDefault;
    }

    private void RequestCanvasView(PendingCanvasViewMode mode)
    {
        if (mode == PendingCanvasViewMode.None)
        {
            return;
        }

        _defaultCanvasViewApplied = false;
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
    {
        CenterCanvasAtScale(scrollViewer, viewModel, 1.0);
        _defaultCanvasViewApplied = true;
    }

    private void ApplyFitCanvasView(ScrollViewer scrollViewer, VizPanelViewModel viewModel)
    {
        var fitWidth = viewModel.ActivityCanvasWidth;
        var fitHeight = viewModel.ActivityCanvasHeight;
        if (TryGetCanvasContentBounds(viewModel, out var contentBounds))
        {
            fitWidth = Math.Max(1.0, contentBounds.Width);
            fitHeight = Math.Max(1.0, contentBounds.Height);
        }

        var fitScaleX = scrollViewer.Viewport.Width / fitWidth;
        var fitScaleY = scrollViewer.Viewport.Height / fitHeight;
        var fitScale = Math.Min(1.0, Math.Min(fitScaleX, fitScaleY));
        if (!double.IsFinite(fitScale) || fitScale <= 0)
        {
            fitScale = 1.0;
        }

        CenterCanvasAtScale(scrollViewer, viewModel, fitScale);
        InvalidateDefaultCanvasViewState();
    }

    private void CenterCanvasAtScale(ScrollViewer scrollViewer, VizPanelViewModel viewModel, double targetScale)
    {
        _canvasScale = ClampScale(targetScale);
        _canvasPan = default;
        scrollViewer.Offset = default;
        SyncCanvasScaleVisuals();

        var centeredOffset = ComputeCenteredOffsetForScale(scrollViewer, viewModel, _canvasScale);
        ApplyCanvasOffset(centeredOffset, scrollViewer, absorbResidualIntoPan: false);
    }

    private Vector ComputeCenteredOffsetForScale(
        ScrollViewer scrollViewer,
        VizPanelViewModel viewModel,
        double scale)
    {
        var centerBiasX = ComputeCanvasCenterBiasX(viewModel);
        if (TryGetCanvasContentBounds(viewModel, out var contentBounds))
        {
            var centeredX = ((contentBounds.X + (contentBounds.Width / 2.0)) * scale) - (scrollViewer.Viewport.Width / 2.0) + centerBiasX;
            var centeredY = ((contentBounds.Y + (contentBounds.Height / 2.0)) * scale) - (scrollViewer.Viewport.Height / 2.0);
            return new Vector(centeredX, centeredY);
        }

        var scaledWidth = viewModel.ActivityCanvasWidth * scale;
        var scaledHeight = viewModel.ActivityCanvasHeight * scale;
        return new Vector(
            Math.Max(0.0, ((scaledWidth - scrollViewer.Viewport.Width) / 2.0) + centerBiasX),
            Math.Max(0.0, (scaledHeight - scrollViewer.Viewport.Height) / 2.0));
    }

    private static double ComputeCanvasCenterBiasX(VizPanelViewModel viewModel)
    {
        if (!viewModel.ShowMiniActivityChart)
        {
            return 0.0;
        }

        var requestedBias = viewModel.MiniActivityChartOverlayWidth * MiniChartCenterBiasRatio;
        return Math.Min(MiniChartCenterBiasMaxPx, Math.Max(0.0, requestedBias));
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

        InvalidateDefaultCanvasViewState();

        var contentDelta = new Vector(deltaX, deltaY);
        var startPan = _canvasPan;
        var targetPan = ClampPan(startPan + contentDelta, scrollViewer);
        var appliedPan = targetPan - startPan;
        if (Math.Abs(appliedPan.X) > 0.0001 || Math.Abs(appliedPan.Y) > 0.0001)
        {
            SetCanvasPan(targetPan, scrollViewer);
        }

        var remainingContentDelta = contentDelta - appliedPan;
        if (Math.Abs(remainingContentDelta.X) <= 0.0001 && Math.Abs(remainingContentDelta.Y) <= 0.0001)
        {
            return;
        }

        var startOffset = scrollViewer.Offset;
        var targetOffset = ClampOffset(
            new Vector(
                startOffset.X - remainingContentDelta.X,
                startOffset.Y - remainingContentDelta.Y),
            scrollViewer);
        scrollViewer.Offset = targetOffset;

        var appliedOffset = targetOffset - startOffset;
        var leftoverContentDelta = remainingContentDelta + appliedOffset;
        if (Math.Abs(leftoverContentDelta.X) > 0.0001 || Math.Abs(leftoverContentDelta.Y) > 0.0001)
        {
            SetCanvasPan(_canvasPan + leftoverContentDelta, scrollViewer);
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

        InvalidateDefaultCanvasViewState();
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
            viewModel.SetCanvasViewportScale(_canvasScale);
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
        var extent = scrollViewer?.Extent ?? default;
        var limitX = Math.Max(MinPanTranslationLimitPx, Math.Max(viewport.Width, extent.Width) * 2.0);
        var limitY = Math.Max(MinPanTranslationLimitPx, Math.Max(viewport.Height, extent.Height) * 2.0);
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

    private void CaptureNavigationContext()
    {
        _lastNavigationFocusRegionId = ViewModel?.ActiveFocusRegionId;
    }

    private void InvalidateDefaultCanvasViewState()
        => _defaultCanvasViewApplied = false;

    private static bool TryGetCanvasContentBounds(VizPanelViewModel viewModel, out Rect bounds)
    {
        const double LabelApproxGlyphWidthPx = 6.1;
        const double LabelApproxHeightPx = 11.5;
        var minX = double.PositiveInfinity;
        var minY = double.PositiveInfinity;
        var maxX = double.NegativeInfinity;
        var maxY = double.NegativeInfinity;
        var hasGeometry = false;

        foreach (var node in viewModel.CanvasNodes)
        {
            hasGeometry = true;
            var nodePadding = Math.Max(3.0, node.StrokeThickness + 2.4);
            minX = Math.Min(minX, node.Left - nodePadding);
            minY = Math.Min(minY, node.Top - nodePadding);
            maxX = Math.Max(maxX, node.Left + node.Diameter + nodePadding);
            maxY = Math.Max(maxY, node.Top + node.Diameter + nodePadding);

            if (!string.IsNullOrWhiteSpace(node.Label))
            {
                var centerX = node.Left + (node.Diameter / 2.0);
                var centerY = node.Top + (node.Diameter / 2.0);
                var labelWidth = Math.Max(node.Diameter, (node.Label.Length * LabelApproxGlyphWidthPx) + 4.0);
                var labelHeight = Math.Max(LabelApproxHeightPx, node.Diameter * 0.4);
                minX = Math.Min(minX, centerX - (labelWidth / 2.0));
                minY = Math.Min(minY, centerY - (labelHeight / 2.0));
                maxX = Math.Max(maxX, centerX + (labelWidth / 2.0));
                maxY = Math.Max(maxY, centerY + (labelHeight / 2.0));
            }

            if (node.IsPinned)
            {
                minX = Math.Min(minX, node.Left + 1.0);
                minY = Math.Min(minY, node.Top - 1.0);
                maxX = Math.Max(maxX, node.Left + 10.0);
                maxY = Math.Max(maxY, node.Top + 10.0);
            }
        }

        foreach (var edge in viewModel.CanvasEdges)
        {
            hasGeometry = true;
            var edgePadding = Math.Max(2.0, edge.HitTestThickness * 0.5);
            minX = Math.Min(minX, Math.Min(edge.SourceX, Math.Min(edge.ControlX, edge.TargetX)) - edgePadding);
            minY = Math.Min(minY, Math.Min(edge.SourceY, Math.Min(edge.ControlY, edge.TargetY)) - edgePadding);
            maxX = Math.Max(maxX, Math.Max(edge.SourceX, Math.Max(edge.ControlX, edge.TargetX)) + edgePadding);
            maxY = Math.Max(maxY, Math.Max(edge.SourceY, Math.Max(edge.ControlY, edge.TargetY)) + edgePadding);
        }

        if (!hasGeometry || !double.IsFinite(minX) || !double.IsFinite(minY) || !double.IsFinite(maxX) || !double.IsFinite(maxY))
        {
            bounds = default;
            return false;
        }

        minX -= FitContentPaddingPx;
        minY -= FitContentPaddingPx;
        maxX += FitContentPaddingPx;
        maxY += FitContentPaddingPx;
        bounds = new Rect(
            minX,
            minY,
            Math.Max(1.0, maxX - minX),
            Math.Max(1.0, maxY - minY));
        return true;
    }

    private void AttachCanvasTopLevelEvents()
    {
        var topLevel = TopLevel.GetTopLevel(this);
        if (ReferenceEquals(_canvasTopLevel, topLevel))
        {
            return;
        }

        DetachCanvasTopLevelEvents();
        _canvasTopLevel = topLevel;
        if (_canvasTopLevel is not null)
        {
            _canvasTopLevel.SizeChanged += CanvasTopLevelSizeChanged;
        }
    }

    private void DetachCanvasTopLevelEvents()
    {
        if (_canvasTopLevel is not null)
        {
            _canvasTopLevel.SizeChanged -= CanvasTopLevelSizeChanged;
            _canvasTopLevel = null;
        }
    }

    private void CanvasTopLevelSizeChanged(object? sender, SizeChangedEventArgs e)
        => UpdateCanvasViewportHeight();

    private void UpdateCanvasViewportHeight()
    {
        var scrollViewer = ActivityCanvasScrollViewer;
        var topLevel = TopLevel.GetTopLevel(this);
        if (scrollViewer is null || topLevel is null)
        {
            return;
        }

        var targetHeight = topLevel.ClientSize.Height * CanvasViewportHeightRatio;
        if (!double.IsFinite(targetHeight))
        {
            return;
        }

        targetHeight = Math.Clamp(targetHeight, CanvasViewportMinHeightPx, CanvasViewportMaxHeightPx);
        if (Math.Abs(scrollViewer.MaxHeight - targetHeight) > 0.5)
        {
            scrollViewer.MaxHeight = targetHeight;
        }
    }

    private void QueueDefaultCenterAfterNavigation()
    {
        Dispatcher.UIThread.Post(
            () => RequestCanvasView(PendingCanvasViewMode.DefaultCenter),
            DispatcherPriority.Background);
    }
}
