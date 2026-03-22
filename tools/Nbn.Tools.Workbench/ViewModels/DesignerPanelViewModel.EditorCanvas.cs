using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Media;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using Nbn.Shared.Validation;
using Nbn.Tools.Workbench.Services;
using ProtoControl = Nbn.Proto.Control;
using ProtoShardPlanMode = Nbn.Proto.Control.ShardPlanMode;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class DesignerPanelViewModel
{
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
        RefreshRegionView();
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

    private void ClearSelection()
    {
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

    private void FocusEdgeEndpoint(DesignerEdgeViewModel? edge)
    {
        if (edge is null || !edge.CanNavigate || Brain is null)
        {
            return;
        }

        var targetRegionId = edge.NavigationRegionId;
        var targetNeuronId = edge.NavigationNeuronId;
        if (!targetRegionId.HasValue || !targetNeuronId.HasValue)
        {
            return;
        }

        var region = Brain.Regions.FirstOrDefault(candidate => candidate.RegionId == targetRegionId.Value);
        if (region is null || targetNeuronId.Value < 0 || targetNeuronId.Value >= region.NeuronCount)
        {
            Status = "Edge endpoint is no longer available.";
            return;
        }

        var neuron = region.Neurons[targetNeuronId.Value];
        SelectRegion(region);
        EnsureNeuronVisible(neuron);
        SelectNeuron(neuron);
        Status = $"Focused edge endpoint R{region.RegionId} N{neuron.NeuronId}.";
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
        ClearResetConfirmation();
        SetDesignDirty(true);
        ResetValidation();
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

    private void OnSelectedRegionNeuronsChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        QueueRegionRefresh();
    }

    private void QueueRegionRefresh()
    {
        if (_regionRefreshPending)
        {
            return;
        }

        _regionRefreshPending = true;
        Dispatcher.UIThread.Post(() =>
        {
            _regionRefreshPending = false;
            RefreshRegionView();
        }, DispatcherPriority.Background);
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

        var nodeSize = CanvasNodeSize;
        var gap = CanvasNodeGap;
        var visibleIds = VisibleNeurons.Select(neuron => neuron.NeuronId).ToHashSet();
        var internalOut = new Dictionary<int, int>(count);
        var internalIn = new Dictionary<int, int>(count);
        var externalOut = new Dictionary<int, int>(count);
        var externalIn = new Dictionary<int, int>(count);
        foreach (var neuron in VisibleNeurons)
        {
            internalOut[neuron.NeuronId] = 0;
            internalIn[neuron.NeuronId] = 0;
            externalOut[neuron.NeuronId] = 0;
            externalIn[neuron.NeuronId] = 0;
        }

        var selectedRegionId = SelectedRegion?.RegionId ?? -1;
        foreach (var neuron in VisibleNeurons)
        {
            foreach (var axon in neuron.Axons)
            {
                if (axon.TargetRegionId == selectedRegionId && visibleIds.Contains(axon.TargetNeuronId))
                {
                    internalOut[neuron.NeuronId]++;
                    if (internalIn.TryGetValue(axon.TargetNeuronId, out var inbound))
                    {
                        internalIn[axon.TargetNeuronId] = inbound + 1;
                    }
                }
                else
                {
                    externalOut[neuron.NeuronId]++;
                }
            }
        }

        if (Brain is not null && selectedRegionId >= 0)
        {
            foreach (var sourceRegion in Brain.Regions)
            {
                foreach (var sourceNeuron in sourceRegion.Neurons)
                {
                    if (!sourceNeuron.Exists)
                    {
                        continue;
                    }

                    foreach (var axon in sourceNeuron.Axons)
                    {
                        if (axon.TargetRegionId != selectedRegionId || !visibleIds.Contains(axon.TargetNeuronId))
                        {
                            continue;
                        }

                        var isVisibleInternalSource = sourceRegion.RegionId == selectedRegionId
                                                      && visibleIds.Contains(sourceNeuron.NeuronId);
                        if (!isVisibleInternalSource && externalIn.TryGetValue(axon.TargetNeuronId, out var inbound))
                        {
                            externalIn[axon.TargetNeuronId] = inbound + 1;
                        }
                    }
                }
            }
        }

        var ordered = VisibleNeurons
            .OrderByDescending(neuron =>
                (internalOut[neuron.NeuronId] * 3)
                + (internalIn[neuron.NeuronId] * 3)
                + (externalOut[neuron.NeuronId] * 2)
                + (externalIn[neuron.NeuronId] * 2)
                + neuron.AxonCount)
            .ThenBy(neuron => neuron.NeuronId)
            .ToList();

        var ringAssignments = new List<(DesignerNeuronViewModel neuron, int ring, int slot, int slotCount)>(count);
        var index = 0;
        var ring = 0;
        while (index < ordered.Count)
        {
            var ringCapacity = ring == 0 ? 1 : Math.Max(8, ring * 12);
            var slotCount = Math.Min(ringCapacity, ordered.Count - index);
            for (var slot = 0; slot < slotCount; slot++)
            {
                ringAssignments.Add((ordered[index + slot], ring, slot, slotCount));
            }

            index += slotCount;
            ring++;
        }

        var maxRing = ringAssignments.Count == 0 ? 0 : ringAssignments.Max(entry => entry.ring);
        var ringSpacing = nodeSize + (gap * 1.5);
        var radius = (maxRing * ringSpacing) + nodeSize;

        CanvasWidth = Math.Max(MinCanvasWidth, (radius * 2) + (CanvasPadding * 2));
        CanvasHeight = Math.Max(MinCanvasHeight, (radius * 2) + (CanvasPadding * 2));

        var centerX = CanvasWidth / 2.0;
        var centerY = CanvasHeight / 2.0;
        foreach (var assignment in ringAssignments)
        {
            double x;
            double y;
            if (assignment.ring == 0)
            {
                x = centerX - (nodeSize / 2.0);
                y = centerY - (nodeSize / 2.0);
            }
            else
            {
                var angle = ((Math.PI * 2.0) * assignment.slot / Math.Max(1, assignment.slotCount)) - (Math.PI / 2.0);
                var ringRadius = assignment.ring * ringSpacing;
                x = centerX + (Math.Cos(angle) * ringRadius) - (nodeSize / 2.0);
                y = centerY + (Math.Sin(angle) * ringRadius) - (nodeSize / 2.0);
            }

            assignment.neuron.CanvasX = Clamp(x, CanvasPadding, Math.Max(CanvasPadding, CanvasWidth - CanvasPadding - nodeSize));
            assignment.neuron.CanvasY = Clamp(y, CanvasPadding, Math.Max(CanvasPadding, CanvasHeight - CanvasPadding - nodeSize));
        }
    }

    private void RefreshEdges()
    {
        VisibleEdges.Clear();
        EdgeSummary = string.Empty;
        EdgeAnalyticsSummary = "Select a neuron to inspect edge analytics.";

        if (SelectedRegion is null || VisibleNeurons.Count == 0)
        {
            return;
        }

        var source = SelectedNeuron;

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

        var offPageRadius = ComputeOffPageEdgeRadius(start, positions);

        var visibleOutbound = 0;
        var offPageOutbound = 0;
        var visibleInbound = 0;
        var offPageInbound = 0;

        var visibleOutboundTargets = new List<(DesignerAxonViewModel Axon, Point End, bool IsSelected)>();
        var visibleInboundSources = new List<(int SourceRegionId, int SourceNeuronId, Point Start)>();
        var offPageOutboundAxons = new List<(DesignerAxonViewModel Axon, bool IsSelected)>();
        var offPageInboundSources = new List<(int SourceRegionId, int SourceNeuronId)>();

        foreach (var axon in source.Axons)
        {
            if (axon.TargetRegionId != SelectedRegion.RegionId)
            {
                offPageOutbound++;
                offPageOutboundAxons.Add((axon, SelectedAxon is not null
                    && SelectedAxon.TargetRegionId == axon.TargetRegionId
                    && SelectedAxon.TargetNeuronId == axon.TargetNeuronId));
                continue;
            }

            if (!positions.TryGetValue(axon.TargetNeuronId, out var end))
            {
                offPageOutbound++;
                offPageOutboundAxons.Add((axon, SelectedAxon is not null
                    && SelectedAxon.TargetRegionId == axon.TargetRegionId
                    && SelectedAxon.TargetNeuronId == axon.TargetNeuronId));
                continue;
            }

            var isSelected = SelectedAxon is not null
                && SelectedAxon.TargetRegionId == axon.TargetRegionId
                && SelectedAxon.TargetNeuronId == axon.TargetNeuronId;

            visibleOutboundTargets.Add((axon, end, isSelected));
        }

        for (var i = 0; i < visibleOutboundTargets.Count; i++)
        {
            var entry = visibleOutboundTargets[i];
            VisibleEdges.Add(new DesignerEdgeViewModel(
                start,
                entry.End,
                false,
                entry.IsSelected,
                DesignerEdgeKind.OutboundInternal,
                bundleIndex: i,
                bundleCount: visibleOutboundTargets.Count));
            visibleOutbound++;
        }

        if (Brain is not null)
        {
            foreach (var sourceRegion in Brain.Regions)
            {
                foreach (var sourceNeuron in sourceRegion.Neurons)
                {
                    if (!sourceNeuron.Exists)
                    {
                        continue;
                    }

                    if (!sourceNeuron.Axons.Any(axon => axon.TargetRegionId == source.RegionId && axon.TargetNeuronId == source.NeuronId))
                    {
                        continue;
                    }

                    if (sourceRegion.RegionId == source.RegionId && positions.TryGetValue(sourceNeuron.NeuronId, out var inboundStart))
                    {
                        if (sourceNeuron.NeuronId == source.NeuronId)
                        {
                            continue;
                        }

                        visibleInboundSources.Add((sourceRegion.RegionId, sourceNeuron.NeuronId, inboundStart));
                    }
                    else
                    {
                        offPageInbound++;
                        offPageInboundSources.Add((sourceRegion.RegionId, sourceNeuron.NeuronId));
                    }
                }
            }
        }

        for (var i = 0; i < visibleInboundSources.Count; i++)
        {
            var entry = visibleInboundSources[i];
            VisibleEdges.Add(new DesignerEdgeViewModel(
                entry.Start,
                start,
                false,
                false,
                DesignerEdgeKind.InboundInternal,
                bundleIndex: i,
                bundleCount: visibleInboundSources.Count));
            visibleInbound++;
        }

        if (offPageOutboundAxons.Count > 0)
        {
            for (var i = 0; i < offPageOutboundAxons.Count; i++)
            {
                var entry = offPageOutboundAxons[i];
                var angle = BuildArcAngle(i, offPageOutboundAxons.Count, -0.55 * Math.PI, 0.35 * Math.PI);
                var endPoint = ClampToCanvas(
                    new Point(start.X + (Math.Cos(angle) * offPageRadius), start.Y + (Math.Sin(angle) * offPageRadius)),
                    CanvasWidth,
                    CanvasHeight,
                    OffPageEdgeCanvasMargin);
                var labelText = BuildOutboundOffPageLabel(entry.Axon);
                var labelPoint = new Point(endPoint.X + 4, endPoint.Y - 6);
                labelPoint = ClampToCanvas(labelPoint, CanvasWidth, CanvasHeight, OffPageEdgeCanvasMargin);
                VisibleEdges.Add(new DesignerEdgeViewModel(
                    start,
                    endPoint,
                    false,
                    entry.IsSelected,
                    DesignerEdgeKind.OutboundExternal,
                    labelText,
                    labelPoint,
                    i,
                    offPageOutboundAxons.Count,
                    entry.Axon.TargetRegionId,
                    entry.Axon.TargetNeuronId));
            }
        }

        if (offPageInboundSources.Count > 0)
        {
            for (var i = 0; i < offPageInboundSources.Count; i++)
            {
                var entry = offPageInboundSources[i];
                var angle = BuildArcAngle(i, offPageInboundSources.Count, 0.55 * Math.PI, 1.45 * Math.PI);
                var endPoint = ClampToCanvas(
                    new Point(start.X + (Math.Cos(angle) * offPageRadius), start.Y + (Math.Sin(angle) * offPageRadius)),
                    CanvasWidth,
                    CanvasHeight,
                    OffPageEdgeCanvasMargin);
                var labelText = BuildInboundOffPageLabel(entry.SourceRegionId, entry.SourceNeuronId);
                var labelPoint = new Point(endPoint.X - 38, endPoint.Y - 6);
                labelPoint = ClampToCanvas(labelPoint, CanvasWidth, CanvasHeight, OffPageEdgeCanvasMargin);
                VisibleEdges.Add(new DesignerEdgeViewModel(
                    endPoint,
                    start,
                    false,
                    false,
                    DesignerEdgeKind.InboundExternal,
                    labelText,
                    labelPoint,
                    i,
                    offPageInboundSources.Count,
                    entry.SourceRegionId,
                    entry.SourceNeuronId));
            }
        }

        var summary = source.Axons.Count == 0 && visibleInbound == 0 && offPageInbound == 0
            ? "No inbound or outbound axons."
            : $"Out: {visibleOutbound} visible, {offPageOutbound} external/off-page. In: {visibleInbound} visible, {offPageInbound} external/off-page.";
        if (offPageOutbound + offPageInbound > 0)
        {
            summary += " Click off-page labels to jump.";
        }

        EdgeSummary = summary;
        EdgeAnalyticsSummary = BuildEdgeAnalyticsSummary(source, visibleOutbound, offPageOutbound, visibleInbound, offPageInbound);
    }

    private string BuildEdgeAnalyticsSummary(
        DesignerNeuronViewModel source,
        int visibleOutbound,
        int offPageOutbound,
        int visibleInbound,
        int offPageInbound)
    {
        var totalOutbound = visibleOutbound + offPageOutbound;
        var totalInbound = visibleInbound + offPageInbound;
        var totalEdges = totalOutbound + totalInbound;
        if (totalEdges == 0)
        {
            return "Density: idle (0 edges).";
        }

        var externalEdges = offPageOutbound + offPageInbound;
        var externalPct = (externalEdges * 100.0) / totalEdges;
        var pressure = totalEdges switch
        {
            < 8 => "light",
            < 20 => "moderate",
            < 45 => "dense",
            _ => "saturated"
        };

        var regionTotal = Math.Max(1, SelectedRegion?.NeuronCount ?? 0);
        var pageCoveragePct = (VisibleNeurons.Count * 100.0) / regionTotal;
        var dominantTargetGroup = source.Axons
            .GroupBy(axon => axon.TargetRegionId)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key)
            .FirstOrDefault();
        var dominantTarget = dominantTargetGroup is null
            ? "none"
            : $"R{dominantTargetGroup.Key} ({dominantTargetGroup.Count()})";

        return $"Density: {pressure} ({totalEdges} edges). External/off-page: {externalPct:0.#}% ({externalEdges}). "
             + $"Page coverage: {pageCoveragePct:0.#}% ({VisibleNeurons.Count}/{regionTotal}). Dominant target: {dominantTarget}.";
    }

    private static double BuildArcAngle(int index, int total, double startAngle, double endAngle)
    {
        if (total <= 1)
        {
            return (startAngle + endAngle) / 2.0;
        }

        var t = index / (double)(total - 1);
        return startAngle + ((endAngle - startAngle) * t);
    }

    private string BuildOutboundOffPageLabel(DesignerAxonViewModel axon)
    {
        if (SelectedRegion is null || axon.TargetRegionId != SelectedRegion.RegionId)
        {
            return $"-> R{axon.TargetRegionId} N{axon.TargetNeuronId}";
        }

        var page = _regionPageSize == 0 ? 0 : axon.TargetNeuronId / _regionPageSize;
        return $"-> P{page + 1} N{axon.TargetNeuronId}";
    }

    private string BuildInboundOffPageLabel(int sourceRegionId, int sourceNeuronId)
    {
        if (SelectedRegion is null || sourceRegionId != SelectedRegion.RegionId)
        {
            return $"<- R{sourceRegionId} N{sourceNeuronId}";
        }

        var page = _regionPageSize == 0 ? 0 : sourceNeuronId / _regionPageSize;
        return $"<- P{page + 1} N{sourceNeuronId}";
    }

    private double ComputeOffPageEdgeRadius(Point start, IReadOnlyDictionary<int, Point> positions)
    {
        var minRadius = CanvasNodeRadius + OffPageEdgeFallbackRadiusPadding;
        foreach (var center in positions.Values)
        {
            var dx = center.X - start.X;
            var dy = center.Y - start.Y;
            var centerDistance = Math.Sqrt((dx * dx) + (dy * dy));
            var edgeDistance = centerDistance + CanvasNodeRadius + OffPageEdgeOuterMargin;
            if (edgeDistance > minRadius)
            {
                minRadius = edgeDistance;
            }
        }

        var maxRadius = Math.Min(
            Math.Min(start.X - OffPageEdgeCanvasMargin, CanvasWidth - start.X - OffPageEdgeCanvasMargin),
            Math.Min(start.Y - OffPageEdgeCanvasMargin, CanvasHeight - start.Y - OffPageEdgeCanvasMargin));
        var floorRadius = CanvasNodeRadius + OffPageEdgeFallbackRadiusPadding;
        if (maxRadius <= floorRadius)
        {
            return floorRadius;
        }

        if (minRadius > maxRadius)
        {
            return maxRadius;
        }

        return minRadius;
    }

    private static Point ClampToCanvas(Point point, double width, double height, double margin = 10)
    {
        var normalizedMargin = Math.Max(0, margin);
        var minX = Math.Min(normalizedMargin, Math.Max(0, width));
        var minY = Math.Min(normalizedMargin, Math.Max(0, height));
        var maxX = Math.Max(minX, width - normalizedMargin);
        var maxY = Math.Max(minY, height - normalizedMargin);
        return new Point(
            Clamp(point.X, minX, maxX),
            Clamp(point.Y, minY, maxY));
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
        if (_suppressNeuronConstraintEnforcement)
        {
            return;
        }

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

            if ((e.PropertyName == nameof(DesignerNeuronViewModel.ActivationFunctionId)
                 || e.PropertyName == nameof(DesignerNeuronViewModel.ResetFunctionId))
                && SelectedNeuron is not null
                && NormalizeNeuronFunctionConstraints(SelectedNeuron, out var statusMessage)
                && !string.IsNullOrWhiteSpace(statusMessage))
            {
                Status = statusMessage;
            }

            if (e.PropertyName == nameof(DesignerNeuronViewModel.ActivationFunctionId))
            {
                OnPropertyChanged(nameof(SelectedNeuronUsesParamA));
                OnPropertyChanged(nameof(SelectedNeuronUsesParamB));
                OnPropertyChanged(nameof(SelectedActivationDescription));
                OnPropertyChanged(nameof(SelectedNeuronConstraintHint));
            }

            if (e.PropertyName == nameof(DesignerNeuronViewModel.ResetFunctionId))
            {
                OnPropertyChanged(nameof(SelectedResetDescription));
                OnPropertyChanged(nameof(SelectedNeuronConstraintHint));
            }

            if (e.PropertyName == nameof(DesignerNeuronViewModel.AccumulationFunctionId))
            {
                OnPropertyChanged(nameof(SelectedAccumulationDescription));
            }
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
        SaveDefinitionArtifactCommand.RaiseCanExecuteChanged();
        SaveSnapshotArtifactCommand.RaiseCanExecuteChanged();
        LoadDefinitionArtifactCommand.RaiseCanExecuteChanged();
        LoadSnapshotArtifactCommand.RaiseCanExecuteChanged();
        RestoreArtifactBrainCommand.RaiseCanExecuteChanged();
        ResetBrainCommand.RaiseCanExecuteChanged();
        SpawnBrainCommand.RaiseCanExecuteChanged();
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
        OnPropertyChanged(nameof(CanSpawnBrain));
        OnPropertyChanged(nameof(CanResetBrain));
    }

    private void ResetValidation()
    {
        ValidationIssues.Clear();
        ValidationSummary = "Validation not run.";
        _validationHasRun = false;
        _validationPassed = false;
    }

    private static string FormatValidationIssueSummary(NbnValidationResult result)
    {
        if (result.Issues.Count == 0)
        {
            return "Validation failed.";
        }

        var firstIssue = result.Issues[0].ToString();
        return result.Issues.Count == 1
            ? firstIssue
            : $"{firstIssue} (+{result.Issues.Count - 1} more issue(s))";
    }

    private void SetDesignDirty(bool isDirty)
    {
        if (SetProperty(ref _designDirty, isDirty, nameof(IsDesignDirty)))
        {
            if (!isDirty)
            {
                ClearResetConfirmation();
            }
        }
    }

    private void ClearResetConfirmation()
    {
        if (!_resetPending)
        {
            return;
        }

        _resetPending = false;
        OnPropertyChanged(nameof(ResetBrainButtonLabel));
        OnPropertyChanged(nameof(ResetBrainButtonBackground));
        OnPropertyChanged(nameof(ResetBrainButtonForeground));
        OnPropertyChanged(nameof(ResetBrainButtonBorder));
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
}
