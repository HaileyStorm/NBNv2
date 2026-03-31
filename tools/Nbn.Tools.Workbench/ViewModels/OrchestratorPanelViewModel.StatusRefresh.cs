using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Nbn.Proto.Control;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class OrchestratorPanelViewModel
{
    /// <summary>
    /// Forces a SettingsMonitor refresh and updates Orchestrator discovery state from the latest runtime snapshot.
    /// </summary>
    public async Task RefreshSettingsAsync()
    {
        await RefreshAsync(force: true).ConfigureAwait(false);
    }

    private async Task RefreshAsync(bool force)
    {
        if (!await _refreshGate.WaitAsync(0).ConfigureAwait(false))
        {
            return;
        }

        if (!Connections.SettingsConnected)
        {
            if (force)
            {
                StatusMessage = "SettingsMonitor not connected.";
                Connections.SettingsStatus = "Disconnected";
            }

            SetDiscoveryUnavailable();
            _refreshGate.Release();
            return;
        }

        if (force)
        {
            StatusMessage = "Loading settings...";
            Connections.SettingsStatus = "Loading";
        }

        try
        {
            var nodesResponseTask = _client.ListNodesAsync();
            var brainsResponseTask = _client.ListBrainsAsync();
            var workerInventoryResponseTask = _client.ListWorkerInventorySnapshotAsync();
            var settingsResponseTask = _client.ListSettingsAsync();
            var hiveMindStatusTask = _client.GetHiveMindStatusAsync();

            var nodesResponse = await nodesResponseTask.ConfigureAwait(false);
            var brainsResponse = await brainsResponseTask.ConfigureAwait(false);
            var workerInventoryResponse = await workerInventoryResponseTask.ConfigureAwait(false);
            var settingsResponse = await settingsResponseTask.ConfigureAwait(false);
            var hiveMindStatus = await hiveMindStatusTask.ConfigureAwait(false);

            var nodes = nodesResponse?.Nodes?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.NodeStatus>();
            var brains = brainsResponse?.Brains?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainStatus>();
            var controllers = brainsResponse?.Controllers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.BrainControllerStatus>();
            var workerInventory = workerInventoryResponse?.Workers?.ToArray() ?? Array.Empty<Nbn.Proto.Settings.WorkerReadinessCapability>();
            var sortedNodes = nodes
                .OrderByDescending(node => node.LastSeenMs)
                .ThenBy(node => node.LogicalName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var settingsNowMs = ResolveSettingsReferenceTimeMs(
                workerInventoryResponse,
                nodes,
                controllers,
                brains,
                nowMs);
            var workerNowMs = ResolveWorkerReferenceTimeMs(workerInventoryResponse, settingsNowMs);
            var actorRowsResult = await BuildActorRowsAsync(controllers, sortedNodes, brains, settingsNowMs).ConfigureAwait(false);
            var workerEndpointState = BuildWorkerEndpointState(
                sortedNodes,
                workerInventory,
                actorRowsResult.WorkerBrainHints,
                actorRowsResult.WorkerBrainBackends,
                workerNowMs);
            var systemLoadState = BuildSystemLoadState(workerInventory, hiveMindStatus, workerNowMs);
            var settings = settingsResponse?.Settings?
                .Select(entry => new SettingItem(
                    entry.Key ?? string.Empty,
                    entry.Value ?? string.Empty,
                    FormatUpdated(entry.UpdatedMs)))
                .ToList() ?? new List<SettingItem>();
            var discoveredServiceEndpoints = BuildServiceEndpointLookup(settings);

            _dispatcher.Post(() =>
            {
                Nodes.Clear();
                WorkerEndpoints.Clear();
                WorkerNodeGroups.Clear();
                Actors.Clear();
                ActorNodeGroups.Clear();
                foreach (var node in sortedNodes)
                {
                    var isFresh = IsFresh(node.LastSeenMs, settingsNowMs);
                    var isAlive = node.IsAlive && isFresh;
                    var seen = DateTimeOffset.FromUnixTimeMilliseconds((long)node.LastSeenMs).ToLocalTime();
                    Nodes.Add(new NodeStatusItem(
                        node.LogicalName ?? string.Empty,
                        node.Address ?? string.Empty,
                        node.RootActorName ?? string.Empty,
                        seen.ToString("g"),
                        isAlive ? "online" : "offline"));
                }

                foreach (var workerRow in workerEndpointState.Rows)
                {
                    WorkerEndpoints.Add(workerRow);
                }

                foreach (var workerGroup in workerEndpointState.Groups)
                {
                    WorkerNodeGroups.Add(workerGroup);
                }

                foreach (var actorRow in actorRowsResult.Rows)
                {
                    Actors.Add(actorRow);
                }

                foreach (var actorGroup in actorRowsResult.Groups)
                {
                    ActorNodeGroups.Add(actorGroup);
                }

                foreach (var entry in settings)
                {
                    var existing = Settings.FirstOrDefault(item => string.Equals(item.Key, entry.Key, StringComparison.OrdinalIgnoreCase));
                    if (existing is null)
                    {
                        Settings.Add(new SettingEntryViewModel(entry.Key, entry.Value, FormatUpdated(entry.Updated)));
                    }
                    else
                    {
                        existing.UpdateFromServer(entry.Value, FormatUpdated(entry.Updated), preserveEdits: true);
                    }

                    TryApplyWorkerPolicySetting(entry);
                }

                var hiveMindFromSettings = ApplyServiceEndpointSettingsToConnections(settings);
                if (!hiveMindFromSettings)
                {
                    UpdateHiveMindEndpoint(nodes, settingsNowMs);
                }

                WorkerEndpointSummary = workerEndpointState.SummaryText;
                SystemLoadResourceSummary = systemLoadState.ResourceSummary;
                SystemLoadPressureSummary = systemLoadState.PressureSummary;
                SystemLoadTickSummary = systemLoadState.TickSummary;
                SystemLoadHealthSummary = systemLoadState.HealthSummary;
                SystemLoadSparklinePathData = systemLoadState.SparklinePathData;
                SystemLoadSparklineStroke = systemLoadState.SparklineStroke;
                Trim(Nodes);
                Trim(WorkerEndpoints);
                Trim(WorkerNodeGroups);
                Trim(Actors);
                Trim(ActorNodeGroups);
                Trim(Settings);
            });

            UpdateConnectionStatusesFromNodes(nodes, settingsNowMs, workerEndpointState, discoveredServiceEndpoints);

            var controllerMap = controllers
                .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                .ToDictionary(
                    entry => entry.BrainId!.ToGuid(),
                    entry =>
                    {
                        var controllerAlive = entry.IsAlive && IsFresh(entry.LastSeenMs, settingsNowMs);
                        return (entry, controllerAlive);
                    });

            var brainEntries = brains.Select(entry =>
            {
                var brainId = entry.BrainId?.ToGuid() ?? Guid.Empty;
                var alive = controllerMap.TryGetValue(brainId, out var controller) && controller.Item2;
                var spawnedRecently = IsSpawnRecent(entry.SpawnedMs, settingsNowMs);
                var item = new BrainListItem(brainId, entry.State ?? string.Empty, alive);
                return (item, spawnedRecently);
            }).Where(entry => entry.item.BrainId != Guid.Empty).ToList();

            var brainListAll = brainEntries
                .Select(entry => entry.item)
                .ToList();
            RecordBrainTerminations(brainListAll);
            var brainList = brainEntries
                .Where(entry => !string.Equals(entry.item.State, "Dead", StringComparison.OrdinalIgnoreCase))
                .Where(entry => entry.item.ControllerAlive || entry.spawnedRecently)
                .Select(entry => entry.item)
                .ToList();
            _brainsUpdated?.Invoke(brainList);

            if (force)
            {
                StatusMessage = "Settings loaded.";
                Connections.SettingsStatus = "Ready";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"Settings load failed: {ex.Message}";
            Connections.SettingsStatus = "Error";
            SetDiscoveryUnavailable();
            WorkbenchLog.Warn($"Settings refresh failed: {ex.Message}");
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private async Task StartAutoRefreshAsync()
    {
        while (!_refreshCts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_autoRefreshInterval, _refreshCts.Token).ConfigureAwait(false);
                await RefreshAsync(force: false).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private bool TryApplyWorkerPolicySetting(SettingItem item)
    {
        if (string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerCapabilityRefreshSecondsServerValue),
                ref _workerCapabilityRefreshSecondsServerValue,
                ref _workerCapabilityRefreshSecondsDirty,
                value => WorkerCapabilityRefreshSecondsText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerPressureRebalanceWindowServerValue),
                ref _workerPressureRebalanceWindowServerValue,
                ref _workerPressureRebalanceWindowDirty,
                value => WorkerPressureRebalanceWindowText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerPressureViolationRatioServerValue),
                ref _workerPressureViolationRatioServerValue,
                ref _workerPressureViolationRatioDirty,
                value => WorkerPressureViolationRatioText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerPressureTolerancePercentServerValue),
                ref _workerPressureTolerancePercentServerValue,
                ref _workerPressureTolerancePercentDirty,
                value => WorkerPressureTolerancePercentText = value);
            return true;
        }

        if (string.Equals(item.Key, WorkerCapabilitySettingsKeys.RegionShardGpuNeuronThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            ApplyWorkerPolicyServerValue(
                NormalizeWorkerPolicyValue(item.Value, _workerRegionShardGpuNeuronThresholdServerValue),
                ref _workerRegionShardGpuNeuronThresholdServerValue,
                ref _workerRegionShardGpuNeuronThresholdDirty,
                value => WorkerRegionShardGpuNeuronThresholdText = value);
            return true;
        }

        return false;
    }

    private async Task ApplyWorkerPolicyAsync()
    {
        if (!Connections.SettingsConnected)
        {
            WorkerPolicyStatus = "SettingsMonitor not connected.";
            return;
        }

        if (!TryParseNonNegativeInt(WorkerCapabilityRefreshSecondsText, out var refreshSeconds))
        {
            WorkerPolicyStatus = "Invalid capability refresh seconds.";
            return;
        }

        if (!TryParsePositiveInt(WorkerPressureRebalanceWindowText, out var window))
        {
            WorkerPolicyStatus = "Invalid pressure window.";
            return;
        }

        if (!TryParseNonNegativeDouble(WorkerPressureViolationRatioText, out var violationRatio))
        {
            WorkerPolicyStatus = "Invalid pressure violation ratio.";
            return;
        }

        if (!TryParseNonNegativeDouble(WorkerPressureTolerancePercentText, out var tolerancePercent))
        {
            WorkerPolicyStatus = "Invalid pressure tolerance percent.";
            return;
        }

        if (!TryParseRegionShardGpuNeuronThreshold(WorkerRegionShardGpuNeuronThresholdText, out var gpuNeuronThreshold))
        {
            WorkerPolicyStatus = "Invalid GPU neuron threshold.";
            return;
        }

        var desired = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey] = refreshSeconds.ToString(CultureInfo.InvariantCulture),
            [WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey] = Math.Max(1, window).ToString(CultureInfo.InvariantCulture),
            [WorkerCapabilitySettingsKeys.PressureViolationRatioKey] = WorkerCapabilityMath.FormatRatio(Math.Clamp(violationRatio, 0d, 1d)),
            [WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey] = WorkerCapabilityMath.FormatRatio(Math.Max(0d, tolerancePercent)),
            [WorkerCapabilitySettingsKeys.RegionShardGpuNeuronThresholdKey] = Math.Max(1, gpuNeuronThreshold).ToString(CultureInfo.InvariantCulture)
        };

        var dirtyKeys = desired
            .Where(entry => !string.Equals(GetWorkerPolicyServerValue(entry.Key), entry.Value, StringComparison.Ordinal))
            .ToArray();
        if (dirtyKeys.Length == 0)
        {
            WorkerPolicyStatus = "No worker policy changes.";
            return;
        }

        WorkerPolicyStatus = $"Applying {dirtyKeys.Length} worker policy setting(s)...";
        foreach (var entry in dirtyKeys)
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            _dispatcher.Post(() =>
            {
                MarkWorkerPolicyApplied(
                    result.Key ?? entry.Key,
                    result.Value ?? entry.Value);
            });
        }

        WorkerPolicyStatus = "Worker policy updated.";
    }

    private static void ApplyWorkerPolicyServerValue(
        string normalizedValue,
        ref string serverValue,
        ref bool dirty,
        Action<string> applyValue)
    {
        serverValue = normalizedValue;
        if (!dirty)
        {
            applyValue(normalizedValue);
        }
    }

    private void MarkWorkerPolicyApplied(string key, string value)
    {
        var normalized = NormalizeWorkerPolicyValue(value);
        if (string.Equals(key, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerCapabilityRefreshSecondsServerValue = normalized;
            _workerCapabilityRefreshSecondsDirty = false;
            WorkerCapabilityRefreshSecondsText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerPressureRebalanceWindowServerValue = normalized;
            _workerPressureRebalanceWindowDirty = false;
            WorkerPressureRebalanceWindowText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerPressureViolationRatioServerValue = normalized;
            _workerPressureViolationRatioDirty = false;
            WorkerPressureViolationRatioText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerPressureTolerancePercentServerValue = normalized;
            _workerPressureTolerancePercentDirty = false;
            WorkerPressureTolerancePercentText = normalized;
            return;
        }

        if (string.Equals(key, WorkerCapabilitySettingsKeys.RegionShardGpuNeuronThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            _workerRegionShardGpuNeuronThresholdServerValue = normalized;
            _workerRegionShardGpuNeuronThresholdDirty = false;
            WorkerRegionShardGpuNeuronThresholdText = normalized;
        }
    }

    private string GetWorkerPolicyServerValue(string key)
        => key switch
        {
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, StringComparison.OrdinalIgnoreCase)
                => _workerCapabilityRefreshSecondsServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, StringComparison.OrdinalIgnoreCase)
                => _workerPressureRebalanceWindowServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.PressureViolationRatioKey, StringComparison.OrdinalIgnoreCase)
                => _workerPressureViolationRatioServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, StringComparison.OrdinalIgnoreCase)
                => _workerPressureTolerancePercentServerValue,
            var value when string.Equals(value, WorkerCapabilitySettingsKeys.RegionShardGpuNeuronThresholdKey, StringComparison.OrdinalIgnoreCase)
                => _workerRegionShardGpuNeuronThresholdServerValue,
            _ => string.Empty
        };

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxRows)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }

    private static string FormatUpdated(long updatedMs)
    {
        if (updatedMs <= 0)
        {
            return string.Empty;
        }

        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(updatedMs).ToLocalTime();
        return timestamp.ToString("g");
    }

    private static string FormatUpdated(ulong updatedMs)
        => FormatUpdated((long)updatedMs);

    private static string FormatUpdated(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        if (long.TryParse(value, out var parsed))
        {
            return FormatUpdated(parsed);
        }

        return value;
    }

    private void SetDiscoveryUnavailable()
    {
        _dispatcher.Post(() =>
        {
            Nodes.Clear();
            WorkerEndpoints.Clear();
            WorkerNodeGroups.Clear();
            Actors.Clear();
            ActorNodeGroups.Clear();
            WorkerEndpointSummary = "No active nodes.";
            SystemLoadResourceSummary = "Resource usage: awaiting worker telemetry.";
            SystemLoadPressureSummary = "Pressure: awaiting HiveMind telemetry.";
            SystemLoadTickSummary = "Tick health: awaiting HiveMind status.";
            SystemLoadHealthSummary = "Health: awaiting HiveMind status.";
            SystemLoadSparklinePathData = WorkbenchSystemLoadSummaryBuilder.EmptySparklinePathData;
            SystemLoadSparklineStroke = WorkbenchSystemLoadSummaryBuilder.NeutralSparklineStroke;
            _systemLoadHistory.Clear();

            Connections.HiveMindDiscoverable = false;
            Connections.HiveMindStatus = "Offline";
            Connections.HiveMindEndpointDisplay = "Missing";

            Connections.IoDiscoverable = false;
            Connections.IoStatus = "Offline";
            Connections.IoEndpointDisplay = "Missing";

            Connections.ReproDiscoverable = false;
            Connections.ReproStatus = "Offline";
            Connections.ReproEndpointDisplay = "Missing";

            Connections.SpeciationDiscoverable = false;
            Connections.SpeciationStatus = "Offline";
            Connections.SpeciationEndpointDisplay = "Missing";

            Connections.WorkerDiscoverable = false;
            Connections.WorkerStatus = "Offline";
            Connections.WorkerEndpointDisplay = "Missing";

            Connections.ObsDiscoverable = false;
            Connections.ObsStatus = "Offline";
            Connections.ObsEndpointDisplay = "Missing";
            RefreshEndpointRows();
        });
    }

    private static IReadOnlyDictionary<string, ServiceEndpoint> BuildServiceEndpointLookup(IEnumerable<SettingItem> settings)
    {
        var lookup = new Dictionary<string, ServiceEndpoint>(StringComparer.OrdinalIgnoreCase);
        foreach (var setting in settings)
        {
            if (string.IsNullOrWhiteSpace(setting.Key)
                || !ServiceEndpointSettings.IsKnownKey(setting.Key)
                || !ServiceEndpointSettings.TryParseValue(setting.Value, out var endpoint))
            {
                continue;
            }

            lookup[setting.Key] = endpoint;
        }

        return lookup;
    }

    private static string ResolveDiscoveredActorName(
        IReadOnlyDictionary<string, ServiceEndpoint> discoveredServiceEndpoints,
        string serviceEndpointKey,
        string fallbackActorName)
    {
        if (discoveredServiceEndpoints.TryGetValue(serviceEndpointKey, out var endpoint)
            && !string.IsNullOrWhiteSpace(endpoint.ActorName))
        {
            return endpoint.ActorName.Trim();
        }

        return fallbackActorName?.Trim() ?? string.Empty;
    }

    private static string ResolveEndpointDisplay(
        IReadOnlyDictionary<string, ServiceEndpoint> discoveredServiceEndpoints,
        string serviceEndpointKey,
        string fallbackActorName)
    {
        if (discoveredServiceEndpoints.TryGetValue(serviceEndpointKey, out var endpoint))
        {
            return FormatEndpointDisplay(endpoint.HostPort, endpoint.ActorName);
        }

        return string.IsNullOrWhiteSpace(fallbackActorName)
            ? "Missing"
            : fallbackActorName.Trim();
    }

    private static string[] BuildActorCandidates(params string?[] names)
    {
        var candidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                continue;
            }

            candidates.Add(name.Trim());
        }

        return candidates.ToArray();
    }

    private static bool IsAnyFreshNodeMatch(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        long nowMs,
        params string[] actorCandidates)
    {
        if (actorCandidates.Length == 0)
        {
            return false;
        }

        foreach (var node in nodes)
        {
            if (!node.IsAlive || !IsFresh(node.LastSeenMs, nowMs))
            {
                continue;
            }

            foreach (var actor in actorCandidates)
            {
                if (string.Equals(node.RootActorName, actor, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static string FormatEndpointDisplay(string? hostPort, string? actorName)
    {
        var hostPortToken = string.IsNullOrWhiteSpace(hostPort) ? "?" : hostPort.Trim();
        var actorToken = string.IsNullOrWhiteSpace(actorName) ? "?" : actorName.Trim();
        return $"{hostPortToken}/{actorToken}";
    }

    private bool ApplyServiceEndpointSettingsToConnections(IEnumerable<SettingItem> settings)
    {
        var hiveMindApplied = false;
        foreach (var setting in settings)
        {
            if (!TryApplyServiceEndpointSetting(setting))
            {
                continue;
            }

            if (string.Equals(setting.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.OrdinalIgnoreCase))
            {
                hiveMindApplied = true;
            }
        }

        return hiveMindApplied;
    }

    private bool TryApplyServiceEndpointSetting(SettingItem item)
    {
        if (string.IsNullOrWhiteSpace(item.Key)
            || !ServiceEndpointSettings.IsKnownKey(item.Key)
            || !ServiceEndpointSettings.TryParseValue(item.Value, out var endpoint)
            || !TryParseHostPort(endpoint.HostPort, out var host, out var port))
        {
            return false;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.HiveMindKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.HiveMindHost = host;
            Connections.HiveMindPortText = port.ToString();
            Connections.HiveMindName = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.IoGatewayKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.IoHost = host;
            Connections.IoPortText = port.ToString();
            Connections.IoGateway = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.ReproductionManagerKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.ReproHost = host;
            Connections.ReproPortText = port.ToString();
            Connections.ReproManager = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.SpeciationManagerKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.SpeciationHost = host;
            Connections.SpeciationPortText = port.ToString();
            Connections.SpeciationManager = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.WorkerNodeKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.WorkerHost = host;
            Connections.WorkerPortText = port.ToString();
            Connections.WorkerRootName = endpoint.ActorName;
            return true;
        }

        if (string.Equals(item.Key, ServiceEndpointSettings.ObservabilityKey, StringComparison.OrdinalIgnoreCase))
        {
            Connections.ObsHost = host;
            Connections.ObsPortText = port.ToString();
            Connections.DebugHub = endpoint.ActorName;
            return true;
        }

        return false;
    }

    private void UpdateHiveMindEndpoint(IEnumerable<Nbn.Proto.Settings.NodeStatus> nodes, long nowMs)
    {
        var match = nodes
            .Where(node => string.Equals(node.RootActorName, Connections.HiveMindName, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(node => IsFresh(node.LastSeenMs, nowMs))
            .ThenByDescending(node => node.LastSeenMs)
            .FirstOrDefault();

        if (match is null)
        {
            return;
        }

        if (!TryParseHostPort(match.Address, out var host, out var port))
        {
            return;
        }

        Connections.HiveMindHost = host;
        Connections.HiveMindPortText = port.ToString();
    }

    private static bool TryParseHostPort(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;

        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        var colonIndex = trimmed.LastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= trimmed.Length - 1)
        {
            return false;
        }

        var hostPart = trimmed[..colonIndex];
        var portPart = trimmed[(colonIndex + 1)..];

        if (hostPart.StartsWith("[", StringComparison.Ordinal) && hostPart.EndsWith("]", StringComparison.Ordinal))
        {
            hostPart = hostPart[1..^1];
        }

        if (!int.TryParse(portPart, out port))
        {
            return false;
        }

        host = hostPart;
        return !string.IsNullOrWhiteSpace(host);
    }

    private static bool TryParsePort(string value, out int port)
        => int.TryParse(value, out port) && port > 0 && port < 65536;

    private static bool TryParsePercent(string? value, out int percent)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out percent)
           && percent >= 0
           && percent <= 100;

    private static bool TryParseNonNegativeInt(string value, out int parsed)
        => int.TryParse(value, out parsed) && parsed >= 0;

    private static bool TryParsePositiveInt(string value, out int parsed)
        => int.TryParse(value, out parsed) && parsed > 0;

    private static bool TryParseNonNegativeDouble(string value, out double parsed)
        => double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out parsed)
           && double.IsFinite(parsed)
           && parsed >= 0d;

    private static bool TryParseRegionShardGpuNeuronThreshold(string value, out int parsed)
        => int.TryParse(value, out parsed) && parsed > 0;

    private static string NormalizeWorkerPolicyValue(string? value, string? fallback = null)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback ?? string.Empty;
        }

        return value.Trim();
    }

    private void UpdateConnectionStatusesFromNodes(
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        long nowMs,
        WorkerEndpointState workerEndpointState,
        IReadOnlyDictionary<string, ServiceEndpoint> discoveredServiceEndpoints)
    {
        var hiveActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.HiveMindKey,
            Connections.HiveMindName);
        var ioActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.IoGatewayKey,
            Connections.IoGateway);
        var reproActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ReproductionManagerKey,
            Connections.ReproManager);
        var speciationActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.SpeciationManagerKey,
            Connections.SpeciationManager);
        var obsActorName = ResolveDiscoveredActorName(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ObservabilityKey,
            Connections.DebugHub);
        var obsCandidates = BuildActorCandidates(obsActorName, Connections.DebugHub, Connections.VizHub);

        var hiveAlive = IsAnyFreshNodeMatch(nodes, nowMs, hiveActorName);
        var ioAlive = IsAnyFreshNodeMatch(nodes, nowMs, ioActorName);
        var reproAlive = IsAnyFreshNodeMatch(nodes, nowMs, reproActorName);
        var speciationAlive = IsAnyFreshNodeMatch(nodes, nowMs, speciationActorName);
        var obsAlive = IsAnyFreshNodeMatch(nodes, nowMs, obsCandidates);

        var hiveEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.HiveMindKey,
            hiveActorName);
        var ioEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.IoGatewayKey,
            ioActorName);
        var reproEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ReproductionManagerKey,
            reproActorName);
        var speciationEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.SpeciationManagerKey,
            speciationActorName);
        var obsEndpointDisplay = ResolveEndpointDisplay(
            discoveredServiceEndpoints,
            ServiceEndpointSettings.ObservabilityKey,
            obsActorName);
        var workerEndpointDisplay = workerEndpointState.Rows.Count > 0
            ? workerEndpointState.SummaryText
            : "Missing";

        _dispatcher.Post(() =>
        {
            Connections.HiveMindDiscoverable = hiveAlive;
            Connections.HiveMindStatus = hiveAlive ? "Online" : "Offline";
            Connections.HiveMindEndpointDisplay = hiveEndpointDisplay;

            Connections.IoDiscoverable = ioAlive;
            Connections.IoStatus = ioAlive ? "Online" : "Offline";
            Connections.IoEndpointDisplay = ioEndpointDisplay;

            Connections.ReproDiscoverable = reproAlive;
            Connections.ReproStatus = reproAlive ? "Online" : "Offline";
            Connections.ReproEndpointDisplay = reproEndpointDisplay;

            Connections.SpeciationDiscoverable = speciationAlive;
            Connections.SpeciationStatus = speciationAlive ? "Online" : "Offline";
            Connections.SpeciationEndpointDisplay = speciationEndpointDisplay;

            Connections.WorkerDiscoverable = workerEndpointState.ActiveCount > 0 || workerEndpointState.LimitedCount > 0;
            Connections.WorkerStatus = workerEndpointState.Rows.Count > 0
                ? workerEndpointState.SummaryText
                : "Offline";
            Connections.WorkerEndpointDisplay = workerEndpointDisplay;

            Connections.ObsDiscoverable = obsAlive;
            Connections.ObsStatus = obsAlive ? "Online" : "Offline";
            Connections.ObsEndpointDisplay = obsEndpointDisplay;
            RefreshEndpointRows();
        });
    }

    private void RefreshEndpointRows()
    {
        Endpoints.Clear();
        Endpoints.Add(CreateEndpointStatusItem("IO Gateway", Connections.IoEndpointDisplay, Connections.IsIoServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("Observability", Connections.ObsEndpointDisplay, Connections.IsObsServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("Reproduction", Connections.ReproEndpointDisplay, Connections.IsReproServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("Speciation", Connections.SpeciationEndpointDisplay, Connections.IsSpeciationServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("SettingsMonitor", BuildSettingsEndpointDisplay(), Connections.IsSettingsServiceReady()));
        Endpoints.Add(CreateEndpointStatusItem("HiveMind", Connections.HiveMindEndpointDisplay, Connections.IsHiveMindServiceReady()));
    }

    private static EndpointStatusItem CreateEndpointStatusItem(string serviceName, string endpointDisplay, bool discoverable)
    {
        var normalizedEndpointDisplay = string.IsNullOrWhiteSpace(endpointDisplay) ? "Missing" : endpointDisplay.Trim();
        return new EndpointStatusItem(
            serviceName,
            normalizedEndpointDisplay,
            discoverable ? "online" : "offline");
    }

    private void OnConnectionsPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (string.Equals(args.PropertyName, nameof(ConnectionViewModel.SettingsHost), StringComparison.Ordinal)
            || string.Equals(args.PropertyName, nameof(ConnectionViewModel.SettingsPortText), StringComparison.Ordinal)
            || string.Equals(args.PropertyName, nameof(ConnectionViewModel.SettingsName), StringComparison.Ordinal))
        {
            SeedPullSettingsSourceFromConnections(force: false);
        }

        if (string.IsNullOrWhiteSpace(args.PropertyName)
            || !EndpointRefreshTriggerProperties.Contains(args.PropertyName))
        {
            return;
        }

        _dispatcher.Post(RefreshEndpointRows);
    }

    private string BuildSettingsEndpointDisplay()
    {
        var host = Connections.SettingsHost?.Trim() ?? string.Empty;
        var port = Connections.SettingsPortText?.Trim() ?? string.Empty;
        var actorName = Connections.SettingsName?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(host) && string.IsNullOrWhiteSpace(port) && string.IsNullOrWhiteSpace(actorName))
        {
            return "Missing";
        }

        var address = host;
        if (!string.IsNullOrWhiteSpace(port))
        {
            address = string.IsNullOrWhiteSpace(address) ? port : $"{address}:{port}";
        }

        if (!string.IsNullOrWhiteSpace(actorName))
        {
            address = string.IsNullOrWhiteSpace(address) ? actorName : $"{address}/{actorName}";
        }

        return string.IsNullOrWhiteSpace(address) ? "Missing" : address;
    }

    private void RecordBrainTerminations(IReadOnlyList<BrainListItem> current)
    {
        var seen = new HashSet<Guid>();
        foreach (var brain in current)
        {
            seen.Add(brain.BrainId);
            if (_lastBrains.TryGetValue(brain.BrainId, out var previous))
            {
                if (previous.ControllerAlive && !brain.ControllerAlive)
                {
                    AddTermination(new BrainTerminatedItem(
                        DateTimeOffset.UtcNow,
                        brain.BrainId.ToString("D"),
                        "Controller offline",
                        0,
                        0));
                }
                else if (!string.Equals(previous.State, "Dead", StringComparison.OrdinalIgnoreCase)
                         && string.Equals(brain.State, "Dead", StringComparison.OrdinalIgnoreCase))
                {
                    AddTermination(new BrainTerminatedItem(
                        DateTimeOffset.UtcNow,
                        brain.BrainId.ToString("D"),
                        "State dead",
                        0,
                        0));
                }
            }

            _lastBrains[brain.BrainId] = brain;
        }

        var missing = _lastBrains.Keys.Where(id => !seen.Contains(id)).ToList();
        foreach (var brainId in missing)
        {
            if (!_lastBrains.TryGetValue(brainId, out var previous))
            {
                continue;
            }

            AddTermination(new BrainTerminatedItem(
                DateTimeOffset.UtcNow,
                brainId.ToString("D"),
                "Missing from registry",
                0,
                0));
            _lastBrains.Remove(brainId);
        }
    }

    private async Task<HostedActorRowsResult> BuildActorRowsAsync(
        IReadOnlyList<Nbn.Proto.Settings.BrainControllerStatus> controllers,
        IReadOnlyList<Nbn.Proto.Settings.NodeStatus> nodes,
        IReadOnlyList<Nbn.Proto.Settings.BrainStatus> brains,
        long nowMs)
    {
        var rows = new List<HostedActorDisplayEntry>();
        var workerBrainHints = new Dictionary<Guid, HashSet<Guid>>();
        var workerBackendProbeRequests = new Dictionary<(Guid NodeId, Guid BrainId), Dictionary<(int RegionId, int ShardIndex), WorkerBrainBackendProbeRequest>>();
        var dedupe = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var nodeById = nodes
            .Where(entry => entry.NodeId is not null && entry.NodeId.TryToGuid(out _))
            .ToDictionary(entry => entry.NodeId!.ToGuid(), entry => entry);

        void AddActorRow(
            Guid brainId,
            string actorKind,
            string actorPid,
            string hostLabel,
            string address,
            long lastSeenMs,
            bool isOnline,
            bool hostIsWorker = false,
            uint regionId = 0,
            uint shardIndex = 0)
        {
            var dedupeKey = BuildHostedActorKey(brainId, actorPid, actorKind, regionId, shardIndex);
            if (!dedupe.Add(dedupeKey))
            {
                return;
            }

            var brainToken = AbbreviateHostedActorBrainId(brainId);
            var kindToken = actorKind;
            if (string.Equals(actorKind, "RegionShard", StringComparison.Ordinal))
            {
                kindToken = $"{actorKind} r{regionId} s{shardIndex}";
            }

            var seen = lastSeenMs > 0 ? FormatUpdated(lastSeenMs) : string.Empty;
            var logicalName = $"{hostLabel} - brain {brainToken} {kindToken}";
            var rootActor = string.IsNullOrWhiteSpace(actorPid) ? actorKind : actorPid;
            rows.Add(new HostedActorDisplayEntry(
                isOnline && hostIsWorker,
                isOnline,
                lastSeenMs,
                WorkbenchWorkerHostGrouping.ResolveHostGroupKey(address, hostLabel, null),
                WorkbenchWorkerHostGrouping.ResolveHostDisplayName(address, hostLabel),
                new NodeStatusItem(
                    logicalName,
                    address,
                    rootActor,
                    seen,
                    isOnline ? "online" : "offline")));
        }

        void AddWorkerBrainHint(Guid nodeId, Guid brainId)
        {
            if (nodeId == Guid.Empty || brainId == Guid.Empty)
            {
                return;
            }

            if (!workerBrainHints.TryGetValue(nodeId, out var brainSet))
            {
                brainSet = new HashSet<Guid>();
                workerBrainHints[nodeId] = brainSet;
            }

            brainSet.Add(brainId);
        }

        void AddWorkerBackendProbe(
            Guid nodeId,
            Guid brainId,
            string workerAddress,
            string workerRootActor,
            uint regionId,
            uint shardIndex)
        {
            if (nodeId == Guid.Empty
                || brainId == Guid.Empty
                || regionId == NbnConstants.InputRegionId
                || regionId == NbnConstants.OutputRegionId
                || string.IsNullOrWhiteSpace(workerAddress)
                || string.IsNullOrWhiteSpace(workerRootActor))
            {
                return;
            }

            var key = (nodeId, brainId);
            if (!workerBackendProbeRequests.TryGetValue(key, out var probesForBrain))
            {
                probesForBrain = new Dictionary<(int RegionId, int ShardIndex), WorkerBrainBackendProbeRequest>();
                workerBackendProbeRequests[key] = probesForBrain;
            }

            var probe = new WorkerBrainBackendProbeRequest(
                nodeId,
                brainId,
                workerAddress.Trim(),
                workerRootActor.Trim(),
                checked((int)regionId),
                checked((int)shardIndex));
            probesForBrain.TryAdd((probe.RegionId, probe.ShardIndex), probe);
        }

        foreach (var controller in controllers
                     .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
                     .OrderByDescending(entry => entry.LastSeenMs)
                     .ThenBy(entry => entry.ActorName ?? string.Empty, StringComparer.OrdinalIgnoreCase))
        {
            var brainId = controller.BrainId!.ToGuid();
            var hostLabel = "controller node";
            var address = string.Empty;
            var hostSeenMs = (long)controller.LastSeenMs;
            var hostIsWorker = false;
            var workerHostNodeId = Guid.Empty;
            if (controller.NodeId is not null
                && controller.NodeId.TryToGuid(out var nodeId)
                && nodeById.TryGetValue(nodeId, out var node))
            {
                hostLabel = string.IsNullOrWhiteSpace(node.LogicalName) ? "controller node" : node.LogicalName!;
                address = node.Address ?? string.Empty;
                hostSeenMs = (long)node.LastSeenMs;
                hostIsWorker = IsWorkerHostCandidate(node);
                if (hostIsWorker)
                {
                    workerHostNodeId = nodeId;
                }
            }

            var actorPid = controller.ActorName?.Trim() ?? string.Empty;
            var isOnline = controller.IsAlive && IsFresh(controller.LastSeenMs, nowMs);
            if (hostIsWorker && isOnline && workerHostNodeId != Guid.Empty)
            {
                AddWorkerBrainHint(workerHostNodeId, brainId);
            }

            AddActorRow(
                brainId,
                actorKind: "Controller",
                actorPid: actorPid,
                hostLabel: hostLabel,
                address: address,
                lastSeenMs: hostSeenMs,
                isOnline: isOnline,
                hostIsWorker: hostIsWorker);
        }

        var activeBrainIds = brains
            .Where(entry => entry.BrainId is not null && entry.BrainId.TryToGuid(out _))
            .Select(entry => entry.BrainId!.ToGuid())
            .Where(id => id != Guid.Empty)
            .Distinct()
            .ToArray();
        if (activeBrainIds.Length == 0)
        {
            return new HostedActorRowsResult(
                OrderHostedActorRows(rows),
                GroupHostedActorRows(rows),
                workerBrainHints,
                new Dictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint>());
        }

        var lifecycleTasks = activeBrainIds.Select(async brainId =>
        {
            var lifecycle = await _client.GetPlacementLifecycleAsync(brainId).ConfigureAwait(false);
            return (BrainId: brainId, Lifecycle: lifecycle);
        });
        var lifecycles = await Task.WhenAll(lifecycleTasks).ConfigureAwait(false);
        var reconcileNodes = nodes
            .Where(entry =>
                entry.IsAlive
                && IsFresh(entry.LastSeenMs, nowMs)
                && !string.IsNullOrWhiteSpace(entry.Address)
                && !string.IsNullOrWhiteSpace(entry.RootActorName))
            .ToArray();

        var reconcileTasks = new List<Task<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)>>();
        foreach (var lifecycleEntry in lifecycles)
        {
            var placementEpoch = lifecycleEntry.Lifecycle?.PlacementEpoch ?? 0;
            if (placementEpoch == 0)
            {
                continue;
            }

            foreach (var node in reconcileNodes)
            {
                reconcileTasks.Add(QueryPlacementReconcileAsync(
                    lifecycleEntry.BrainId,
                    node,
                    placementEpoch));
            }
        }

        var reconcileResults = reconcileTasks.Count == 0
            ? Array.Empty<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)>()
            : await Task.WhenAll(reconcileTasks).ConfigureAwait(false);
        foreach (var reconcileResult in reconcileResults)
        {
            if (reconcileResult.Report?.Assignments is null
                || reconcileResult.Report.Assignments.Count == 0)
            {
                continue;
            }

            var report = reconcileResult.Report;
            var reportBrainId = report.BrainId is not null && report.BrainId.TryToGuid(out var parsedBrainId)
                ? parsedBrainId
                : reconcileResult.BrainId;

            foreach (var assignment in report.Assignments)
            {
                var actorKind = ToAssignmentTargetLabel(assignment.Target);
                var actorPid = assignment.ActorPid?.Trim() ?? string.Empty;
                var hostLabel = string.IsNullOrWhiteSpace(reconcileResult.Node.LogicalName)
                    ? "host node"
                    : reconcileResult.Node.LogicalName!;
                var address = reconcileResult.Node.Address ?? string.Empty;
                var workerRootActor = reconcileResult.Node.RootActorName ?? string.Empty;
                var hostSeenMs = (long)reconcileResult.Node.LastSeenMs;
                var isOnline = reconcileResult.Node.IsAlive && IsFresh(reconcileResult.Node.LastSeenMs, nowMs);
                var hostIsWorker = IsWorkerHostCandidate(reconcileResult.Node);
                var hostNodeId = reconcileResult.Node.NodeId is not null
                                 && reconcileResult.Node.NodeId.TryToGuid(out var reconcileNodeId)
                    ? reconcileNodeId
                    : Guid.Empty;

                if (assignment.WorkerNodeId is not null
                    && assignment.WorkerNodeId.TryToGuid(out var workerNodeId)
                    && nodeById.TryGetValue(workerNodeId, out var workerNode))
                {
                    hostLabel = string.IsNullOrWhiteSpace(workerNode.LogicalName) ? "host node" : workerNode.LogicalName!;
                    address = workerNode.Address ?? address;
                    workerRootActor = workerNode.RootActorName ?? workerRootActor;
                    hostSeenMs = (long)workerNode.LastSeenMs;
                    isOnline = workerNode.IsAlive && IsFresh(workerNode.LastSeenMs, nowMs);
                    hostIsWorker = IsWorkerHostCandidate(workerNode);
                    hostNodeId = workerNodeId;
                }

                if (hostIsWorker && isOnline && hostNodeId != Guid.Empty)
                {
                    AddWorkerBrainHint(hostNodeId, reportBrainId);
                    if (string.Equals(actorKind, "RegionShard", StringComparison.Ordinal))
                    {
                        AddWorkerBackendProbe(
                            hostNodeId,
                            reportBrainId,
                            address,
                            workerRootActor,
                            assignment.RegionId,
                            assignment.ShardIndex);
                    }
                }

                AddActorRow(
                    reportBrainId,
                    actorKind: actorKind,
                    actorPid: actorPid,
                    hostLabel: hostLabel,
                    address: address,
                    lastSeenMs: hostSeenMs,
                    isOnline: isOnline,
                    hostIsWorker: hostIsWorker,
                    regionId: assignment.RegionId,
                    shardIndex: assignment.ShardIndex);
            }
        }

        var workerBrainBackends = await ResolveWorkerBrainBackendsAsync(
                workerBackendProbeRequests.Values.SelectMany(static probes => probes.Values))
            .ConfigureAwait(false);

        return new HostedActorRowsResult(
            OrderHostedActorRows(rows),
            GroupHostedActorRows(rows),
            workerBrainHints,
            workerBrainBackends);

        async Task<(Guid BrainId, Nbn.Proto.Settings.NodeStatus Node, PlacementReconcileReport? Report)> QueryPlacementReconcileAsync(
            Guid brainId,
            Nbn.Proto.Settings.NodeStatus node,
            ulong placementEpoch)
        {
            var report = await _client.RequestPlacementReconcileAsync(
                    node.Address ?? string.Empty,
                    node.RootActorName ?? string.Empty,
                    brainId,
                    placementEpoch)
                .ConfigureAwait(false);
            return (brainId, node, report);
        }
    }

    private static string BuildHostedActorKey(
        Guid brainId,
        string actorPid,
        string actorKind,
        uint regionId,
        uint shardIndex)
    {
        if (!string.IsNullOrWhiteSpace(actorPid))
        {
            return actorPid.Trim();
        }

        return $"{brainId:N}|{actorKind}|{regionId}|{shardIndex}";
    }

    private bool IsWorkerHostCandidate(Nbn.Proto.Settings.NodeStatus node)
    {
        return IsWorkerHostCandidate(node.LogicalName, node.RootActorName);
    }

    private bool IsWorkerHostCandidate(string? logicalName, string? rootActorName)
        => WorkbenchWorkerHostGrouping.IsWorkerHostCandidate(Connections, logicalName, rootActorName);

    private sealed record HostedActorRowsResult(
        IReadOnlyList<NodeStatusItem> Rows,
        IReadOnlyList<HostedActorNodeGroupItem> Groups,
        IReadOnlyDictionary<Guid, HashSet<Guid>> WorkerBrainHints,
        IReadOnlyDictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint> WorkerBrainBackends);

    private sealed record HostedActorDisplayEntry(
        bool IsOnlineWorkerHost,
        bool IsOnline,
        long LastSeenMs,
        string GroupKey,
        string GroupLabel,
        NodeStatusItem Row);

    private static IReadOnlyList<NodeStatusItem> OrderHostedActorRows(IReadOnlyList<HostedActorDisplayEntry> rows)
        => rows
            .OrderByDescending(entry => entry.IsOnlineWorkerHost)
            .ThenByDescending(entry => entry.IsOnline)
            .ThenByDescending(entry => entry.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .Select(entry => entry.Row)
            .ToArray();

    private static IReadOnlyList<HostedActorNodeGroupItem> GroupHostedActorRows(IReadOnlyList<HostedActorDisplayEntry> rows)
        => rows
            .OrderByDescending(entry => entry.IsOnlineWorkerHost)
            .ThenByDescending(entry => entry.IsOnline)
            .ThenByDescending(entry => entry.LastSeenMs)
            .ThenBy(entry => entry.Row.LogicalName, StringComparer.OrdinalIgnoreCase)
            .GroupBy(entry => entry.GroupKey, StringComparer.Ordinal)
            .Select(group => new HostedActorNodeGroupItem(
                group.First().GroupLabel,
                group.Select(static entry => entry.Row).ToArray()))
            .ToArray();

    private readonly record struct WorkerBrainBackendProbeRequest(
        Guid NodeId,
        Guid BrainId,
        string WorkerAddress,
        string WorkerRootActor,
        int RegionId,
        int ShardIndex);

    private async Task<IReadOnlyDictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint>> ResolveWorkerBrainBackendsAsync(
        IEnumerable<WorkerBrainBackendProbeRequest> probes)
    {
        var probeArray = probes.ToArray();
        var expectedProbeCounts = probeArray
            .GroupBy(static probe => (probe.NodeId, probe.BrainId))
            .ToDictionary(static group => group.Key, static group => group.Count());
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        PruneWorkerBrainBackendProbeCache(expectedProbeCounts.Keys, nowMs);

        var results = new Dictionary<(Guid NodeId, Guid BrainId), WorkerBrainBackendHint>();
        var probesToRefresh = new List<WorkerBrainBackendProbeRequest>(probeArray.Length);
        foreach (var probe in probeArray)
        {
            var key = (probe.NodeId, probe.BrainId);
            if (_workerBrainBackendCache.TryGetValue(key, out var cached)
                && cached.ExpectedProbeCount == expectedProbeCounts[key]
                && nowMs - cached.UpdatedMs < WorkerBrainBackendProbeCacheMs)
            {
                if (cached.Hint is { } cachedHint)
                {
                    results[key] = cachedHint;
                }

                continue;
            }

            probesToRefresh.Add(probe);
        }

        if (probesToRefresh.Count == 0)
        {
            return results;
        }

        var tasks = probesToRefresh.Select(async probe =>
        {
            var info = await _client
                .GetHostedRegionShardBackendExecutionInfoAsync(
                    probe.WorkerAddress,
                    probe.WorkerRootActor,
                    probe.BrainId,
                    probe.RegionId,
                    probe.ShardIndex)
                .ConfigureAwait(false);
            return (probe, info);
        });

        var aggregate = new Dictionary<(Guid NodeId, Guid BrainId), (int ExecutedCount, bool SawCpu, bool SawGpu, bool Incomplete)>();
        foreach (var (probe, info) in await Task.WhenAll(tasks).ConfigureAwait(false))
        {
            var key = (probe.NodeId, probe.BrainId);
            if (!aggregate.TryGetValue(key, out var observed))
            {
                observed = (ExecutedCount: 0, SawCpu: false, SawGpu: false, Incomplete: false);
            }

            if (info is null)
            {
                observed.Incomplete = true;
                aggregate[key] = observed;
                continue;
            }

            var backendInfo = info.Value;
            if (!backendInfo.HasExecuted || string.Equals(backendInfo.BackendName, "unavailable", StringComparison.OrdinalIgnoreCase))
            {
                observed.Incomplete = true;
                aggregate[key] = observed;
                continue;
            }

            observed.ExecutedCount++;
            if (backendInfo.UsedGpu)
            {
                observed.SawGpu = true;
            }
            else
            {
                observed.SawCpu = true;
            }

            aggregate[key] = observed;
        }

        foreach (var (key, observed) in aggregate)
        {
            if (!expectedProbeCounts.TryGetValue(key, out var expectedProbeCount))
            {
                continue;
            }

            WorkerBrainBackendHint? resolvedHint = null;
            if (!observed.Incomplete && observed.ExecutedCount == expectedProbeCount)
            {
                if (observed.SawCpu && observed.SawGpu)
                {
                    resolvedHint = WorkerBrainBackendHint.Mixed;
                }
                else if (observed.SawGpu)
                {
                    resolvedHint = WorkerBrainBackendHint.Gpu;
                }
                else if (observed.SawCpu)
                {
                    resolvedHint = WorkerBrainBackendHint.Cpu;
                }
            }

            _workerBrainBackendCache[key] = new WorkerBrainBackendProbeCacheEntry(
                resolvedHint,
                expectedProbeCount,
                nowMs);
            if (resolvedHint is { } definiteHint)
            {
                results[key] = definiteHint;
            }
        }

        return results;
    }

    private void PruneWorkerBrainBackendProbeCache(
        IEnumerable<(Guid NodeId, Guid BrainId)> activeKeys,
        long nowMs)
    {
        var active = activeKeys.ToHashSet();
        foreach (var key in _workerBrainBackendCache.Keys.ToArray())
        {
            if (!active.Contains(key)
                || nowMs - _workerBrainBackendCache[key].UpdatedMs > WorkerBrainBackendProbeCacheMs * 4)
            {
                _workerBrainBackendCache.Remove(key);
            }
        }
    }

    private static string ToAssignmentTargetLabel(PlacementAssignmentTarget target)
    {
        return target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => "BrainHost",
            PlacementAssignmentTarget.PlacementTargetSignalRouter => "SignalRouter",
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => "InputCoordinator",
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => "OutputCoordinator",
            PlacementAssignmentTarget.PlacementTargetRegionShard => "RegionShard",
            _ => "HostedActor"
        };
    }
}
