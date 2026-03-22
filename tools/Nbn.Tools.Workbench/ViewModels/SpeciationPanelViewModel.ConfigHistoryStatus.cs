using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Speciation;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class SpeciationPanelViewModel
{
    /// <summary>
    /// Applies SettingsMonitor-backed speciation settings into the current draft state.
    /// </summary>
    public bool ApplySetting(SettingItem item)
    {
        if (item is null || string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        var key = item.Key.Trim();
        var value = item.Value?.Trim() ?? string.Empty;
        bool Applied()
        {
            UpdateSettingsBackedConfigStatus();
            return true;
        }

        if (string.Equals(key, SpeciationSettingsKeys.ConfigEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            ConfigEnabled = ParseBool(value, ConfigEnabled);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.PolicyVersionKey, StringComparison.OrdinalIgnoreCase))
        {
            PolicyVersion = string.IsNullOrWhiteSpace(value) ? PolicyVersion : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.DefaultSpeciesIdKey, StringComparison.OrdinalIgnoreCase))
        {
            DefaultSpeciesId = string.IsNullOrWhiteSpace(value) ? DefaultSpeciesId : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey, StringComparison.OrdinalIgnoreCase))
        {
            DefaultSpeciesDisplayName = string.IsNullOrWhiteSpace(value) ? DefaultSpeciesDisplayName : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.StartupReconcileReasonKey, StringComparison.OrdinalIgnoreCase))
        {
            StartupReconcileReason = string.IsNullOrWhiteSpace(value) ? StartupReconcileReason : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageMatchThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageMatchThreshold = ParseDouble(value, 0.92d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageSplitThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageSplitThreshold = ParseDouble(value, 0.88d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.ParentConsensusThresholdKey, StringComparison.OrdinalIgnoreCase))
        {
            ParentConsensusThreshold = ParseDouble(value, 0.70d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageHysteresisMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            HysteresisMargin = ParseDouble(value, 0.04d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageSplitGuardMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageSplitGuardMargin = ParseDouble(value, 0.02d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageMinParentMembershipsBeforeSplit = Math.Max(1, ParseInt(value, 1)).ToString(CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageRealignParentMembershipWindow = Math.Max(0, ParseInt(value, 3)).ToString(CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageRealignMatchMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageRealignMatchMargin = ParseDouble(value, 0.05d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageHindsightReassignCommitWindow = Math.Max(0, ParseInt(value, 6)).ToString(CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey, StringComparison.OrdinalIgnoreCase))
        {
            LineageHindsightSimilarityMargin = ParseDouble(value, 0.015d).ToString("0.###", CultureInfo.InvariantCulture);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey, StringComparison.OrdinalIgnoreCase))
        {
            CreateDerivedSpecies = ParseBool(value, CreateDerivedSpecies);
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.DerivedSpeciesPrefixKey, StringComparison.OrdinalIgnoreCase))
        {
            DerivedSpeciesPrefix = string.IsNullOrWhiteSpace(value) ? DerivedSpeciesPrefix : value;
            return Applied();
        }

        if (string.Equals(key, SpeciationSettingsKeys.HistoryLimitKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = Math.Max(1u, ParseUInt(value, DefaultHistoryLimit));
            HistoryLimitText = parsed.ToString(CultureInfo.InvariantCulture);
            _lastPersistedHistoryLimit = parsed;
            return Applied();
        }

        return false;
    }

    private void UpdateSettingsBackedConfigStatus()
    {
        if (!string.Equals(ConfigStatus, "Settings-backed draft pending.", StringComparison.Ordinal)
            && !string.Equals(ConfigStatus, "Config not loaded.", StringComparison.Ordinal)
            && !string.Equals(ConfigStatus, "Settings-backed draft active.", StringComparison.Ordinal)
            && !string.Equals(ConfigStatus, "Config loaded.", StringComparison.Ordinal))
        {
            return;
        }

        ConfigStatus = "Settings-backed draft active.";
    }

    public async ValueTask DisposeAsync()
    {
        _connections.PropertyChanged -= OnConnectionsPropertyChanged;
        StopLiveChartsPolling();
        _simPollCts?.Cancel();
        await StopSimulatorAsync().ConfigureAwait(false);
    }

    private async Task RefreshAllAsync()
    {
        await RefreshPaneDataAsync(includeMemberships: true).ConfigureAwait(false);
        await RefreshSimulatorStatusAsync().ConfigureAwait(false);
    }

    private void OnConnectionsPropertyChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (string.IsNullOrWhiteSpace(args.PropertyName)
            || !SpeciationAutoRefreshTriggerProperties.Contains(args.PropertyName))
        {
            return;
        }

        if (!Connections.SettingsConnected || !Connections.SpeciationDiscoverable)
        {
            return;
        }

        if (Interlocked.Exchange(ref _autoRefreshInFlight, 1) != 0)
        {
            return;
        }

        _ = RefreshPaneStateFromConnectionsAsync();
    }

    private async Task RefreshPaneStateFromConnectionsAsync()
    {
        try
        {
            await RefreshPaneDataAsync(includeMemberships: true).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Status = $"Speciation refresh failed: {ex.GetBaseException().Message}";
        }
        finally
        {
            Interlocked.Exchange(ref _autoRefreshInFlight, 0);
        }
    }

    private void StartLiveChartsPolling()
    {
        StopLiveChartsPolling();
        _liveChartsPollCts = new CancellationTokenSource();
        _ = PollLiveChartsAsync(_liveChartsPollCts.Token);
    }

    private void StopLiveChartsPolling()
    {
        _liveChartsPollCts?.Cancel();
        _liveChartsPollCts = null;
    }

    private async Task RefreshPaneDataAsync(bool includeMemberships)
    {
        await RefreshStatusAsync().ConfigureAwait(false);
        if (includeMemberships)
        {
            await RefreshMembershipsAsync().ConfigureAwait(false);
        }

        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task PollLiveChartsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var intervalSeconds = ParseLiveChartIntervalSecondsOrDefault();
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (cancellationToken.IsCancellationRequested || !LiveChartsEnabled)
            {
                break;
            }

            if (!Connections.SettingsConnected || !Connections.SpeciationDiscoverable)
            {
                LiveChartsStatus = "Waiting for Settings/speciation discovery.";
                continue;
            }

            var includeMemberships = DateTimeOffset.UtcNow - _lastMembershipRefreshAt >= MembershipRefreshCadence;
            await RefreshPaneDataAsync(includeMemberships).ConfigureAwait(false);
            if (_evolutionRunner.IsRunning || !string.IsNullOrWhiteSpace(_simStdoutLogPath))
            {
                await RefreshSimulatorStatusAsync().ConfigureAwait(false);
            }

            LiveChartsStatus = includeMemberships
                ? $"Auto updates active ({intervalSeconds}s, counts every ~{MembershipRefreshCadence.TotalSeconds:0}s)."
                : $"Auto updates active ({intervalSeconds}s).";
        }
    }

    private void ResetHistoryMutationConfirmations()
    {
        if (_clearAllHistoryConfirmPending)
        {
            _clearAllHistoryConfirmPending = false;
            OnPropertyChanged(nameof(ClearAllHistoryLabel));
        }

        if (_deleteEpochConfirmPending || _deleteEpochConfirmTarget.HasValue)
        {
            _deleteEpochConfirmPending = false;
            _deleteEpochConfirmTarget = null;
            OnPropertyChanged(nameof(DeleteEpochLabel));
        }
    }

    private async Task RefreshStatusAsync()
    {
        var response = await _client.GetSpeciationStatusAsync().ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ServiceSummary = $"Status failed: {reason}";
            Status = ServiceSummary;
            return;
        }

        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            CurrentMembershipCount = response.Status.MembershipCount;
            CurrentSpeciesCount = response.Status.SpeciesCount;
            CurrentLineageEdgeCount = response.Status.LineageEdgeCount;
            ServiceSummary = $"Epoch {CurrentEpochLabel} | memberships={CurrentMembershipCount} species={CurrentSpeciesCount} lineage={CurrentLineageEdgeCount}";
            Status = "Speciation status refreshed.";
        });
    }

    private async Task LoadConfigAsync()
    {
        var response = await _client.GetSpeciationConfigAsync().ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ConfigStatus = $"Load failed: {reason}";
            Status = ConfigStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            ConfigStatus = "Config loaded.";
            Status = "Speciation config refreshed.";
        });
    }

    private async Task ApplyConfigAsync()
    {
        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        var config = BuildRuntimeConfigFromDraft();
        var response = await _client.SetSpeciationConfigAsync(config, startNewEpoch: false).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ConfigStatus = $"Apply failed: {reason}";
            Status = ConfigStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            ConfigStatus = "Config applied.";
            Status = "Speciation config updated.";
        });
        await PersistSpeciationSettingsAsync().ConfigureAwait(false);

        if (_refreshOrchestrator is not null)
        {
            await _refreshOrchestrator().ConfigureAwait(false);
        }
    }

    private async Task StartNewEpochAsync()
    {
        ResetHistoryMutationConfirmations();
        if (!_startNewEpochConfirmPending)
        {
            _startNewEpochConfirmPending = true;
            OnPropertyChanged(nameof(StartNewEpochLabel));
            ConfigStatus = "Click Start New Epoch again to confirm.";
            Status = ConfigStatus;
            return;
        }

        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        var config = BuildRuntimeConfigFromDraft();
        var response = await _client.SetSpeciationConfigAsync(config, startNewEpoch: true).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            ConfigStatus = $"New epoch failed: {reason}";
            Status = ConfigStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            ConfigStatus = $"New epoch started ({CurrentEpochLabel}).";
            Status = "Speciation epoch advanced.";
        });
        await PersistSpeciationSettingsAsync().ConfigureAwait(false);

        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task ClearAllHistoryAsync()
    {
        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        if (!_clearAllHistoryConfirmPending)
        {
            _clearAllHistoryConfirmPending = true;
            _deleteEpochConfirmPending = false;
            _deleteEpochConfirmTarget = null;
            OnPropertyChanged(nameof(ClearAllHistoryLabel));
            OnPropertyChanged(nameof(DeleteEpochLabel));
            HistoryStatus = "Click Delete All Epochs again to confirm. This removes all epoch history and starts a new epoch.";
            Status = HistoryStatus;
            return;
        }

        _clearAllHistoryConfirmPending = false;
        OnPropertyChanged(nameof(ClearAllHistoryLabel));
        var response = await _client.ResetSpeciationHistoryAsync().ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"Clear history failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        ApplyConfig(response.Config);
        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            CurrentMembershipCount = 0;
            CurrentSpeciesCount = 0;
            CurrentLineageEdgeCount = 0;
            CurrentEpochMaxDivergenceLabel = $"Max within-species divergence (epoch {CurrentEpochLabel}): (n/a)";
            CurrentEpochSplitProximityLabel = $"Split proximity (epoch {CurrentEpochLabel}): (n/a)";
            HistoryStatus =
                $"History cleared: deleted epochs={response.DeletedEpochCount}, memberships={response.DeletedMembershipCount}, species={response.DeletedSpeciesCount}, decisions={response.DeletedDecisionCount}.";
            Status = HistoryStatus;
        });

        await RefreshStatusAsync().ConfigureAwait(false);
        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task DeleteEpochAsync()
    {
        _startNewEpochConfirmPending = false;
        OnPropertyChanged(nameof(StartNewEpochLabel));

        if (!long.TryParse(DeleteEpochText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var epochId) || epochId <= 0)
        {
            _deleteEpochConfirmPending = false;
            _deleteEpochConfirmTarget = null;
            OnPropertyChanged(nameof(DeleteEpochLabel));
            HistoryStatus = "Enter a positive epoch id to delete.";
            Status = HistoryStatus;
            return;
        }

        if (!_deleteEpochConfirmPending || _deleteEpochConfirmTarget != epochId)
        {
            _deleteEpochConfirmPending = true;
            _deleteEpochConfirmTarget = epochId;
            _clearAllHistoryConfirmPending = false;
            OnPropertyChanged(nameof(DeleteEpochLabel));
            OnPropertyChanged(nameof(ClearAllHistoryLabel));
            HistoryStatus = $"Click Delete Epoch again to confirm deletion of epoch {epochId}.";
            Status = HistoryStatus;
            return;
        }

        _deleteEpochConfirmPending = false;
        _deleteEpochConfirmTarget = null;
        OnPropertyChanged(nameof(DeleteEpochLabel));

        var response = await _client.DeleteSpeciationEpochAsync(epochId).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"Delete epoch failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        if (!response.Deleted)
        {
            HistoryStatus = $"Epoch {epochId} was not deleted.";
            Status = HistoryStatus;
            return;
        }

        _dispatcher.Post(() =>
        {
            CurrentEpochId = (long)response.CurrentEpoch.EpochId;
            HistoryStatus =
                $"Deleted epoch {epochId}: memberships={response.DeletedMembershipCount}, species={response.DeletedSpeciesCount}, decisions={response.DeletedDecisionCount}.";
            Status = HistoryStatus;
        });

        await RefreshStatusAsync().ConfigureAwait(false);
        await RefreshMembershipsAsync().ConfigureAwait(false);
        await RefreshHistoryAsync().ConfigureAwait(false);
    }

    private async Task RefreshMembershipsAsync()
    {
        var requestedEpochId = ResolveEpochFilter();
        var response = await _client.ListSpeciationMembershipsAsync(requestedEpochId).ConfigureAwait(false);
        if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(response.FailureReason, response.FailureDetail);
            HistoryStatus = $"Membership load failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        var rows = response.Memberships
            .GroupBy(m => new
            {
                SpeciesId = string.IsNullOrWhiteSpace(m.SpeciesId) ? "(unknown)" : m.SpeciesId.Trim(),
                SpeciesName = BuildCompactSpeciesName(m.SpeciesDisplayName, m.SpeciesId)
            })
            .Select(group => new
            {
                group.Key.SpeciesId,
                group.Key.SpeciesName,
                Count = group.Count()
            })
            .OrderByDescending(entry => entry.Count)
            .ThenBy(entry => entry.SpeciesId, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var total = rows.Sum(entry => entry.Count);
        var maxCount = rows.Count == 0 ? 0 : rows.Max(entry => entry.Count);

        _dispatcher.Post(() =>
        {
            SpeciesCounts.Clear();
            foreach (var row in rows)
            {
                var ratio = total > 0 ? row.Count / (double)total : 0d;
                var bar = BuildBar(row.Count, maxCount);
                SpeciesCounts.Add(new SpeciationSpeciesCountItem(
                    row.SpeciesId,
                    row.SpeciesName,
                    row.Count,
                    ratio.ToString("P1", CultureInfo.InvariantCulture),
                    bar));
            }

            HistoryStatus = $"Loaded {total} memberships across {rows.Count} species.";
            Status = HistoryStatus;
            _lastMembershipRefreshAt = DateTimeOffset.UtcNow;
        });
    }

    private async Task RefreshHistoryAsync()
    {
        var historyLimit = Math.Max(1u, ParseUInt(HistoryLimitText, DefaultHistoryLimit));
        HistoryLimitText = historyLimit.ToString(CultureInfo.InvariantCulture);
        if (_lastPersistedHistoryLimit != historyLimit)
        {
            await _client.SetSettingAsync(
                    SpeciationSettingsKeys.HistoryLimitKey,
                    historyLimit.ToString(CultureInfo.InvariantCulture))
                .ConfigureAwait(false);
            _lastPersistedHistoryLimit = historyLimit;
        }

        var chartWindow = ParseUInt(ChartWindowText, DefaultVisibleChartWindow);
        ChartWindowText = chartWindow.ToString(CultureInfo.InvariantCulture);
        var chartHistoryLimit = chartWindow == 0u ? DefaultChartHistoryLimit : chartWindow;
        var historyPageSize = Math.Max(historyLimit, Math.Max(chartHistoryLimit, DefaultCladogramHistoryLimit));
        var epochFilter = ResolveEpochFilter();

        var chartResponse = await LoadCompleteSpeciationHistoryAsync(
                epochFilter,
                brainId: null,
                historyPageSize)
            .ConfigureAwait(false);
        if (chartResponse.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
        {
            var reason = NormalizeFailure(chartResponse.FailureReason, chartResponse.FailureDetail);
            HistoryStatus = $"History load failed: {reason}";
            Status = HistoryStatus;
            return;
        }

        var chartHistory = TrimHistoryToChartWindow(chartResponse.History, chartWindow);
        var populationFrame = BuildEpochPopulationFrame(chartHistory);
        var epochSummaries = BuildEpochSummaries(chartHistory);
        var speciesColors = BuildSpeciesColorMap(chartResponse.History, _speciesColorOverrides);
        var flowChartSource = new FlowChartSourceFrame(populationFrame.EpochRows, populationFrame.SpeciesOrder, speciesColors);
        var speciationChartSource = new SpeciationChartSourceFrame(
            ChartHistory: chartHistory,
            ColorSourceHistory: chartResponse.History,
            CladogramHistory: chartResponse.History);
        var populationSnapshot = BuildPopulationChartSnapshot(populationFrame.EpochRows, populationFrame.SpeciesOrder, speciesColors);
        var flowSnapshot = BuildFlowChartSnapshot(
            flowChartSource.EpochRows,
            flowChartSource.SpeciesOrder,
            flowChartSource.SpeciesColors,
            BuildInlineFlowChartRenderLayout());
        var expandedFlowSnapshot = BuildFlowChartSnapshot(
            flowChartSource.EpochRows,
            flowChartSource.SpeciesOrder,
            flowChartSource.SpeciesColors,
            BuildExpandedFlowChartRenderLayout());
        var divergenceSnapshot = BuildCurrentEpochDivergenceSnapshot(chartHistory, CurrentEpochId);
        var splitProximitySnapshot = BuildSplitProximityChartSnapshot(
            chartHistory,
            CurrentEpochId,
            ParseDouble(LineageSplitThreshold, 0.88d),
            ParseDouble(LineageSplitGuardMargin, 0.02d),
            speciesColors);
        var cladogramSourceHistory = chartResponse.History;
        var cladogramSnapshot = BuildCladogramSnapshot(cladogramSourceHistory, speciesColors);

        _dispatcher.Post(() =>
        {
            EpochSummaries.Clear();
            foreach (var row in epochSummaries)
            {
                EpochSummaries.Add(row);
            }

            _lastSpeciationChartSource = speciationChartSource;
            _lastFlowChartSource = flowChartSource;
            ApplyPopulationChartSnapshot(populationSnapshot);
            ApplyFlowChartSnapshot(flowSnapshot);
            ApplyExpandedFlowChartSnapshot(expandedFlowSnapshot);
            CurrentEpochMaxDivergenceLabel = divergenceSnapshot.Label;
            CurrentEpochSplitProximityLabel = splitProximitySnapshot.CurrentEpochSummaryLabel;
            ApplySplitProximityChartSnapshot(splitProximitySnapshot);
            ApplyCladogramSnapshot(cladogramSnapshot);
            HistoryStatus =
                $"Speciation data loaded: fetched={chartResponse.History.Count} total={chartResponse.TotalRecords}";
            Status = HistoryStatus;
        });
    }

    private async Task<SpeciationListHistoryResponse> LoadCompleteSpeciationHistoryAsync(
        long? epochId,
        Guid? brainId,
        uint pageSize)
    {
        var normalizedPageSize = Math.Max(1u, pageSize);
        var combined = new List<SpeciationMembershipRecord>();
        uint offset = 0;
        uint totalRecords = 0;

        while (true)
        {
            var response = await _client.ListSpeciationHistoryAsync(
                    epochId: epochId,
                    brainId: brainId,
                    limit: normalizedPageSize,
                    offset: offset)
                .ConfigureAwait(false);
            if (response.FailureReason != SpeciationFailureReason.SpeciationFailureNone)
            {
                return response;
            }

            totalRecords = response.TotalRecords;
            if (response.History.Count == 0)
            {
                break;
            }

            combined.AddRange(response.History);
            if ((uint)combined.Count >= totalRecords)
            {
                break;
            }

            var nextOffset = (uint)combined.Count;
            if (nextOffset <= offset)
            {
                break;
            }

            offset = nextOffset;
        }

        var combinedResponse = new SpeciationListHistoryResponse
        {
            FailureReason = SpeciationFailureReason.SpeciationFailureNone,
            FailureDetail = string.Empty,
            TotalRecords = totalRecords
        };
        combinedResponse.History.AddRange(combined);
        return combinedResponse;
    }

    private static IReadOnlyList<SpeciationMembershipRecord> TrimHistoryToChartWindow(
        IReadOnlyList<SpeciationMembershipRecord> history,
        uint chartWindow)
    {
        if (chartWindow == 0u || history.Count == 0 || history.Count <= chartWindow)
        {
            return history;
        }

        var skip = history.Count - (int)chartWindow;
        return history.Skip(skip).ToArray();
    }

    private void ApplyConfig(SpeciationRuntimeConfig config)
    {
        if (config is null)
        {
            return;
        }

        var snapshot = ParseSnapshot(config.ConfigSnapshotJson);
        _dispatcher.Post(() =>
        {
            PolicyVersion = string.IsNullOrWhiteSpace(config.PolicyVersion) ? "default" : config.PolicyVersion.Trim();
            DefaultSpeciesId = string.IsNullOrWhiteSpace(config.DefaultSpeciesId) ? "species.default" : config.DefaultSpeciesId.Trim();
            DefaultSpeciesDisplayName = string.IsNullOrWhiteSpace(config.DefaultSpeciesDisplayName)
                ? "Default species"
                : config.DefaultSpeciesDisplayName.Trim();
            StartupReconcileReason = string.IsNullOrWhiteSpace(config.StartupReconcileDecisionReason)
                ? "startup_reconcile"
                : config.StartupReconcileDecisionReason.Trim();

            ConfigEnabled = snapshot.Enabled;
            LineageMatchThreshold = snapshot.MatchThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            LineageSplitThreshold = snapshot.SplitThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            ParentConsensusThreshold = snapshot.ParentConsensusThreshold.ToString("0.###", CultureInfo.InvariantCulture);
            HysteresisMargin = snapshot.HysteresisMargin.ToString("0.###", CultureInfo.InvariantCulture);
            LineageSplitGuardMargin = snapshot.LineageSplitGuardMargin.ToString("0.###", CultureInfo.InvariantCulture);
            LineageMinParentMembershipsBeforeSplit = snapshot.LineageMinParentMembershipsBeforeSplit.ToString(CultureInfo.InvariantCulture);
            LineageRealignParentMembershipWindow = snapshot.LineageRealignParentMembershipWindow.ToString(CultureInfo.InvariantCulture);
            LineageRealignMatchMargin = snapshot.LineageRealignMatchMargin.ToString("0.###", CultureInfo.InvariantCulture);
            LineageHindsightReassignCommitWindow = snapshot.LineageHindsightReassignCommitWindow.ToString(CultureInfo.InvariantCulture);
            LineageHindsightSimilarityMargin = snapshot.LineageHindsightSimilarityMargin.ToString("0.###", CultureInfo.InvariantCulture);
            CreateDerivedSpecies = snapshot.CreateDerivedSpecies;
            DerivedSpeciesPrefix = snapshot.DerivedSpeciesPrefix;
        });
    }

    private SpeciationRuntimeConfig BuildRuntimeConfigFromDraft()
    {
        var matchThreshold = Clamp01(ParseDouble(LineageMatchThreshold, 0.92));
        var splitThreshold = Clamp01(ParseDouble(LineageSplitThreshold, Math.Max(0d, matchThreshold - 0.04d)));
        if (splitThreshold > matchThreshold)
        {
            splitThreshold = matchThreshold;
        }

        var parentConsensus = Clamp01(ParseDouble(ParentConsensusThreshold, 0.70));
        var hysteresisMargin = Math.Max(0d, ParseDouble(HysteresisMargin, Math.Max(0d, matchThreshold - splitThreshold)));
        var splitGuardMargin = Clamp01(ParseDouble(LineageSplitGuardMargin, 0.02d));
        var minParentMembershipsBeforeSplit = Math.Max(1, ParseInt(LineageMinParentMembershipsBeforeSplit, 1));
        var realignParentMembershipWindow = Math.Max(0, ParseInt(LineageRealignParentMembershipWindow, 3));
        var realignMatchMargin = Clamp01(ParseDouble(LineageRealignMatchMargin, 0.05d));
        var hindsightReassignCommitWindow = Math.Max(0, ParseInt(LineageHindsightReassignCommitWindow, 6));
        var hindsightSimilarityMargin = Clamp01(ParseDouble(LineageHindsightSimilarityMargin, 0.015d));
        var derivedPrefix = string.IsNullOrWhiteSpace(DerivedSpeciesPrefix) ? "branch" : DerivedSpeciesPrefix.Trim();
        var snapshot = new JsonObject
        {
            ["enabled"] = ConfigEnabled,
            ["assignment_policy"] = new JsonObject
            {
                ["lineage_match_threshold"] = matchThreshold,
                ["lineage_split_threshold"] = splitThreshold,
                ["parent_consensus_threshold"] = parentConsensus,
                ["lineage_hysteresis_margin"] = hysteresisMargin,
                ["lineage_split_guard_margin"] = splitGuardMargin,
                ["lineage_min_parent_memberships_before_split"] = minParentMembershipsBeforeSplit,
                ["lineage_realign_parent_membership_window"] = realignParentMembershipWindow,
                ["lineage_realign_match_margin"] = realignMatchMargin,
                ["lineage_hindsight_reassign_commit_window"] = hindsightReassignCommitWindow,
                ["lineage_hindsight_similarity_margin"] = hindsightSimilarityMargin,
                ["create_derived_species_on_divergence"] = CreateDerivedSpecies,
                ["derived_species_prefix"] = derivedPrefix
            }
        };

        return new SpeciationRuntimeConfig
        {
            PolicyVersion = string.IsNullOrWhiteSpace(PolicyVersion) ? "default" : PolicyVersion.Trim(),
            ConfigSnapshotJson = snapshot.ToJsonString(),
            DefaultSpeciesId = string.IsNullOrWhiteSpace(DefaultSpeciesId) ? "species.default" : DefaultSpeciesId.Trim(),
            DefaultSpeciesDisplayName = string.IsNullOrWhiteSpace(DefaultSpeciesDisplayName)
                ? "Default species"
                : DefaultSpeciesDisplayName.Trim(),
            StartupReconcileDecisionReason = string.IsNullOrWhiteSpace(StartupReconcileReason)
                ? "startup_reconcile"
                : StartupReconcileReason.Trim()
        };
    }

    private (
        bool Enabled,
        double MatchThreshold,
        double SplitThreshold,
        double ParentConsensusThreshold,
        double HysteresisMargin,
        double LineageSplitGuardMargin,
        int LineageMinParentMembershipsBeforeSplit,
        int LineageRealignParentMembershipWindow,
        double LineageRealignMatchMargin,
        int LineageHindsightReassignCommitWindow,
        double LineageHindsightSimilarityMargin,
        bool CreateDerivedSpecies,
        string DerivedSpeciesPrefix) ParseSnapshot(string snapshotJson)
    {
        var defaults = (
            Enabled: true,
            MatchThreshold: 0.92d,
            SplitThreshold: 0.88d,
            ParentConsensusThreshold: 0.70d,
            HysteresisMargin: 0.04d,
            LineageSplitGuardMargin: 0.02d,
            LineageMinParentMembershipsBeforeSplit: 1,
            LineageRealignParentMembershipWindow: 3,
            LineageRealignMatchMargin: 0.05d,
            LineageHindsightReassignCommitWindow: 6,
            LineageHindsightSimilarityMargin: 0.015d,
            CreateDerivedSpecies: true,
            DerivedSpeciesPrefix: "branch");

        if (string.IsNullOrWhiteSpace(snapshotJson))
        {
            return defaults;
        }

        try
        {
            var root = JsonNode.Parse(snapshotJson) as JsonObject;
            if (root is null)
            {
                return defaults;
            }

            var policy = root["assignment_policy"] as JsonObject
                         ?? root["assignmentPolicy"] as JsonObject
                         ?? root;
            var enabled = TryReadBool(root, "enabled") ?? defaults.Enabled;
            var match = Clamp01(TryReadDouble(policy, "lineage_match_threshold", "lineageMatchThreshold") ?? defaults.MatchThreshold);
            var split = Clamp01(TryReadDouble(policy, "lineage_split_threshold", "lineageSplitThreshold") ?? defaults.SplitThreshold);
            var consensus = Clamp01(TryReadDouble(policy, "parent_consensus_threshold", "parentConsensusThreshold") ?? defaults.ParentConsensusThreshold);
            var hysteresis = Math.Max(0d, TryReadDouble(policy, "lineage_hysteresis_margin", "lineageHysteresisMargin") ?? defaults.HysteresisMargin);
            var splitGuardMargin = Clamp01(TryReadDouble(policy, "lineage_split_guard_margin", "lineageSplitGuardMargin") ?? defaults.LineageSplitGuardMargin);
            var minParentMembershipsBeforeSplit = Math.Max(
                1,
                RoundToNonNegativeInt(
                    TryReadDouble(policy, "lineage_min_parent_memberships_before_split", "lineageMinParentMembershipsBeforeSplit")
                    ?? defaults.LineageMinParentMembershipsBeforeSplit,
                    defaults.LineageMinParentMembershipsBeforeSplit));
            var realignParentMembershipWindow = Math.Max(
                0,
                RoundToNonNegativeInt(
                    TryReadDouble(policy, "lineage_realign_parent_membership_window", "lineageRealignParentMembershipWindow")
                    ?? defaults.LineageRealignParentMembershipWindow,
                    defaults.LineageRealignParentMembershipWindow));
            var realignMatchMargin = Clamp01(TryReadDouble(policy, "lineage_realign_match_margin", "lineageRealignMatchMargin") ?? defaults.LineageRealignMatchMargin);
            var hindsightReassignCommitWindow = Math.Max(
                0,
                RoundToNonNegativeInt(
                    TryReadDouble(policy, "lineage_hindsight_reassign_commit_window", "lineageHindsightReassignCommitWindow")
                    ?? defaults.LineageHindsightReassignCommitWindow,
                    defaults.LineageHindsightReassignCommitWindow));
            var hindsightSimilarityMargin = Clamp01(
                TryReadDouble(policy, "lineage_hindsight_similarity_margin", "lineageHindsightSimilarityMargin")
                ?? defaults.LineageHindsightSimilarityMargin);
            var createDerived = TryReadBool(policy, "create_derived_species_on_divergence", "createDerivedSpeciesOnDivergence")
                                ?? defaults.CreateDerivedSpecies;
            var prefix = TryReadString(policy, "derived_species_prefix", "derivedSpeciesPrefix")
                         ?? defaults.DerivedSpeciesPrefix;
            return (
                enabled,
                match,
                split,
                consensus,
                hysteresis,
                splitGuardMargin,
                minParentMembershipsBeforeSplit,
                realignParentMembershipWindow,
                realignMatchMargin,
                hindsightReassignCommitWindow,
                hindsightSimilarityMargin,
                createDerived,
                string.IsNullOrWhiteSpace(prefix) ? "branch" : prefix.Trim());
        }
        catch (JsonException)
        {
            return defaults;
        }
    }

    private static bool? TryReadBool(JsonObject source, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (source[key] is not JsonNode node)
            {
                continue;
            }

            if (node is JsonValue value)
            {
                if (value.TryGetValue<bool>(out var boolValue))
                {
                    return boolValue;
                }

                if (value.TryGetValue<string>(out var stringValue)
                    && bool.TryParse(stringValue, out var parsed))
                {
                    return parsed;
                }
            }
        }

        return null;
    }

    private static double? TryReadDouble(JsonObject source, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (source[key] is not JsonNode node)
            {
                continue;
            }

            if (node is JsonValue value)
            {
                if (value.TryGetValue<double>(out var asDouble))
                {
                    return asDouble;
                }

                if (value.TryGetValue<float>(out var asFloat))
                {
                    return asFloat;
                }

                if (value.TryGetValue<string>(out var asString)
                    && double.TryParse(asString, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
                {
                    return parsed;
                }
            }
        }

        return null;
    }

    private static string? TryReadString(JsonObject source, params string[] keys)
    {
        foreach (var key in keys)
        {
            if (source[key] is JsonValue value && value.TryGetValue<string>(out var stringValue))
            {
                return stringValue;
            }
        }

        return null;
    }

    private static string NormalizeFailure(SpeciationFailureReason reason, string? detail)
    {
        var reasonText = reason.ToString();
        if (string.IsNullOrWhiteSpace(detail))
        {
            return reasonText;
        }

        return $"{reasonText}: {detail.Trim()}";
    }

    private long? ResolveEpochFilter()
    {
        if (string.IsNullOrWhiteSpace(EpochFilterText))
        {
            return CurrentEpochId > 0 ? CurrentEpochId : null;
        }

        return long.TryParse(EpochFilterText.Trim(), out var parsed) && parsed > 0 ? parsed : null;
    }

    private async Task PersistSpeciationSettingsAsync()
    {
        var updates = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SpeciationSettingsKeys.ConfigEnabledKey] = ConfigEnabled ? "true" : "false",
            [SpeciationSettingsKeys.PolicyVersionKey] = string.IsNullOrWhiteSpace(PolicyVersion) ? "default" : PolicyVersion.Trim(),
            [SpeciationSettingsKeys.DefaultSpeciesIdKey] = string.IsNullOrWhiteSpace(DefaultSpeciesId) ? "species.default" : DefaultSpeciesId.Trim(),
            [SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey] = string.IsNullOrWhiteSpace(DefaultSpeciesDisplayName) ? "Default species" : DefaultSpeciesDisplayName.Trim(),
            [SpeciationSettingsKeys.StartupReconcileReasonKey] = string.IsNullOrWhiteSpace(StartupReconcileReason) ? "startup_reconcile" : StartupReconcileReason.Trim(),
            [SpeciationSettingsKeys.LineageMatchThresholdKey] = Clamp01(ParseDouble(LineageMatchThreshold, 0.92d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageSplitThresholdKey] = Clamp01(ParseDouble(LineageSplitThreshold, 0.88d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.ParentConsensusThresholdKey] = Clamp01(ParseDouble(ParentConsensusThreshold, 0.70d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageHysteresisMarginKey] = Math.Max(0d, ParseDouble(HysteresisMargin, 0.04d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageSplitGuardMarginKey] = Clamp01(ParseDouble(LineageSplitGuardMargin, 0.02d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey] = Math.Max(1, ParseInt(LineageMinParentMembershipsBeforeSplit, 1)).ToString(CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey] = Math.Max(0, ParseInt(LineageRealignParentMembershipWindow, 3)).ToString(CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageRealignMatchMarginKey] = Clamp01(ParseDouble(LineageRealignMatchMargin, 0.05d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey] = Math.Max(0, ParseInt(LineageHindsightReassignCommitWindow, 6)).ToString(CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey] = Clamp01(ParseDouble(LineageHindsightSimilarityMargin, 0.015d)).ToString("0.###", CultureInfo.InvariantCulture),
            [SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey] = CreateDerivedSpecies ? "true" : "false",
            [SpeciationSettingsKeys.DerivedSpeciesPrefixKey] = string.IsNullOrWhiteSpace(DerivedSpeciesPrefix) ? "branch" : DerivedSpeciesPrefix.Trim(),
            [SpeciationSettingsKeys.HistoryLimitKey] = Math.Max(1u, ParseUInt(HistoryLimitText, DefaultHistoryLimit)).ToString(CultureInfo.InvariantCulture)
        };

        foreach (var (key, value) in updates)
        {
            await _client.SetSettingAsync(key, value).ConfigureAwait(false);
        }
    }
}
