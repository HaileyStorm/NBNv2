using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class SpeciationPanelViewModel
{
    /// <summary>
    /// Updates the simulator parent choices from the current active-brain set.
    /// </summary>
    public void UpdateActiveBrains(IReadOnlyList<BrainListItem> brains)
    {
        var options = brains
            .Where(entry => entry.BrainId != Guid.Empty)
            .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            .Select(entry => new SpeciationSimulatorBrainOption(entry.BrainId, entry.Display))
            .GroupBy(entry => entry.BrainId)
            .Select(group => group.First())
            .OrderBy(entry => entry.Label, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var selectedAId = SimSelectedParentABrain?.BrainId;
        var selectedBId = SimSelectedParentBBrain?.BrainId;
        var selectedExtraCandidateId = SimExtraParentCandidateBrain?.BrainId;

        _dispatcher.Post(() =>
        {
            SimActiveBrains.Clear();
            foreach (var option in options)
            {
                SimActiveBrains.Add(option);
            }

            SimSelectedParentABrain = selectedAId.HasValue
                ? SimActiveBrains.FirstOrDefault(entry => entry.BrainId == selectedAId.Value)
                : null;
            SimSelectedParentBBrain = selectedBId.HasValue
                ? SimActiveBrains.FirstOrDefault(entry => entry.BrainId == selectedBId.Value)
                : null;

            if (SimSelectedParentABrain is null && SimActiveBrains.Count > 0)
            {
                SimSelectedParentABrain = SimActiveBrains[0];
            }

            if (SimSelectedParentBBrain is null)
            {
                SimSelectedParentBBrain = SimActiveBrains
                    .FirstOrDefault(entry => entry.BrainId != SimSelectedParentABrain?.BrainId);
            }

            SimExtraParentCandidateBrain = selectedExtraCandidateId.HasValue
                ? SimActiveBrains.FirstOrDefault(entry => entry.BrainId == selectedExtraCandidateId.Value)
                : SimExtraParentCandidateBrain;
            if (SimExtraParentCandidateBrain is null && SimActiveBrains.Count > 0)
            {
                SimExtraParentCandidateBrain = SimActiveBrains[0];
            }

            RefreshEffectiveSimulatorSeedParents();
        });
    }

    private async Task StartServiceAsync()
    {
        if (_startSpeciationService is null)
        {
            ServiceSummary = "Speciation launcher is unavailable.";
            Status = ServiceSummary;
            return;
        }

        await _startSpeciationService().ConfigureAwait(false);
        if (_refreshOrchestrator is not null)
        {
            await _refreshOrchestrator().ConfigureAwait(false);
        }

        ServiceSummary = $"Speciation service launch requested ({Connections.SpeciationStatusLabel}).";
        Status = ServiceSummary;
    }

    private async Task StopServiceAsync()
    {
        if (_stopSpeciationService is null)
        {
            ServiceSummary = "Speciation stopper is unavailable.";
            Status = ServiceSummary;
            return;
        }

        await _stopSpeciationService().ConfigureAwait(false);
        if (_refreshOrchestrator is not null)
        {
            await _refreshOrchestrator().ConfigureAwait(false);
        }

        ServiceSummary = "Speciation service stop requested.";
        Status = ServiceSummary;
    }

    private async Task StartSimulatorAsync()
    {
        if (!TryParsePort(Connections.IoPortText, out var ioPort))
        {
            SimulatorStatus = "Invalid IO port for simulator.";
            Status = SimulatorStatus;
            return;
        }

        if (!TryParsePort(SimPortText, out var simPort))
        {
            SimulatorStatus = "Invalid simulator port.";
            Status = SimulatorStatus;
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("tools", "Nbn.Tools.EvolutionSim");
        if (!TryResolveSimulatorParentPool(out var parentPool, out var parentError))
        {
            SimulatorStatus = parentError;
            Status = SimulatorStatus;
            return;
        }

        var args = BuildEvolutionSimArgs(ioPort, simPort, parentPool);
        if (string.IsNullOrWhiteSpace(args))
        {
            SimulatorStatus = "Simulator requires at least two usable parent brain IDs.";
            Status = SimulatorStatus;
            return;
        }

        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Tools.EvolutionSim", args, "EvolutionSim").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            SimulatorStatus = launch.Message;
            Status = $"Evolution simulator: {launch.Message}";
            SimulatorDetailedStats = "No simulator statistics yet.";
            return;
        }

        var startInfo = launch.StartInfo;
        var startResult = await _evolutionRunner.StartAsync(startInfo, waitForExit: false, label: "EvolutionSim").ConfigureAwait(false);
        var startMessage = await AppendFirewallAttentionAsync("EvolutionSim", SimBindHost, simPort, startResult.Message).ConfigureAwait(false);
        SimulatorStatus = startMessage;
        Status = $"Evolution simulator: {startMessage}";
        SimulatorDetailedStats = startResult.Success
            ? "Starting simulator status polling..."
            : "No simulator statistics yet.";
        OnPropertyChanged(nameof(SimRunnerActive));

        _simStdoutLogPath = ExtractLogPath(startResult.Message);
        _simStdoutLogPosition = 0;
        _simLastStatusLine = null;
        _simPollCts?.Cancel();
        if (startResult.Success)
        {
            _simPollCts = new CancellationTokenSource();
            _ = PollSimulatorStatusAsync(_simPollCts.Token);
        }
    }

    private async Task StopSimulatorAsync()
    {
        _simPollCts?.Cancel();
        _simPollCts = null;
        _simStdoutLogPosition = 0;
        _simLastStatusLine = null;
        _simStdoutLogPath = null;

        var stopMessage = await _evolutionRunner.StopAsync().ConfigureAwait(false);
        SimulatorStatus = stopMessage;
        Status = $"Evolution simulator: {stopMessage}";
        SimulatorProgress = "No active simulator session.";
        SimulatorDetailedStats = "No simulator statistics yet.";
        OnPropertyChanged(nameof(SimRunnerActive));
    }

    private Task RefreshSimulatorStatusAsync()
    {
        if (string.IsNullOrWhiteSpace(_simStdoutLogPath))
        {
            if (_evolutionRunner.IsRunning)
            {
                SimulatorProgress = "Running (enable Workbench logging for live session details).";
                SimulatorDetailedStats = "Waiting for first simulator status payload.";
            }
            else
            {
                SimulatorProgress = "No active simulator session.";
                SimulatorDetailedStats = "No simulator statistics yet.";
            }

            return Task.CompletedTask;
        }

        if (!File.Exists(_simStdoutLogPath))
        {
            SimulatorProgress = _evolutionRunner.IsRunning
                ? "Waiting for simulator status stream..."
                : "Simulator log not found.";
            SimulatorDetailedStats = "No simulator statistics yet.";
            return Task.CompletedTask;
        }

        var lastLine = ReadLatestNonEmptyLine(_simStdoutLogPath, ref _simStdoutLogPosition, ref _simLastStatusLine);
        if (string.IsNullOrWhiteSpace(lastLine))
        {
            SimulatorProgress = _evolutionRunner.IsRunning
                ? "Waiting for simulator status stream..."
                : "No simulator status rows.";
            SimulatorDetailedStats = "No simulator statistics yet.";
            return Task.CompletedTask;
        }

        if (!TryParseSimulatorStatus(lastLine, out var snapshot))
        {
            return Task.CompletedTask;
        }

        _dispatcher.Post(() =>
        {
            SimulatorSessionId = snapshot.SessionId;
            var childrenLabel = snapshot.ChildrenAddedToPool.ToString(CultureInfo.InvariantCulture);
            if (!SimSpawnChildren && snapshot.ChildrenAddedToPool == 0 && snapshot.ReproductionCalls > 0)
            {
                childrenLabel = "0 (no parent-pool growth yet)";
            }

            var overallSimilarityLabel = FormatSimilarityRange(
                snapshot.SimilaritySamples,
                snapshot.MinSimilarityObserved,
                snapshot.MaxSimilarityObserved);
            var assessmentSimilarityLabel = FormatSimilarityRange(
                snapshot.AssessmentSimilaritySamples,
                snapshot.MinAssessmentSimilarityObserved,
                snapshot.MaxAssessmentSimilarityObserved);
            var reproductionSimilarityLabel = FormatSimilarityRange(
                snapshot.ReproductionSimilaritySamples,
                snapshot.MinReproductionSimilarityObserved,
                snapshot.MaxReproductionSimilarityObserved);
            var commitSimilarityLabel = FormatSimilarityRange(
                snapshot.SpeciationCommitSimilaritySamples,
                snapshot.MinSpeciationCommitSimilarityObserved,
                snapshot.MaxSpeciationCommitSimilarityObserved);

            SimulatorProgress =
                $"running={snapshot.Running} final={snapshot.Final} iter={snapshot.Iterations} parent_pool_size={snapshot.ParentPoolSize}";
            SimulatorDetailedStats =
                $"compat={snapshot.CompatiblePairs}/{snapshot.CompatibilityChecks} " +
                $"repro_calls={snapshot.ReproductionCalls} repro_fail={snapshot.ReproductionFailures} " +
                $"parent_pool_size={snapshot.ParentPoolSize} children_added_to_pool={childrenLabel} " +
                $"runs={snapshot.ReproductionRunsObserved} runs_mutated={snapshot.ReproductionRunsWithMutations} mutation_events={snapshot.ReproductionMutationEvents} " +
                $"sim_overall={overallSimilarityLabel} sim_assess={assessmentSimilarityLabel} sim_repro={reproductionSimilarityLabel} sim_commit={commitSimilarityLabel} " +
                $"speciation={snapshot.SpeciationCommitSuccesses}/{snapshot.SpeciationCommitAttempts} " +
                $"seed={snapshot.LastSeed}";
            SimulatorLastFailure = string.IsNullOrWhiteSpace(snapshot.LastFailure) ? "(none)" : snapshot.LastFailure;
            if (!snapshot.Running)
            {
                SimulatorStatus = snapshot.Final ? "Completed." : "Stopped.";
            }
        });

        return Task.CompletedTask;
    }

    private async Task PollSimulatorStatusAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            await RefreshSimulatorStatusAsync().ConfigureAwait(false);
            if (!_evolutionRunner.IsRunning)
            {
                break;
            }
        }
    }

    private async Task BrowseSimulatorParentFileAsync(SimulatorParentFileKind kind)
    {
        var title = kind == SimulatorParentFileKind.ParentAOverride
            ? "Select Parent A override file"
            : "Select Parent B override file";
        var file = await PickOpenFileAsync(title).ConfigureAwait(false);
        if (file is null)
        {
            return;
        }

        var path = FormatPath(file);
        _dispatcher.Post(() =>
        {
            if (kind == SimulatorParentFileKind.ParentAOverride)
            {
                SimParentAOverrideFilePath = path;
            }
            else
            {
                SimParentBOverrideFilePath = path;
            }
        });
    }

    private void AddSimulatorSeedParentFromCandidate()
    {
        if (SimExtraParentCandidateBrain is null || SimExtraParentCandidateBrain.BrainId == Guid.Empty)
        {
            SimulatorStatus = "Select an active brain to add as a seed parent.";
            Status = SimulatorStatus;
            return;
        }

        TryAddSimulatorSeedParent(
            SimExtraParentCandidateBrain.BrainId,
            SimExtraParentCandidateBrain.Label,
            source: "dropdown");
    }

    private void AddAllSimulatorSeedParents()
    {
        if (SimActiveBrains.Count == 0)
        {
            SimulatorStatus = "No active brains are available to add as seed parents.";
            Status = SimulatorStatus;
            return;
        }

        var added = 0;
        foreach (var activeBrain in SimActiveBrains)
        {
            if (TryAddSimulatorSeedParent(activeBrain.BrainId, activeBrain.Label, source: "dropdown", updateStatus: false))
            {
                added++;
            }
        }

        SimulatorStatus = added > 0
            ? $"Added {added} seed parent(s) from active brains."
            : "No new seed parents were added from active brains (all duplicates).";
        Status = SimulatorStatus;
    }

    private async Task AddSimulatorSeedParentsFromFileAsync()
    {
        var file = await PickOpenFileAsync("Select simulator extra-parents file").ConfigureAwait(false);
        if (file is null)
        {
            return;
        }

        var path = FormatPath(file);
        IReadOnlyList<Guid> parentIds;
        try
        {
            parentIds = ParseBrainIdsFromFile(path, "--extra-parents-file");
        }
        catch (Exception ex)
        {
            SimulatorStatus = $"Extra parent file failed: {ex.GetBaseException().Message}";
            Status = SimulatorStatus;
            return;
        }

        if (parentIds.Count == 0)
        {
            SimulatorStatus = $"Extra parent file has no usable brain GUIDs: {path}";
            Status = SimulatorStatus;
            return;
        }

        var labelsByBrainId = SimActiveBrains
            .GroupBy(entry => entry.BrainId)
            .ToDictionary(group => group.Key, group => group.First().Label);
        var added = 0;
        foreach (var parentId in parentIds)
        {
            var label = labelsByBrainId.TryGetValue(parentId, out var activeLabel)
                ? activeLabel
                : parentId.ToString("D");
            if (TryAddSimulatorSeedParent(parentId, label, source: "file", updateStatus: false))
            {
                added++;
            }
        }

        SimulatorStatus = added > 0
            ? $"Added {added} seed parent(s) from file."
            : "No new seed parents were added from file (all duplicates).";
        Status = SimulatorStatus;
    }

    private void ClearSimulatorSeedParents()
    {
        if (_simAdditionalSeedParents.Count == 0)
        {
            return;
        }

        _simAdditionalSeedParents.Clear();
        RefreshEffectiveSimulatorSeedParents();
        SimulatorStatus = "Cleared extra seed parents.";
        Status = SimulatorStatus;
    }

    private bool TryAddSimulatorSeedParent(Guid brainId, string label, string source, bool updateStatus = true)
    {
        if (brainId == Guid.Empty)
        {
            return false;
        }

        if (SimSeedParents.Any(entry => entry.BrainId == brainId)
            || _simAdditionalSeedParents.Any(entry => entry.BrainId == brainId))
        {
            if (updateStatus)
            {
                SimulatorStatus = $"Seed parent already added: {brainId:D}";
                Status = SimulatorStatus;
            }

            return false;
        }

        _simAdditionalSeedParents.Add(new SpeciationSimulatorSeedParentItem(brainId, label, source));
        RefreshEffectiveSimulatorSeedParents();
        if (updateStatus)
        {
            SimulatorStatus = $"Added seed parent: {brainId:D}";
            Status = SimulatorStatus;
        }

        return true;
    }

    private bool TryResolveSimulatorParentPool(out List<Guid> parents, out string error)
    {
        parents = new List<Guid>(2 + _simAdditionalSeedParents.Count);
        var uniqueParents = new HashSet<Guid>();

        if (!TryResolveParentBrainId(
                selected: SimSelectedParentABrain,
                overrideFilePath: SimParentAOverrideFilePath,
                parentLabel: "A",
                out var parentA,
                out error))
        {
            return false;
        }

        uniqueParents.Add(parentA);
        parents.Add(parentA);

        if (!TryResolveParentBrainId(
                selected: SimSelectedParentBBrain,
                overrideFilePath: SimParentBOverrideFilePath,
                parentLabel: "B",
                out var parentB,
                out error))
        {
            return false;
        }

        if (uniqueParents.Add(parentB))
        {
            parents.Add(parentB);
        }

        foreach (var extraParent in _simAdditionalSeedParents)
        {
            if (uniqueParents.Add(extraParent.BrainId))
            {
                parents.Add(extraParent.BrainId);
            }
        }

        if (parents.Count < 2)
        {
            error = "Simulator requires at least two distinct brain parents.";
            return false;
        }

        error = string.Empty;
        return true;
    }

    private static bool TryResolveParentBrainId(
        SpeciationSimulatorBrainOption? selected,
        string? overrideFilePath,
        string parentLabel,
        out Guid brainId,
        out string error)
    {
        if (TryReadParentOverrideGuid(overrideFilePath, parentLabel, out brainId, out error))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(overrideFilePath))
        {
            brainId = Guid.Empty;
            error = string.IsNullOrWhiteSpace(error)
                ? $"Parent {parentLabel} override file has no usable brain GUID: {overrideFilePath}"
                : error;
            return false;
        }

        if (selected is null || selected.BrainId == Guid.Empty)
        {
            brainId = Guid.Empty;
            error = $"Simulator requires Parent {parentLabel}.";
            return false;
        }

        brainId = selected.BrainId;
        error = string.Empty;
        return true;
    }

    private void RefreshEffectiveSimulatorSeedParents()
    {
        var items = new List<SpeciationSimulatorSeedParentItem>(2 + _simAdditionalSeedParents.Count);
        var seen = new HashSet<Guid>();

        AddEffectiveSimulatorSeedParent(
            items,
            seen,
            BuildSimulatorParentSlotItem(SimSelectedParentABrain, SimParentAOverrideFilePath, "A"));
        AddEffectiveSimulatorSeedParent(
            items,
            seen,
            BuildSimulatorParentSlotItem(SimSelectedParentBBrain, SimParentBOverrideFilePath, "B"));

        foreach (var extraParent in _simAdditionalSeedParents)
        {
            AddEffectiveSimulatorSeedParent(
                items,
                seen,
                extraParent with { Label = ResolveSimulatorSeedParentLabel(extraParent.BrainId, extraParent.Label) });
        }

        SimSeedParents.Clear();
        foreach (var item in items)
        {
            SimSeedParents.Add(item);
        }

        OnPropertyChanged(nameof(SimSeedParentsSummary));
    }

    private SpeciationSimulatorSeedParentItem? BuildSimulatorParentSlotItem(
        SpeciationSimulatorBrainOption? selected,
        string? overrideFilePath,
        string parentLabel)
    {
        if (TryReadParentOverrideGuid(overrideFilePath, parentLabel, out var overrideBrainId, out _))
        {
            return new SpeciationSimulatorSeedParentItem(
                overrideBrainId,
                ResolveSimulatorSeedParentLabel(overrideBrainId),
                $"Parent {parentLabel} override");
        }

        if (!string.IsNullOrWhiteSpace(overrideFilePath))
        {
            return null;
        }

        if (selected is null || selected.BrainId == Guid.Empty)
        {
            return null;
        }

        return new SpeciationSimulatorSeedParentItem(
            selected.BrainId,
            ResolveSimulatorSeedParentLabel(selected.BrainId, selected.Label),
            $"Parent {parentLabel}");
    }

    private void AddEffectiveSimulatorSeedParent(
        List<SpeciationSimulatorSeedParentItem> items,
        HashSet<Guid> seen,
        SpeciationSimulatorSeedParentItem? item)
    {
        if (item is null || item.BrainId == Guid.Empty || !seen.Add(item.BrainId))
        {
            return;
        }

        items.Add(item);
    }

    private string ResolveSimulatorSeedParentLabel(Guid brainId, string? fallbackLabel = null)
    {
        var active = SimActiveBrains.FirstOrDefault(entry => entry.BrainId == brainId);
        if (active is not null && !string.IsNullOrWhiteSpace(active.Label))
        {
            return active.Label;
        }

        return string.IsNullOrWhiteSpace(fallbackLabel)
            ? brainId.ToString("D")
            : fallbackLabel;
    }

    private static bool TryReadParentOverrideGuid(
        string? overrideFilePath,
        string parentLabel,
        out Guid brainId,
        out string error)
    {
        brainId = Guid.Empty;
        error = string.Empty;
        if (string.IsNullOrWhiteSpace(overrideFilePath))
        {
            return false;
        }

        if (!File.Exists(overrideFilePath))
        {
            error = $"Parent {parentLabel} override file not found: {overrideFilePath}";
            return false;
        }

        foreach (var rawLine in File.ReadLines(overrideFilePath))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith("#", StringComparison.Ordinal))
            {
                continue;
            }

            if (Guid.TryParse(line, out brainId) && brainId != Guid.Empty)
            {
                return true;
            }

            brainId = Guid.Empty;
            error = $"Parent {parentLabel} override file must contain a brain GUID: {overrideFilePath}";
            return false;
        }

        error = $"Parent {parentLabel} override file has no usable brain GUID: {overrideFilePath}";
        return false;
    }

    private static IReadOnlyList<Guid> ParseBrainIdsFromFile(string path, string sourceLabel)
    {
        if (!File.Exists(path))
        {
            throw new InvalidOperationException($"{sourceLabel} not found: {path}");
        }

        var parentIds = new List<Guid>();
        foreach (var rawLine in File.ReadLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith("#", StringComparison.Ordinal))
            {
                continue;
            }

            if (!Guid.TryParse(line, out var brainId) || brainId == Guid.Empty)
            {
                throw new InvalidOperationException($"Invalid brain GUID '{line}' in {path}.");
            }

            parentIds.Add(brainId);
        }

        return parentIds;
    }

    private static async Task<IStorageFile?> PickOpenFileAsync(string title)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false
        };
        var results = await provider.OpenFilePickerAsync(options).ConfigureAwait(false);
        return results.FirstOrDefault();
    }

    private static IStorageProvider? GetStorageProvider()
    {
        var window = GetMainWindow();
        return window?.StorageProvider;
    }

    private static Window? GetMainWindow()
        => Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop
            ? desktop.MainWindow
            : null;

    private static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private string BuildEvolutionSimArgs(int ioPort, int simPort, IReadOnlyList<Guid> parentBrainIds)
    {
        var normalizedParentIds = parentBrainIds
            .Where(static brainId => brainId != Guid.Empty)
            .Distinct()
            .ToList();

        if (normalizedParentIds.Count < 2)
        {
            return string.Empty;
        }

        var args = new List<string>
        {
            "run",
            $"--io-address {Connections.IoHost}:{ioPort}",
            $"--io-id {QuoteIfNeeded(Connections.IoGateway)}",
            $"--settings-address {Connections.SettingsHost}:{Math.Max(1, ParseInt(Connections.SettingsPortText, 12010))}",
            $"--settings-name {QuoteIfNeeded(Connections.SettingsName)}",
            $"--bind-host {QuoteIfNeeded(SimBindHost)}",
            $"--port {simPort}",
            $"--seed {ParseULong(SimSeedText, 12345UL)}",
            $"--interval-ms {Math.Max(0, ParseInt(SimIntervalMsText, 100))}",
            $"--status-seconds {Math.Max(1, ParseInt(SimStatusSecondsText, 2))}",
            $"--timeout-seconds {Math.Max(1, ParseInt(SimTimeoutSecondsText, 45))}",
            $"--max-iterations {Math.Max(0, ParseInt(SimMaxIterationsText, 0))}",
            $"--max-parent-pool {Math.Max(2, ParseInt(SimMaxParentPoolText, 512))}",
            $"--min-runs {Math.Max(1, ParseInt(SimMinRunsText, 1))}",
            $"--max-runs {Math.Min(64, Math.Max(1, ParseInt(SimMaxRunsText, 6)))}",
            $"--run-gamma {ParseDouble(SimGammaText, 1d).ToString("0.###", CultureInfo.InvariantCulture)}",
            $"--run-pressure-mode {NormalizeRunPressureModeToken(SimRunPressureMode)}",
            $"--parent-selection-bias {NormalizeParentSelectionBiasToken(SimParentSelectionBias)}",
            $"--commit-to-speciation {(SimCommitToSpeciation ? "true" : "false")}",
            $"--spawn-children {(SimSpawnChildren ? "true" : "false")}",
            "--json"
        };

        var advertiseHost = ResolveEvolutionSimAdvertiseHost();
        if (!string.IsNullOrWhiteSpace(advertiseHost))
        {
            args.Add($"--advertise-host {QuoteIfNeeded(advertiseHost)}");
        }

        foreach (var parentBrainId in normalizedParentIds)
        {
            args.Add($"--parent-brain {parentBrainId:D}");
        }

        return string.Join(" ", args);
    }

    private string? ResolveEvolutionSimAdvertiseHost()
    {
        if (TryResolveExplicitLocalAdvertiseHost(Connections.LocalBindHost, out var configuredLocalHost))
        {
            return configuredLocalHost;
        }

        if (TryResolveExplicitLocalAdvertiseHost(SimBindHost, out var configuredSimHost))
        {
            return configuredSimHost;
        }

        var resolved = NetworkAddressDefaults.ResolveDefaultAdvertisedHost();
        return TryResolveExplicitLocalAdvertiseHost(resolved, out var resolvedHost)
            ? resolvedHost
            : null;
    }

    private async Task<string> AppendFirewallAttentionAsync(string serviceLabel, string bindHost, int port, string launchMessage)
    {
        var firewall = await _firewallManager
            .EnsureInboundTcpAccessAsync(serviceLabel, bindHost, port)
            .ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(firewall.Message))
        {
            var logMessage = $"{serviceLabel} firewall: {firewall.Message}";
            if (firewall.RequiresAttention)
            {
                WorkbenchLog.Warn(logMessage);
            }
            else
            {
                WorkbenchLog.Info(logMessage);
            }
        }

        return firewall.RequiresAttention && !string.IsNullOrWhiteSpace(firewall.Message)
            ? $"{launchMessage} {firewall.Message}"
            : launchMessage;
    }

    private static bool TryResolveExplicitLocalAdvertiseHost(string? host, out string advertiseHost)
    {
        advertiseHost = string.Empty;
        if (string.IsNullOrWhiteSpace(host))
        {
            return false;
        }

        var trimmed = host.Trim();
        if (NetworkAddressDefaults.IsLoopbackHost(trimmed)
            || NetworkAddressDefaults.IsAllInterfaces(trimmed)
            || !NetworkAddressDefaults.IsLocalHost(trimmed))
        {
            return false;
        }

        advertiseHost = trimmed;
        return true;
    }

    private static string? ExtractLogPath(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return null;
        }

        const string token = "Logs:";
        var index = message.IndexOf(token, StringComparison.OrdinalIgnoreCase);
        if (index < 0)
        {
            return null;
        }

        var path = message[(index + token.Length)..].Trim();
        return string.IsNullOrWhiteSpace(path) ? null : path;
    }

    private static string? ReadLatestNonEmptyLine(string path, ref long position, ref string? lastNonEmptyLine)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            if (position < 0 || position > stream.Length)
            {
                position = 0;
            }

            if (stream.Length <= position)
            {
                return lastNonEmptyLine;
            }

            stream.Seek(position, SeekOrigin.Begin);
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
            var chunk = reader.ReadToEnd();
            position = stream.Position;
            if (!string.IsNullOrWhiteSpace(chunk))
            {
                foreach (var line in chunk.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
                {
                    var trimmed = line.Trim();
                    if (!string.IsNullOrWhiteSpace(trimmed))
                    {
                        lastNonEmptyLine = trimmed;
                    }
                }
            }
        }
        catch
        {
        }

        return lastNonEmptyLine;
    }

    private static bool TryParseSimulatorStatus(string rawLine, out EvolutionSimStatusSnapshot snapshot)
    {
        snapshot = default;
        if (string.IsNullOrWhiteSpace(rawLine))
        {
            return false;
        }

        var jsonIndex = rawLine.IndexOf('{');
        if (jsonIndex < 0)
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(rawLine[jsonIndex..]);
            var root = doc.RootElement;
            if (!root.TryGetProperty("type", out var typeNode)
                || !string.Equals(typeNode.GetString(), "evolution_sim_status", StringComparison.Ordinal))
            {
                return false;
            }

            snapshot = new EvolutionSimStatusSnapshot(
                SessionId: root.TryGetProperty("session_id", out var sessionIdNode) ? sessionIdNode.GetString() ?? "(unknown)" : "(unknown)",
                Running: root.TryGetProperty("running", out var runningNode) && runningNode.GetBoolean(),
                Final: root.TryGetProperty("final", out var finalNode) && finalNode.GetBoolean(),
                Iterations: root.TryGetProperty("iterations", out var iterationsNode) ? iterationsNode.GetUInt64() : 0UL,
                ParentPoolSize: root.TryGetProperty("parent_pool_size", out var poolNode) ? poolNode.GetInt32() : 0,
                CompatibilityChecks: root.TryGetProperty("compatibility_checks", out var checksNode) ? checksNode.GetUInt64() : 0UL,
                CompatiblePairs: root.TryGetProperty("compatible_pairs", out var pairsNode) ? pairsNode.GetUInt64() : 0UL,
                ReproductionCalls: root.TryGetProperty("reproduction_calls", out var callsNode) ? callsNode.GetUInt64() : 0UL,
                ReproductionFailures: root.TryGetProperty("reproduction_failures", out var failuresNode) ? failuresNode.GetUInt64() : 0UL,
                ReproductionRunsObserved: root.TryGetProperty("reproduction_runs_observed", out var runsNode) ? runsNode.GetUInt64() : 0UL,
                ReproductionRunsWithMutations: root.TryGetProperty("reproduction_runs_with_mutations", out var runsMutatedNode) ? runsMutatedNode.GetUInt64() : 0UL,
                ReproductionMutationEvents: root.TryGetProperty("reproduction_mutation_events", out var mutationEventsNode) ? mutationEventsNode.GetUInt64() : 0UL,
                SimilaritySamples: root.TryGetProperty("similarity_samples", out var similaritySamplesNode) ? similaritySamplesNode.GetUInt64() : 0UL,
                MinSimilarityObserved: TryGetJsonDouble(root, "min_similarity_observed", out var minSimilarityObserved) ? minSimilarityObserved : null,
                MaxSimilarityObserved: TryGetJsonDouble(root, "max_similarity_observed", out var maxSimilarityObserved) ? maxSimilarityObserved : null,
                AssessmentSimilaritySamples: root.TryGetProperty("assessment_similarity_samples", out var assessmentSamplesNode)
                    ? assessmentSamplesNode.GetUInt64()
                    : (root.TryGetProperty("similarity_samples", out var overallSamplesNode) ? overallSamplesNode.GetUInt64() : 0UL),
                MinAssessmentSimilarityObserved: TryGetJsonDouble(root, "min_assessment_similarity_observed", out var minAssessmentSimilarityObserved)
                    ? minAssessmentSimilarityObserved
                    : (TryGetJsonDouble(root, "min_similarity_observed", out var legacyMinAssessmentSimilarityObserved) ? legacyMinAssessmentSimilarityObserved : null),
                MaxAssessmentSimilarityObserved: TryGetJsonDouble(root, "max_assessment_similarity_observed", out var maxAssessmentSimilarityObserved)
                    ? maxAssessmentSimilarityObserved
                    : (TryGetJsonDouble(root, "max_similarity_observed", out var legacyMaxAssessmentSimilarityObserved) ? legacyMaxAssessmentSimilarityObserved : null),
                ReproductionSimilaritySamples: root.TryGetProperty("reproduction_similarity_samples", out var reproductionSamplesNode)
                    ? reproductionSamplesNode.GetUInt64()
                    : 0UL,
                MinReproductionSimilarityObserved: TryGetJsonDouble(root, "min_reproduction_similarity_observed", out var minReproductionSimilarityObserved)
                    ? minReproductionSimilarityObserved
                    : null,
                MaxReproductionSimilarityObserved: TryGetJsonDouble(root, "max_reproduction_similarity_observed", out var maxReproductionSimilarityObserved)
                    ? maxReproductionSimilarityObserved
                    : null,
                SpeciationCommitSimilaritySamples: root.TryGetProperty("speciation_commit_similarity_samples", out var commitSamplesNode)
                    ? commitSamplesNode.GetUInt64()
                    : 0UL,
                MinSpeciationCommitSimilarityObserved: TryGetJsonDouble(root, "min_speciation_commit_similarity_observed", out var minCommitSimilarityObserved)
                    ? minCommitSimilarityObserved
                    : null,
                MaxSpeciationCommitSimilarityObserved: TryGetJsonDouble(root, "max_speciation_commit_similarity_observed", out var maxCommitSimilarityObserved)
                    ? maxCommitSimilarityObserved
                    : null,
                ChildrenAddedToPool: root.TryGetProperty("children_added_to_pool", out var childrenNode) ? childrenNode.GetUInt64() : 0UL,
                SpeciationCommitAttempts: root.TryGetProperty("speciation_commit_attempts", out var attemptsNode) ? attemptsNode.GetUInt64() : 0UL,
                SpeciationCommitSuccesses: root.TryGetProperty("speciation_commit_successes", out var successNode) ? successNode.GetUInt64() : 0UL,
                LastFailure: root.TryGetProperty("last_failure", out var lastFailureNode) ? lastFailureNode.GetString() ?? string.Empty : string.Empty,
                LastSeed: root.TryGetProperty("last_seed", out var lastSeedNode) ? lastSeedNode.GetUInt64() : 0UL);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private readonly record struct EvolutionSimStatusSnapshot(
        string SessionId,
        bool Running,
        bool Final,
        ulong Iterations,
        int ParentPoolSize,
        ulong CompatibilityChecks,
        ulong CompatiblePairs,
        ulong ReproductionCalls,
        ulong ReproductionFailures,
        ulong ReproductionRunsObserved,
        ulong ReproductionRunsWithMutations,
        ulong ReproductionMutationEvents,
        ulong SimilaritySamples,
        double? MinSimilarityObserved,
        double? MaxSimilarityObserved,
        ulong AssessmentSimilaritySamples,
        double? MinAssessmentSimilarityObserved,
        double? MaxAssessmentSimilarityObserved,
        ulong ReproductionSimilaritySamples,
        double? MinReproductionSimilarityObserved,
        double? MaxReproductionSimilarityObserved,
        ulong SpeciationCommitSimilaritySamples,
        double? MinSpeciationCommitSimilarityObserved,
        double? MaxSpeciationCommitSimilarityObserved,
        ulong ChildrenAddedToPool,
        ulong SpeciationCommitAttempts,
        ulong SpeciationCommitSuccesses,
        string LastFailure,
        ulong LastSeed);

    private enum SimulatorParentFileKind
    {
        ParentAOverride,
        ParentBOverride
    }
}
