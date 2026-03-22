using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class IoPanelViewModel
{
    public void SelectBrain(Guid? brainId, bool preserveOutputs = false)
    {
        if (_selectedBrainId == brainId)
        {
            return;
        }

        var previousBrainId = _selectedBrainId;

        if (_selectedBrainId.HasValue)
        {
            _client.UnsubscribeOutputs(_selectedBrainId.Value, vector: false);
            _client.UnsubscribeOutputs(_selectedBrainId.Value, vector: true);
        }

        _selectedBrainId = brainId;
        _selectedBrainInputWidth = -1;
        _selectedBrainInputReplayEveryTick = false;
        OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
        BrainIdText = brainId?.ToString("D") ?? string.Empty;
        LogInputDiagnostic(
            $"IoSelectBrain previous={FormatBrainId(previousBrainId)} next={FormatBrainId(brainId)} brainIdText={BrainIdText}");
        TrackKnownBrain(brainId);
        ResetAutoVectorSendTickGate();
        if (!preserveOutputs)
        {
            OutputEvents.Clear();
            VectorEvents.Clear();
            LastOutputTickLabel = "-";
        }

        if (brainId.HasValue)
        {
            _client.SubscribeOutputs(brainId.Value, vector: false);
            _client.SubscribeOutputs(brainId.Value, vector: true);
            RequestBrainInfo(brainId.Value);
        }
        else
        {
            BrainInfoSummary = "No brain selected.";
        }
    }

    public void EnsureSelectedBrain(Guid brainId)
    {
        if (_selectedBrainId.HasValue && _selectedBrainId.Value == brainId)
        {
            RefreshSubscriptions();
            return;
        }

        SelectBrain(brainId, preserveOutputs: true);
    }

    public Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
        => _client.RequestBrainInfoAsync(brainId);

    public Task<HiveMindStatus?> GetHiveMindStatusAsync()
        => _client.GetHiveMindStatusAsync();

    public Task<Nbn.Proto.ArtifactRef?> ExportBrainDefinitionReferenceAsync(Guid brainId, bool rebaseOverlays = false)
        => _client.ExportBrainDefinitionAsync(brainId, rebaseOverlays);

    public Task<SetTickRateOverrideAck?> SetTickRateOverrideAsync(float? targetTickHz)
        => _client.SetTickRateOverrideAsync(targetTickHz);

    public Task<SettingValue?> SetSettingAsync(string key, string value)
        => _client.SetSettingAsync(key, value);

    public void UpdateActiveBrains(IReadOnlyList<Guid> brains)
    {
        _activeBrains = brains.Distinct().ToList();
        ActiveBrainsSummary = _activeBrains.Count == 0
            ? "No active brains loaded."
            : $"Active brains: {_activeBrains.Count}";

        var activeSet = new HashSet<Guid>(_activeBrains);
        foreach (var option in _knownBrainsById.Values)
        {
            option.IsActive = activeSet.Contains(option.BrainId);
        }

        foreach (var brainId in _activeBrains)
        {
            if (_knownBrainsById.ContainsKey(brainId))
            {
                continue;
            }

            var option = new KnownBrainOption(brainId, isActive: true);
            _knownBrainsById[brainId] = option;
            KnownBrains.Add(option);
        }

        if (SelectedFeedbackBrain is null && KnownBrains.Count > 0)
        {
            SelectedFeedbackBrain = KnownBrains[0];
        }
    }

    public void RefreshSubscriptions()
    {
        if (!_selectedBrainId.HasValue)
        {
            return;
        }

        _client.SubscribeOutputs(_selectedBrainId.Value, vector: false);
        _client.SubscribeOutputs(_selectedBrainId.Value, vector: true);
        RequestBrainInfo(_selectedBrainId.Value);
    }

    private void RequestBrainInfo(Guid brainId)
    {
        _ = _client.RequestBrainInfoAsync(brainId, info => ApplyBrainInfoForSelection(brainId, info));
    }

    private async Task RequestInfoAsync()
    {
        if (!TryGetPreferredBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        LogInputDiagnostic(
            $"IoRequestInfo selected={FormatBrainId(_selectedBrainId)} brainIdText={BrainIdText} resolved={brainId:D}");

        await _client.RequestBrainInfoAsync(brainId, info => ApplyBrainInfoForSelection(brainId, info));
    }

    private void ApplyBrainInfo(BrainInfo? info)
    {
        ApplyBrainInfoForSelection(_selectedBrainId, info);
    }

    private void ApplyBrainInfoForSelection(Guid? requestedBrainId, BrainInfo? info)
    {
        if (requestedBrainId.HasValue
            && (!_selectedBrainId.HasValue || _selectedBrainId.Value != requestedBrainId.Value))
        {
            return;
        }

        if (info is null)
        {
            _selectedBrainInputWidth = -1;
            _selectedBrainInputReplayEveryTick = false;
            OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
            BrainInfoSummary = "Brain not found or IO unavailable.";
            return;
        }

        var inputWidth = checked((int)Math.Max(0, info.InputWidth));
        _selectedBrainInputWidth = inputWidth;
        _selectedBrainInputReplayEveryTick =
            info.InputCoordinatorMode == InputCoordinatorMode.ReplayLatestVector;
        OnPropertyChanged(nameof(AutoSendInputVectorEveryTickAvailable));
        if (_selectedBrainInputReplayEveryTick && AutoSendInputVectorEveryTick)
        {
            AutoSendInputVectorEveryTick = false;
        }
        CostEnabled = info.CostEnabled;
        EnergyEnabled = info.EnergyEnabled;
        PlasticityEnabled = info.PlasticityEnabled;
        HomeostasisEnabled = info.HomeostasisEnabled;
        HomeostasisEnergyCouplingEnabled = info.HomeostasisEnergyCouplingEnabled;
        EnergyRateText = info.EnergyRateUnitsPerSecond.ToString(CultureInfo.InvariantCulture);
        PlasticityRateText = info.PlasticityRate.ToString("0.######", CultureInfo.InvariantCulture);
        HomeostasisBaseProbabilityText = info.HomeostasisBaseProbability.ToString("0.######", CultureInfo.InvariantCulture);
        HomeostasisMinStepCodesText = info.HomeostasisMinStepCodes.ToString(CultureInfo.InvariantCulture);
        HomeostasisEnergyTargetScaleText = info.HomeostasisEnergyTargetScale.ToString("0.######", CultureInfo.InvariantCulture);
        HomeostasisEnergyProbabilityScaleText = info.HomeostasisEnergyProbabilityScale.ToString("0.######", CultureInfo.InvariantCulture);

        var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == info.PlasticityProbabilisticUpdates);
        if (selectedMode is not null)
        {
            SelectedPlasticityMode = selectedMode;
        }

        var selectedTargetMode = HomeostasisTargetModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisTargetMode);
        if (selectedTargetMode is not null)
        {
            SelectedHomeostasisTargetMode = selectedTargetMode;
        }

        var selectedUpdateMode = HomeostasisUpdateModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisUpdateMode);
        if (selectedUpdateMode is not null)
        {
            SelectedHomeostasisUpdateMode = selectedUpdateMode;
        }

        var shouldRegenerateSuggestion = string.IsNullOrWhiteSpace(InputVectorText);
        if (!shouldRegenerateSuggestion
            && string.Equals(InputVectorText, _lastSuggestedInputVector, StringComparison.Ordinal))
        {
            if (!TryParseVector(InputVectorText, out var existingSuggestedValues, out _)
                || existingSuggestedValues.Count != inputWidth)
            {
                shouldRegenerateSuggestion = true;
            }
        }

        if (shouldRegenerateSuggestion)
        {
            var suggestedVector = _buildSuggestedVector(inputWidth);
            _lastSuggestedInputVector = suggestedVector;
            InputVectorText = suggestedVector;
        }

        BrainInfoSummary = BuildBrainSummary(info);
    }

    private void TrackKnownBrain(Guid? brainId)
    {
        if (!brainId.HasValue)
        {
            return;
        }

        if (_knownBrainsById.ContainsKey(brainId.Value))
        {
            return;
        }

        var option = new KnownBrainOption(brainId.Value, isActive: _activeBrains.Contains(brainId.Value));
        _knownBrainsById[brainId.Value] = option;
        KnownBrains.Add(option);
    }

    private async Task RefreshFeedbackBrainSummaryAsync()
    {
        var selected = SelectedFeedbackBrain;
        if (selected is null)
        {
            FeedbackBrainSummary = "No known brain selected.";
            return;
        }

        var brainId = selected.BrainId;
        var info = await _client.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            if (SelectedFeedbackBrain?.BrainId != brainId)
            {
                return;
            }

            if (info is null)
            {
                FeedbackBrainSummary = $"Brain {brainId:D}: unavailable.";
                return;
            }

            FeedbackBrainSummary = BuildBrainSummary(info);
        });
    }

    private string BuildBrainSummary(BrainInfo info)
    {
        var plasticityModeLabel = info.PlasticityProbabilisticUpdates ? "probabilistic" : "absolute";
        var homeostasisTargetLabel = HomeostasisTargetModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisTargetMode)?.Label
                                     ?? info.HomeostasisTargetMode.ToString();
        var homeostasisUpdateLabel = HomeostasisUpdateModes.FirstOrDefault(mode => mode.Mode == info.HomeostasisUpdateMode)?.Label
                                     ?? info.HomeostasisUpdateMode.ToString();
        var inputCoordinatorModeLabel = FormatInputCoordinatorMode(info.InputCoordinatorMode);
        var outputVectorSourceLabel = FormatOutputVectorSource(info.OutputVectorSource);
        return
            $"Inputs: {info.InputWidth} | Outputs: {info.OutputWidth} | IO: input={inputCoordinatorModeLabel}, output_vector={outputVectorSourceLabel} | Energy: {info.EnergyRemaining} @ {info.EnergyRateUnitsPerSecond}/s | LastCost: {info.LastTickCost} | Plasticity: {(info.PlasticityEnabled ? "on" : "off")} ({plasticityModeLabel}, {info.PlasticityRate:0.######}) | Homeostasis: {(info.HomeostasisEnabled ? "on" : "off")} ({homeostasisTargetLabel}, {homeostasisUpdateLabel}, p={info.HomeostasisBaseProbability:0.######}, step={info.HomeostasisMinStepCodes}, coupling={(info.HomeostasisEnergyCouplingEnabled ? "on" : "off")})";
    }
}
