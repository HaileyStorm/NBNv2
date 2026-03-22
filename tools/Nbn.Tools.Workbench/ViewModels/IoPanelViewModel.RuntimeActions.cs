using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class IoPanelViewModel
{
    public void ApplyEnergyCreditSelected()
    {
        QueueSelectedBrainCommand(ApplyEnergyCreditSelectedAsync);
    }

    public void ApplyEnergyRateSelected()
    {
        QueueSelectedBrainCommand(ApplyEnergyRateSelectedAsync);
    }

    public void ApplyCostEnergySelected()
    {
        QueueSelectedBrainCommand(ApplyCostEnergySelectedAsync);
    }

    public void ApplyPlasticitySelected()
    {
        QueueSelectedBrainCommand(ApplyPlasticitySelectedAsync);
    }

    public bool TrySendInputSelected(uint index, float value, out string status)
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            status = "No brain selected.";
            return false;
        }

        if (!float.IsFinite(value))
        {
            BrainInfoSummary = "Input value invalid.";
            status = "Input value invalid.";
            return false;
        }

        _client.SendInput(brainId, index, value);
        status = FormattableString.Invariant($"Input pulse queued: brain {brainId:D}, index {index}, value {value:0.###}.");
        BrainInfoSummary = status;
        return true;
    }

    public bool TrySendRuntimeNeuronPulseSelected(uint regionId, uint neuronId, float value, out string status)
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            status = "No brain selected.";
            return false;
        }

        if (!float.IsFinite(value))
        {
            BrainInfoSummary = "Pulse value invalid.";
            status = "Pulse value invalid.";
            return false;
        }

        _client.SendRuntimeNeuronPulse(brainId, regionId, neuronId, value);
        status = FormattableString.Invariant($"Runtime pulse queued: brain {brainId:D}, R{regionId}/N{neuronId}, value {value:0.###}.");
        BrainInfoSummary = status;
        return true;
    }

    public bool TrySetRuntimeNeuronStateSelected(
        uint regionId,
        uint neuronId,
        bool setBuffer,
        float bufferValue,
        bool setAccumulator,
        float accumulatorValue,
        out string status)
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            status = "No brain selected.";
            return false;
        }

        if (!setBuffer && !setAccumulator)
        {
            status = "Specify at least one runtime value.";
            BrainInfoSummary = status;
            return false;
        }

        if (setBuffer && !float.IsFinite(bufferValue))
        {
            status = "Buffer value invalid.";
            BrainInfoSummary = status;
            return false;
        }

        if (setAccumulator && !float.IsFinite(accumulatorValue))
        {
            status = "Accumulator value invalid.";
            BrainInfoSummary = status;
            return false;
        }

        _client.SendRuntimeNeuronStateWrite(
            brainId,
            regionId,
            neuronId,
            setBuffer,
            bufferValue,
            setAccumulator,
            accumulatorValue);

        var updates = new List<string>(2);
        if (setBuffer)
        {
            updates.Add(FormattableString.Invariant($"buffer={bufferValue:0.###}"));
        }

        if (setAccumulator)
        {
            updates.Add(FormattableString.Invariant($"accumulator={accumulatorValue:0.###}"));
        }

        status = FormattableString.Invariant(
            $"Runtime state queued: brain {brainId:D}, R{regionId}/N{neuronId}, {string.Join(", ", updates)}.");
        BrainInfoSummary = status;
        return true;
    }

    private void QueueSelectedBrainCommand(Func<Task> command)
    {
        _ = RunSelectedBrainCommandAsync(command);
    }

    private async Task RunSelectedBrainCommandAsync(Func<Task> command)
    {
        await _selectedBrainCommandGate.WaitAsync().ConfigureAwait(false);
        try
        {
            await command().ConfigureAwait(false);
        }
        finally
        {
            _selectedBrainCommandGate.Release();
        }
    }

    private async Task ApplyEnergyCreditSelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        if (!long.TryParse(EnergyCreditText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
        {
            BrainInfoSummary = "Credit value invalid.";
            return;
        }

        var result = await _client.SendEnergyCreditAsync(brainId, amount).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy credit", new[] { result });
    }

    private async Task ApplyEnergyRateSelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        if (!long.TryParse(EnergyRateText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rate))
        {
            BrainInfoSummary = "Rate value invalid.";
            return;
        }

        var result = await _client.SendEnergyRateAsync(brainId, rate).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy rate", new[] { result });
    }

    private async Task ApplyCostEnergySelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        var enabled = CostEnergyEnabled;
        var result = await _client.SetCostEnergyAsync(brainId, enabled, enabled).ConfigureAwait(false);
        ApplyCommandResultToSummary("Cost/Energy flags", new[] { result });
    }

    private async Task ApplyPlasticitySelectedAsync()
    {
        if (!TryGetSelectedBrain(out var brainId))
        {
            return;
        }

        if (!TryParsePlasticityRate(out var rate))
        {
            BrainInfoSummary = "Plasticity rate invalid.";
            return;
        }

        var probabilistic = SelectedPlasticityMode?.Probabilistic ?? true;
        var enabled = PlasticityEnabled;
        var result = await _client.SetPlasticityAsync(brainId, enabled, rate, probabilistic).ConfigureAwait(false);
        ApplyCommandResultToSummary("Plasticity", new[] { result });
    }
}
