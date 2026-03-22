using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class IoPanelViewModel
{
    private void Subscribe(bool vector)
    {
        if (!TryGetPreferredBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        _client.SubscribeOutputs(brainId, vector);
    }

    private void Unsubscribe(bool vector)
    {
        if (!TryGetPreferredBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        _client.UnsubscribeOutputs(brainId, vector);
    }

    private void SendInput()
    {
        if (!TryGetPreferredBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        if (!uint.TryParse(InputIndexText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index))
        {
            BrainInfoSummary = "Input index invalid.";
            return;
        }

        if (!float.TryParse(InputValueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
        {
            BrainInfoSummary = "Input value invalid.";
            return;
        }

        LogInputDiagnostic(
            $"IoSendInput selected={FormatBrainId(_selectedBrainId)} brainIdText={BrainIdText} resolved={brainId:D} index={index} value={value:0.###}");
        _client.SendInput(brainId, index, value);
    }

    private void ClearOutputs()
    {
        OutputEvents.Clear();
        VectorEvents.Clear();
    }

    private void ClearVectorOutputs()
    {
        VectorEvents.Clear();
    }

    private void ToggleVectorUiUpdates()
    {
        PauseVectorUiUpdates = !PauseVectorUiUpdates;
    }

    private bool TryGetBrainId(out Guid brainId)
    {
        if (Guid.TryParse(BrainIdText, out brainId))
        {
            return true;
        }

        brainId = Guid.Empty;
        return false;
    }

    private bool TryGetPreferredBrainId(out Guid brainId)
    {
        if (_selectedBrainId.HasValue)
        {
            brainId = _selectedBrainId.Value;
            return true;
        }

        return TryGetBrainId(out brainId);
    }

    private bool TryGetSelectedBrain(out Guid brainId)
    {
        if (_selectedBrainId.HasValue)
        {
            brainId = _selectedBrainId.Value;
            return true;
        }

        BrainInfoSummary = "No brain selected.";
        brainId = Guid.Empty;
        return false;
    }

    private void LogInputDiagnostic(string message)
    {
        if (!LogInputDiagnostics || !WorkbenchLog.Enabled)
        {
            return;
        }

        WorkbenchLog.Info(message);
    }

    private static string FormatBrainId(Guid? brainId)
        => brainId.HasValue ? brainId.Value.ToString("D") : "none";

    private static string PreviewValues(IReadOnlyList<float> values)
    {
        if (values.Count == 0)
        {
            return "(empty)";
        }

        var take = Math.Min(values.Count, 8);
        var preview = string.Join(",", values.Take(take).Select(static value => value.ToString("0.###", CultureInfo.InvariantCulture)));
        return values.Count > take ? $"{preview},...({values.Count})" : preview;
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";
    }

}
