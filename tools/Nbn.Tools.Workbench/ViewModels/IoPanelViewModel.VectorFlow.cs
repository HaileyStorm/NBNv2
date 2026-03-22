using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class IoPanelViewModel
{
    public void AddOutputEvent(OutputEventItem item)
    {
        if (_selectedBrainId is not null && !string.Equals(item.BrainId, _selectedBrainId.Value.ToString("D"), StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var brainId = Guid.Empty;
        var shouldObserveTick =
            AutoSendInputVectorEveryTick
            && _selectedBrainId.HasValue
            && Guid.TryParse(item.BrainId, out brainId);

        _dispatcher.Post(() =>
        {
            LastOutputTickLabel = item.TickId.ToString();
            if (FilterZeroOutputs && item.IsZero)
            {
                return;
            }

            OutputEvents.Insert(0, item);
            Trim(OutputEvents);
        });

        if (shouldObserveTick)
        {
            ObserveTick(brainId, item.TickId);
        }
    }

    public void AddVectorEvent(OutputVectorEventItem item)
    {
        if (_selectedBrainId is not null && !string.Equals(item.BrainId, _selectedBrainId.Value.ToString("D"), StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        _dispatcher.Post(() =>
        {
            LastOutputTickLabel = item.TickId.ToString();
            if (PauseVectorUiUpdates)
            {
                return;
            }

            if (FilterZeroVectorOutputs && item.AllZero)
            {
                return;
            }

            VectorEvents.Insert(0, item);
            Trim(VectorEvents);
        });
    }

    public void ObserveTick(Guid brainId, ulong tickId)
    {
        _dispatcher.Post(() =>
        {
            if (!_selectedBrainId.HasValue || _selectedBrainId.Value != brainId)
            {
                return;
            }

            TryAutoSendInputVectorForTick(brainId, tickId);
        });
    }

    private void SendVector()
    {
        if (!TryGetPreferredBrainId(out var brainId))
        {
            BrainInfoSummary = "Invalid BrainId.";
            return;
        }

        if (!TryGetValidatedInputVector(brainId, updateSummaryOnFailure: true, out var values))
        {
            return;
        }

        LogInputDiagnostic(
            $"IoSendVector selected={FormatBrainId(_selectedBrainId)} brainIdText={BrainIdText} resolved={brainId:D} width={values.Count} values={PreviewValues(values)}");
        DispatchInputVector(brainId, values);
    }

    private void TryAutoSendInputVectorForTick(Guid brainId, ulong tickId)
    {
        if (!AutoSendInputVectorEveryTick
            || !AutoSendInputVectorEveryTickAvailable
            || !_selectedBrainId.HasValue
            || _selectedBrainId.Value != brainId)
        {
            return;
        }

        if (_hasLastAutoVectorSendTick
            && _lastAutoVectorSendBrainId == brainId
            && _lastAutoVectorSendTickId == tickId)
        {
            return;
        }

        if (!TryGetValidatedInputVector(brainId, updateSummaryOnFailure: true, out var values))
        {
            return;
        }

        _lastAutoVectorSendBrainId = brainId;
        _lastAutoVectorSendTickId = tickId;
        _hasLastAutoVectorSendTick = true;
        LogInputDiagnostic(
            $"IoAutoSendVector selected={FormatBrainId(_selectedBrainId)} brainIdText={BrainIdText} resolved={brainId:D} tick={tickId} width={values.Count} values={PreviewValues(values)}");
        DispatchInputVector(brainId, values);
    }

    private void DispatchInputVector(Guid brainId, IReadOnlyList<float> values)
    {
        _client.SendInputVector(brainId, values);
        MaybeRandomizeInputVectorAfterSend(values.Count);
    }

    private void MaybeRandomizeInputVectorAfterSend(int inputWidth)
    {
        if (!RandomizeInputVectorAfterEverySend || inputWidth <= 0)
        {
            return;
        }

        var suggestedVector = _buildSuggestedVector(inputWidth);
        _lastSuggestedInputVector = suggestedVector;
        InputVectorText = suggestedVector;
    }

    private void ResetAutoVectorSendTickGate()
    {
        _lastAutoVectorSendBrainId = Guid.Empty;
        _lastAutoVectorSendTickId = 0;
        _hasLastAutoVectorSendTick = false;
    }

    private bool TryGetValidatedInputVector(Guid brainId, bool updateSummaryOnFailure, out IReadOnlyList<float> values)
    {
        if (!TryParseVector(InputVectorText, out values, out var parseError))
        {
            if (updateSummaryOnFailure)
            {
                BrainInfoSummary = parseError;
            }

            return false;
        }

        if (!TryValidateInputVectorWidth(brainId, values.Count, out var widthError))
        {
            if (updateSummaryOnFailure)
            {
                BrainInfoSummary = widthError;
            }

            return false;
        }

        return true;
    }

    private bool TryValidateInputVectorWidth(Guid brainId, int width, out string error)
    {
        if (!_selectedBrainId.HasValue
            || _selectedBrainId.Value != brainId
            || _selectedBrainInputWidth < 0
            || _selectedBrainInputWidth == width)
        {
            error = string.Empty;
            return true;
        }

        error = FormattableString.Invariant(
            $"Input vector width mismatch for brain {brainId:D}: expected {_selectedBrainInputWidth}, got {width}.");
        return false;
    }

    private static bool TryParseVector(string raw, out IReadOnlyList<float> values, out string error)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            values = Array.Empty<float>();
            error = "Vector is empty.";
            return false;
        }

        var parts = raw.Split(',', StringSplitOptions.TrimEntries);
        var parsedValues = new List<float>(parts.Length);
        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            if (string.IsNullOrWhiteSpace(part))
            {
                values = Array.Empty<float>();
                error = FormattableString.Invariant($"Vector value #{i + 1} is empty.");
                return false;
            }

            if (!float.TryParse(part, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || !float.IsFinite(value))
            {
                values = Array.Empty<float>();
                error = FormattableString.Invariant($"Vector value #{i + 1} is invalid.");
                return false;
            }

            parsedValues.Add(value);
        }

        if (parsedValues.Count == 0)
        {
            values = Array.Empty<float>();
            error = "Vector is empty.";
            return false;
        }

        values = parsedValues;
        error = string.Empty;
        return true;
    }

    private static void Trim<T>(ObservableCollection<T> collection)
    {
        while (collection.Count > MaxEvents)
        {
            collection.RemoveAt(collection.Count - 1);
        }
    }

    private static string BuildSuggestedVector(int inputWidth)
    {
        if (inputWidth <= 0)
        {
            return string.Empty;
        }

        var count = inputWidth;
        const double minMagnitude = 0.15d;
        var values = new string[count];
        for (var i = 0; i < count; i++)
        {
            var magnitude = minMagnitude + ((1d - minMagnitude) * Math.Sqrt(Random.Shared.NextDouble()));
            var sign = Random.Shared.Next(0, 2) == 0 ? -1d : 1d;
            var value = sign * magnitude;
            values[i] = value.ToString("0.##", CultureInfo.InvariantCulture);
        }

        return string.Join(",", values);
    }
}
