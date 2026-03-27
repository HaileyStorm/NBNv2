using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class IoPanelViewModel
{
    /// <summary>
    /// Applies SettingsMonitor-backed IO policy values into the panel's draft and runtime mirrors.
    /// </summary>
    public bool ApplySetting(SettingItem item)
    {
        if (item is null || string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        if (string.Equals(item.Key, CostEnergySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(item.Value, SystemCostEnergyEnabled);
            ApplySystemSettingsSync(() =>
            {
                SystemCostEnergyEnabled = parsed;
                SystemCostEnergyEnabledDraft = parsed;
            });

            return true;
        }

        if (string.Equals(item.Key, PlasticitySettingsKeys.SystemEnabledKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(item.Value, SystemPlasticityEnabled);
            ApplySystemSettingsSync(() =>
            {
                SystemPlasticityEnabled = parsed;
                SystemPlasticityEnabledDraft = parsed;
            });

            return true;
        }

        if (string.Equals(item.Key, PlasticitySettingsKeys.SystemRateKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseSystemPlasticityRateText(item.Value, fallback: SystemPlasticityRateText);
            SystemPlasticityRateText = parsed;
            ApplySystemSettingsSync(() =>
            {
                SystemPlasticityRateTextDraft = parsed;
            });

            return true;
        }

        if (string.Equals(item.Key, PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseBooleanSetting(item.Value, SystemPlasticityProbabilisticUpdates);
            SystemPlasticityProbabilisticUpdates = parsed;
            var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == parsed) ?? PlasticityModes[0];
            ApplySystemSettingsSync(() =>
            {
                SystemPlasticityProbabilisticUpdatesDraft = parsed;
                SelectedSystemPlasticityModeDraft = selectedMode;
            });

            return true;
        }

        if (string.Equals(item.Key, IoCoordinatorSettingsKeys.InputCoordinatorModeKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseInputCoordinatorModeSetting(item.Value, SelectedInputCoordinatorMode.Mode);
            var selectedMode = InputCoordinatorModes.FirstOrDefault(option => option.Mode == parsed) ?? InputCoordinatorModes[0];
            ApplySystemSettingsSync(() =>
            {
                SelectedInputCoordinatorMode = selectedMode;
                SelectedInputCoordinatorModeDraft = selectedMode;
            });

            return true;
        }

        if (string.Equals(item.Key, IoCoordinatorSettingsKeys.OutputVectorSourceKey, StringComparison.OrdinalIgnoreCase))
        {
            var parsed = ParseOutputVectorSourceSetting(item.Value, SelectedOutputVectorSource.Source);
            var selectedSource = OutputVectorSources.FirstOrDefault(option => option.Source == parsed) ?? OutputVectorSources[0];
            ApplySystemSettingsSync(() =>
            {
                SelectedOutputVectorSource = selectedSource;
                SelectedOutputVectorSourceDraft = selectedSource;
            });

            return true;
        }

        return false;
    }

    private void ApplySystemSettingsSync(Action update)
    {
        _systemSettingsSyncInProgress = true;
        try
        {
            update();
        }
        finally
        {
            _systemSettingsSyncInProgress = false;
        }
    }

    private async Task ApplyEnergyCreditAsync()
    {
        if (!long.TryParse(EnergyCreditText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
        {
            BrainInfoSummary = "Credit value invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var results = await Task.WhenAll(targets.Select(brainId => _client.SendEnergyCreditAsync(brainId, amount))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy credit", results);
    }

    private async Task ApplyEnergyRateAsync()
    {
        if (!long.TryParse(EnergyRateText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rate))
        {
            BrainInfoSummary = "Rate value invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var results = await Task.WhenAll(targets.Select(brainId => _client.SendEnergyRateAsync(brainId, rate))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Energy rate", results);
    }

    private async Task ApplyCostEnergyAsync()
    {
        var enabled = SystemCostEnergyEnabledDraft;
        var result = await _client.SetSettingAsync(CostEnergySettingsKeys.SystemEnabledKey, enabled ? "true" : "false").ConfigureAwait(false);
        ApplySystemBooleanSettingToSummary("Cost/Energy system policy", CostEnergySettingsKeys.SystemEnabledKey, enabled, result);
    }

    private async Task ApplyPlasticityAsync()
    {
        var enabled = SystemPlasticityEnabledDraft;
        var result = await _client.SetSettingAsync(PlasticitySettingsKeys.SystemEnabledKey, enabled ? "true" : "false").ConfigureAwait(false);
        ApplySystemBooleanSettingToSummary("Plasticity system policy", PlasticitySettingsKeys.SystemEnabledKey, enabled, result);
    }

    private async Task ApplyInputCoordinatorModeAsync()
    {
        var selected = SelectedInputCoordinatorModeDraft ?? InputCoordinatorModes[0];
        var result = await _client.SetSettingAsync(
                IoCoordinatorSettingsKeys.InputCoordinatorModeKey,
                selected.SettingValue)
            .ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            if (result is null)
            {
                BrainInfoSummary = "Input coordinator mode: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(result.Key, IoCoordinatorSettingsKeys.InputCoordinatorModeKey, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = $"Input coordinator mode: update returned unexpected key '{result.Key}'.";
                return;
            }

            ApplySetting(new SettingItem(result.Key, result.Value, result.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var persisted = ParseInputCoordinatorModeSetting(result.Value, selected.Mode);
            BrainInfoSummary = $"Input coordinator mode: applied ({FormatInputCoordinatorMode(persisted)}).";
            if (_selectedBrainId.HasValue)
            {
                RequestBrainInfo(_selectedBrainId.Value);
            }
        });
    }

    private async Task ApplyOutputVectorSourceAsync()
    {
        var selected = SelectedOutputVectorSourceDraft ?? OutputVectorSources[0];
        var result = await _client.SetSettingAsync(
                IoCoordinatorSettingsKeys.OutputVectorSourceKey,
                selected.SettingValue)
            .ConfigureAwait(false);
        _dispatcher.Post(() =>
        {
            if (result is null)
            {
                BrainInfoSummary = "Output vector source: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(result.Key, IoCoordinatorSettingsKeys.OutputVectorSourceKey, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = $"Output vector source: update returned unexpected key '{result.Key}'.";
                return;
            }

            ApplySetting(new SettingItem(result.Key, result.Value, result.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var persisted = ParseOutputVectorSourceSetting(result.Value, selected.Source);
            BrainInfoSummary = $"Output vector source: applied ({FormatOutputVectorSource(persisted)}).";
            if (_selectedBrainId.HasValue)
            {
                RequestBrainInfo(_selectedBrainId.Value);
            }
        });
    }

    private async Task ApplySystemPlasticityModeRateAsync()
    {
        if (!TryParseSystemPlasticityRateDraft(out var rate, out var rateText))
        {
            BrainInfoSummary = "Plasticity system rate invalid.";
            return;
        }

        var probabilistic = SelectedSystemPlasticityModeDraft?.Probabilistic ?? SystemPlasticityProbabilisticUpdatesDraft;
        var modeValue = probabilistic ? "true" : "false";
        var rateResult = await _client.SetSettingAsync(PlasticitySettingsKeys.SystemRateKey, rateText).ConfigureAwait(false);
        var modeResult = await _client.SetSettingAsync(PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, modeValue).ConfigureAwait(false);
        var runtimeApplied = 0;
        var runtimeSkipped = 0;
        var runtimeFailed = 0;

        if (TryGetTargetBrains(out var targets) && targets.Count > 0)
        {
            foreach (var brainId in targets)
            {
                var info = await _client.RequestBrainInfoAsync(brainId).ConfigureAwait(false);
                if (info is null)
                {
                    runtimeSkipped++;
                    continue;
                }

                var result = await _client.SetPlasticityAsync(
                        brainId,
                        info.PlasticityEnabled,
                        rate,
                        probabilistic)
                    .ConfigureAwait(false);
                if (result.Success)
                {
                    runtimeApplied++;
                }
                else
                {
                    runtimeFailed++;
                }
            }
        }

        _dispatcher.Post(() =>
        {
            if (rateResult is null || modeResult is null)
            {
                BrainInfoSummary = "Plasticity system mode/rate: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(rateResult.Key, PlasticitySettingsKeys.SystemRateKey, StringComparison.OrdinalIgnoreCase)
                || !string.Equals(modeResult.Key, PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = "Plasticity system mode/rate: update returned unexpected keys.";
                return;
            }

            ApplySetting(new SettingItem(rateResult.Key, rateResult.Value, rateResult.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            ApplySetting(new SettingItem(modeResult.Key, modeResult.Value, modeResult.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var appliedMode = ParseBooleanSetting(modeResult.Value, probabilistic) ? "probabilistic" : "absolute";
            BrainInfoSummary = $"Plasticity system mode/rate: applied ({appliedMode}, rate={rate:0.######}); runtime updated={runtimeApplied}, skipped={runtimeSkipped}, failed={runtimeFailed}.";
        });
    }

    private void ApplySystemBooleanSettingToSummary(string operation, string key, bool requested, SettingValue? result)
    {
        _dispatcher.Post(() =>
        {
            if (result is null)
            {
                BrainInfoSummary = $"{operation}: update failed (settings unavailable).";
                return;
            }

            if (!string.Equals(result.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                BrainInfoSummary = $"{operation}: update returned unexpected key '{result.Key}'.";
                return;
            }

            ApplySetting(new SettingItem(result.Key, result.Value, result.UpdatedMs.ToString(CultureInfo.InvariantCulture)));
            var persisted = ParseBooleanSetting(result.Value, requested);
            BrainInfoSummary = $"{operation}: {(persisted ? "enabled" : "disabled")}.";
        });
    }

    private async Task ApplyHomeostasisAsync()
    {
        if (!TryParseHomeostasisBaseProbability(out var baseProbability))
        {
            BrainInfoSummary = "Homeostasis probability invalid.";
            return;
        }

        if (!TryParseHomeostasisMinStepCodes(out var minStepCodes))
        {
            BrainInfoSummary = "Homeostasis min-step-codes invalid.";
            return;
        }

        if (!TryParseHomeostasisScale(HomeostasisEnergyTargetScaleText, out var energyTargetScale))
        {
            BrainInfoSummary = "Homeostasis energy-target-scale invalid.";
            return;
        }

        if (!TryParseHomeostasisScale(HomeostasisEnergyProbabilityScaleText, out var energyProbabilityScale))
        {
            BrainInfoSummary = "Homeostasis energy-probability-scale invalid.";
            return;
        }

        if (!TryGetTargetBrains(out var targets))
        {
            return;
        }

        var enabled = HomeostasisEnabled;
        var targetMode = SelectedHomeostasisTargetMode?.Mode ?? HomeostasisTargetMode.HomeostasisTargetZero;
        var updateMode = SelectedHomeostasisUpdateMode?.Mode ?? HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep;
        var energyCouplingEnabled = HomeostasisEnergyCouplingEnabled;
        var results = await Task.WhenAll(targets.Select(brainId => _client.SetHomeostasisAsync(
            brainId,
            enabled,
            targetMode,
            updateMode,
            baseProbability,
            minStepCodes,
            energyCouplingEnabled,
            energyTargetScale,
            energyProbabilityScale))).ConfigureAwait(false);
        ApplyCommandResultToSummary("Homeostasis", results);
    }

    private bool TryGetTargetBrains(out IReadOnlyList<Guid> targets)
    {
        if (_activeBrains.Count > 0)
        {
            targets = _activeBrains;
            return true;
        }

        if (!TryGetBrainId(out var fallbackBrainId))
        {
            BrainInfoSummary = "No active brains available.";
            targets = Array.Empty<Guid>();
            return false;
        }

        targets = new[] { fallbackBrainId };
        return true;
    }

    private bool TryParsePlasticityRate(out float rate)
    {
        if (!float.TryParse(PlasticityRateText, NumberStyles.Float, CultureInfo.InvariantCulture, out rate))
        {
            return false;
        }

        return float.IsFinite(rate) && rate >= 0f;
    }

    private bool TryParseHomeostasisBaseProbability(out float probability)
    {
        if (!float.TryParse(HomeostasisBaseProbabilityText, NumberStyles.Float, CultureInfo.InvariantCulture, out probability))
        {
            return false;
        }

        return float.IsFinite(probability) && probability >= 0f && probability <= 1f;
    }

    private bool TryParseHomeostasisMinStepCodes(out uint minStepCodes)
    {
        if (!uint.TryParse(HomeostasisMinStepCodesText, NumberStyles.Integer, CultureInfo.InvariantCulture, out minStepCodes))
        {
            return false;
        }

        return minStepCodes > 0;
    }

    private static bool TryParseHomeostasisScale(string text, out float scale)
    {
        if (!float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out scale))
        {
            return false;
        }

        return float.IsFinite(scale) && scale >= 0f && scale <= 4f;
    }

    private void ApplyCommandResultToSummary(string operation, IReadOnlyList<IoCommandResult> results)
    {
        if (results.Count == 0)
        {
            return;
        }

        BrainEnergyState? selectedState = null;
        foreach (var result in results)
        {
            if (!result.Success || result.EnergyState is null)
            {
                continue;
            }

            if (_selectedBrainId.HasValue && result.BrainId == _selectedBrainId.Value)
            {
                selectedState = result.EnergyState;
            }
        }

        var successes = results.Where(result => result.Success).ToList();
        var failures = results.Where(result => !result.Success).ToList();
        string summary;
        if (failures.Count == 0)
        {
            summary = FormattableString.Invariant($"{operation}: applied to {successes.Count} brain(s).");
        }
        else
        {
            var failedIds = string.Join(", ", failures.Select(result => FormattableString.Invariant($"{result.BrainId:D} ({result.Message})")));
            if (successes.Count == 0)
            {
                summary = FormattableString.Invariant($"{operation}: failed for {failures.Count} brain(s): {failedIds}");
            }
            else
            {
                summary = FormattableString.Invariant($"{operation}: {successes.Count}/{results.Count} succeeded. Failed: {failedIds}");
            }
        }

        _dispatcher.Post(() =>
        {
            if (selectedState is not null)
            {
                CostEnabled = selectedState.CostEnabled;
                EnergyEnabled = selectedState.EnergyEnabled;
                PlasticityEnabled = selectedState.PlasticityEnabled;
                HomeostasisEnabled = selectedState.HomeostasisEnabled;
                HomeostasisEnergyCouplingEnabled = selectedState.HomeostasisEnergyCouplingEnabled;
                EnergyRateText = selectedState.EnergyRateUnitsPerSecond.ToString(CultureInfo.InvariantCulture);
                PlasticityRateText = selectedState.PlasticityRate.ToString("0.######", CultureInfo.InvariantCulture);
                HomeostasisBaseProbabilityText = selectedState.HomeostasisBaseProbability.ToString("0.######", CultureInfo.InvariantCulture);
                HomeostasisMinStepCodesText = selectedState.HomeostasisMinStepCodes.ToString(CultureInfo.InvariantCulture);
                HomeostasisEnergyTargetScaleText = selectedState.HomeostasisEnergyTargetScale.ToString("0.######", CultureInfo.InvariantCulture);
                HomeostasisEnergyProbabilityScaleText = selectedState.HomeostasisEnergyProbabilityScale.ToString("0.######", CultureInfo.InvariantCulture);

                var selectedMode = PlasticityModes.FirstOrDefault(mode => mode.Probabilistic == selectedState.PlasticityProbabilisticUpdates);
                if (selectedMode is not null)
                {
                    SelectedPlasticityMode = selectedMode;
                }

                var targetMode = HomeostasisTargetModes.FirstOrDefault(mode => mode.Mode == selectedState.HomeostasisTargetMode);
                if (targetMode is not null)
                {
                    SelectedHomeostasisTargetMode = targetMode;
                }

                var updateMode = HomeostasisUpdateModes.FirstOrDefault(mode => mode.Mode == selectedState.HomeostasisUpdateMode);
                if (updateMode is not null)
                {
                    SelectedHomeostasisUpdateMode = updateMode;
                }
            }

            BrainInfoSummary = summary;
        });
    }

    private bool TryParseSystemPlasticityRateDraft(out float rate, out string normalizedText)
    {
        if (!float.TryParse(SystemPlasticityRateTextDraft, NumberStyles.Float, CultureInfo.InvariantCulture, out rate)
            || !float.IsFinite(rate)
            || rate < 0f)
        {
            normalizedText = string.Empty;
            return false;
        }

        normalizedText = rate.ToString("0.######", CultureInfo.InvariantCulture);
        return true;
    }

    private static string ParseSystemPlasticityRateText(string? value, string fallback)
    {
        if (!float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed < 0f)
        {
            return fallback;
        }

        return parsed.ToString("0.######", CultureInfo.InvariantCulture);
    }

    private static InputCoordinatorMode ParseInputCoordinatorModeSetting(string? value, InputCoordinatorMode fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "0" or "dirty" or "dirty_on_change" => InputCoordinatorMode.DirtyOnChange,
            "1" or "replay" or "replay_latest_vector" => InputCoordinatorMode.ReplayLatestVector,
            _ => fallback
        };
    }

    private static OutputVectorSource ParseOutputVectorSourceSetting(string? value, OutputVectorSource fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "0" or "potential" => OutputVectorSource.Potential,
            "1" or "buffer" => OutputVectorSource.Buffer,
            _ => fallback
        };
    }

    private static bool ParseBooleanSetting(string? value, bool fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => fallback
        };
    }

    private static string FormatInputCoordinatorMode(InputCoordinatorMode mode)
    {
        return mode switch
        {
            InputCoordinatorMode.ReplayLatestVector => "replay_latest_vector",
            _ => "dirty_on_change"
        };
    }

    private static string FormatOutputVectorSource(OutputVectorSource source)
    {
        return source switch
        {
            OutputVectorSource.Buffer => "buffer",
            _ => "potential"
        };
    }
}
