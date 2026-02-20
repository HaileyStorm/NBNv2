using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Nbn.Shared;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class RandomBrainOptionsViewModel : ViewModelBase
{
    private string _regionCountText = "3";
    private string _neuronCountText = "16";
    private string _axonCountText = "8";
    private string _inputNeuronCountText = "1-10";
    private string _outputNeuronCountText = "1-10";
    private string _seedText = string.Empty;
    private RandomOptionChoice<RandomSeedMode> _selectedSeedMode;
    private RandomOptionChoice<RandomRegionSelectionMode> _selectedRegionSelectionMode;
    private string _regionListText = string.Empty;
    private RandomOptionChoice<RandomCountMode> _selectedNeuronCountMode;
    private string _neuronCountMinText = "2";
    private string _neuronCountMaxText = "50";
    private RandomOptionChoice<RandomCountMode> _selectedAxonCountMode;
    private string _axonCountMinText = "1";
    private string _axonCountMaxText = "25";
    private bool _allowSelfLoops = true;
    private bool _allowIntraRegion = true;
    private bool _allowInterRegion = true;
    private RandomOptionChoice<RandomTargetBiasMode> _selectedTargetBiasMode;
    private string _strengthMinText = "0";
    private string _strengthMaxText = "31";
    private RandomOptionChoice<RandomStrengthDistribution> _selectedStrengthDistribution;
    private RandomOptionChoice<RandomFunctionSelectionMode> _selectedActivationMode;
    private string _activationFixedIdText = "11";
    private RandomOptionChoice<RandomFunctionSelectionMode> _selectedResetMode;
    private string _resetFixedIdText = "0";
    private RandomOptionChoice<RandomFunctionSelectionMode> _selectedAccumulationMode;
    private string _accumulationFixedIdText = "0";
    private RandomOptionChoice<RandomRangeMode> _selectedThresholdMode;
    private string _preActivationMinText = "0";
    private string _preActivationMaxText = "63";
    private string _activationThresholdMinText = "0";
    private string _activationThresholdMaxText = "63";
    private RandomOptionChoice<RandomRangeMode> _selectedParamMode;
    private string _paramAMinText = "0";
    private string _paramAMaxText = "63";
    private string _paramBMinText = "0";
    private string _paramBMaxText = "63";

    public RandomBrainOptionsViewModel()
    {
        SeedModes = new ObservableCollection<RandomOptionChoice<RandomSeedMode>>(BuildSeedModes());
        _selectedSeedMode = SeedModes[0];
        RegionSelectionModes = new ObservableCollection<RandomOptionChoice<RandomRegionSelectionMode>>(BuildRegionModes());
        _selectedRegionSelectionMode = RegionSelectionModes[0];
        NeuronCountModes = new ObservableCollection<RandomOptionChoice<RandomCountMode>>(BuildCountModes());
        _selectedNeuronCountMode = NeuronCountModes[1];
        AxonCountModes = new ObservableCollection<RandomOptionChoice<RandomCountMode>>(BuildCountModes());
        _selectedAxonCountMode = AxonCountModes[1];
        TargetBiasModes = new ObservableCollection<RandomOptionChoice<RandomTargetBiasMode>>(BuildTargetBiasModes());
        _selectedTargetBiasMode = TargetBiasModes[1];
        StrengthDistributions = new ObservableCollection<RandomOptionChoice<RandomStrengthDistribution>>(BuildStrengthDistributions());
        _selectedStrengthDistribution = StrengthDistributions[0];
        ActivationModes = new ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>>(BuildFunctionModes());
        _selectedActivationMode = ActivationModes[2];
        ResetModes = new ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>>(BuildFunctionModes());
        _selectedResetMode = ResetModes[2];
        AccumulationModes = new ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>>(BuildFunctionModes());
        _selectedAccumulationMode = AccumulationModes[2];
        ThresholdModes = new ObservableCollection<RandomOptionChoice<RandomRangeMode>>(BuildRangeModes());
        _selectedThresholdMode = ThresholdModes[1];
        ParamModes = new ObservableCollection<RandomOptionChoice<RandomRangeMode>>(BuildRangeModes());
        _selectedParamMode = ParamModes[1];
    }

    public ObservableCollection<RandomOptionChoice<RandomSeedMode>> SeedModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomRegionSelectionMode>> RegionSelectionModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomCountMode>> NeuronCountModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomCountMode>> AxonCountModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomTargetBiasMode>> TargetBiasModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomStrengthDistribution>> StrengthDistributions { get; }
    public ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>> ActivationModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>> ResetModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>> AccumulationModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomRangeMode>> ThresholdModes { get; }
    public ObservableCollection<RandomOptionChoice<RandomRangeMode>> ParamModes { get; }

    public string RegionCountText
    {
        get => _regionCountText;
        set => SetProperty(ref _regionCountText, value);
    }

    public string NeuronCountText
    {
        get => _neuronCountText;
        set => SetProperty(ref _neuronCountText, value);
    }

    public string AxonCountText
    {
        get => _axonCountText;
        set => SetProperty(ref _axonCountText, value);
    }

    public string InputNeuronCountText
    {
        get => _inputNeuronCountText;
        set => SetProperty(ref _inputNeuronCountText, value);
    }

    public string OutputNeuronCountText
    {
        get => _outputNeuronCountText;
        set => SetProperty(ref _outputNeuronCountText, value);
    }

    public RandomOptionChoice<RandomSeedMode> SelectedSeedMode
    {
        get => _selectedSeedMode;
        set
        {
            if (SetProperty(ref _selectedSeedMode, value))
            {
                OnPropertyChanged(nameof(IsSeedTextEnabled));
            }
        }
    }

    public string SeedText
    {
        get => _seedText;
        set => SetProperty(ref _seedText, value);
    }

    public RandomOptionChoice<RandomRegionSelectionMode> SelectedRegionSelectionMode
    {
        get => _selectedRegionSelectionMode;
        set
        {
            if (SetProperty(ref _selectedRegionSelectionMode, value))
            {
                OnPropertyChanged(nameof(IsRegionListEnabled));
            }
        }
    }

    public string RegionListText
    {
        get => _regionListText;
        set => SetProperty(ref _regionListText, value);
    }

    public RandomOptionChoice<RandomCountMode> SelectedNeuronCountMode
    {
        get => _selectedNeuronCountMode;
        set
        {
            if (SetProperty(ref _selectedNeuronCountMode, value))
            {
                OnPropertyChanged(nameof(IsNeuronRangeEnabled));
            }
        }
    }

    public string NeuronCountMinText
    {
        get => _neuronCountMinText;
        set => SetProperty(ref _neuronCountMinText, value);
    }

    public string NeuronCountMaxText
    {
        get => _neuronCountMaxText;
        set => SetProperty(ref _neuronCountMaxText, value);
    }

    public RandomOptionChoice<RandomCountMode> SelectedAxonCountMode
    {
        get => _selectedAxonCountMode;
        set
        {
            if (SetProperty(ref _selectedAxonCountMode, value))
            {
                OnPropertyChanged(nameof(IsAxonRangeEnabled));
            }
        }
    }

    public string AxonCountMinText
    {
        get => _axonCountMinText;
        set => SetProperty(ref _axonCountMinText, value);
    }

    public string AxonCountMaxText
    {
        get => _axonCountMaxText;
        set => SetProperty(ref _axonCountMaxText, value);
    }

    public bool AllowSelfLoops
    {
        get => _allowSelfLoops;
        set => SetProperty(ref _allowSelfLoops, value);
    }

    public bool AllowIntraRegion
    {
        get => _allowIntraRegion;
        set => SetProperty(ref _allowIntraRegion, value);
    }

    public bool AllowInterRegion
    {
        get => _allowInterRegion;
        set => SetProperty(ref _allowInterRegion, value);
    }

    public RandomOptionChoice<RandomTargetBiasMode> SelectedTargetBiasMode
    {
        get => _selectedTargetBiasMode;
        set => SetProperty(ref _selectedTargetBiasMode, value);
    }

    public string StrengthMinText
    {
        get => _strengthMinText;
        set => SetProperty(ref _strengthMinText, value);
    }

    public string StrengthMaxText
    {
        get => _strengthMaxText;
        set => SetProperty(ref _strengthMaxText, value);
    }

    public RandomOptionChoice<RandomStrengthDistribution> SelectedStrengthDistribution
    {
        get => _selectedStrengthDistribution;
        set => SetProperty(ref _selectedStrengthDistribution, value);
    }

    public RandomOptionChoice<RandomFunctionSelectionMode> SelectedActivationMode
    {
        get => _selectedActivationMode;
        set
        {
            if (SetProperty(ref _selectedActivationMode, value))
            {
                OnPropertyChanged(nameof(IsActivationFixedEnabled));
            }
        }
    }

    public string ActivationFixedIdText
    {
        get => _activationFixedIdText;
        set => SetProperty(ref _activationFixedIdText, value);
    }

    public RandomOptionChoice<RandomFunctionSelectionMode> SelectedResetMode
    {
        get => _selectedResetMode;
        set
        {
            if (SetProperty(ref _selectedResetMode, value))
            {
                OnPropertyChanged(nameof(IsResetFixedEnabled));
            }
        }
    }

    public string ResetFixedIdText
    {
        get => _resetFixedIdText;
        set => SetProperty(ref _resetFixedIdText, value);
    }

    public RandomOptionChoice<RandomFunctionSelectionMode> SelectedAccumulationMode
    {
        get => _selectedAccumulationMode;
        set
        {
            if (SetProperty(ref _selectedAccumulationMode, value))
            {
                OnPropertyChanged(nameof(IsAccumulationFixedEnabled));
            }
        }
    }

    public string AccumulationFixedIdText
    {
        get => _accumulationFixedIdText;
        set => SetProperty(ref _accumulationFixedIdText, value);
    }

    public RandomOptionChoice<RandomRangeMode> SelectedThresholdMode
    {
        get => _selectedThresholdMode;
        set
        {
            if (SetProperty(ref _selectedThresholdMode, value))
            {
                OnPropertyChanged(nameof(IsThresholdRangeEnabled));
            }
        }
    }

    public string PreActivationMinText
    {
        get => _preActivationMinText;
        set => SetProperty(ref _preActivationMinText, value);
    }

    public string PreActivationMaxText
    {
        get => _preActivationMaxText;
        set => SetProperty(ref _preActivationMaxText, value);
    }

    public string ActivationThresholdMinText
    {
        get => _activationThresholdMinText;
        set => SetProperty(ref _activationThresholdMinText, value);
    }

    public string ActivationThresholdMaxText
    {
        get => _activationThresholdMaxText;
        set => SetProperty(ref _activationThresholdMaxText, value);
    }

    public RandomOptionChoice<RandomRangeMode> SelectedParamMode
    {
        get => _selectedParamMode;
        set
        {
            if (SetProperty(ref _selectedParamMode, value))
            {
                OnPropertyChanged(nameof(IsParamRangeEnabled));
            }
        }
    }

    public string ParamAMinText
    {
        get => _paramAMinText;
        set => SetProperty(ref _paramAMinText, value);
    }

    public string ParamAMaxText
    {
        get => _paramAMaxText;
        set => SetProperty(ref _paramAMaxText, value);
    }

    public string ParamBMinText
    {
        get => _paramBMinText;
        set => SetProperty(ref _paramBMinText, value);
    }

    public string ParamBMaxText
    {
        get => _paramBMaxText;
        set => SetProperty(ref _paramBMaxText, value);
    }

    public bool IsSeedTextEnabled => SelectedSeedMode.Value == RandomSeedMode.Fixed;
    public bool IsRegionListEnabled => SelectedRegionSelectionMode.Value == RandomRegionSelectionMode.ExplicitList;
    public bool IsNeuronRangeEnabled => SelectedNeuronCountMode.Value == RandomCountMode.Range;
    public bool IsAxonRangeEnabled => SelectedAxonCountMode.Value == RandomCountMode.Range;
    public bool IsActivationFixedEnabled => SelectedActivationMode.Value == RandomFunctionSelectionMode.Fixed;
    public bool IsResetFixedEnabled => SelectedResetMode.Value == RandomFunctionSelectionMode.Fixed;
    public bool IsAccumulationFixedEnabled => SelectedAccumulationMode.Value == RandomFunctionSelectionMode.Fixed;
    public bool IsThresholdRangeEnabled => SelectedThresholdMode.Value == RandomRangeMode.Range;
    public bool IsParamRangeEnabled => SelectedParamMode.Value == RandomRangeMode.Range;

    public bool TryBuildOptions(Func<ulong> seedFactory, out RandomBrainGenerationOptions options, out string? error)
    {
        options = default;
        error = null;

        var nonIoMax = NbnConstants.RegionCount - 2;
        var maxNeuronId = NbnConstants.MaxAxonTargetNeuronId;

        var regionSelectionMode = SelectedRegionSelectionMode.Value;
        var regionCount = 0;
        var explicitRegions = Array.Empty<int>();
        if (regionSelectionMode == RandomRegionSelectionMode.ExplicitList)
        {
            if (!TryParseRegionList(RegionListText, out explicitRegions, out error))
            {
                return false;
            }

            if (explicitRegions.Length > nonIoMax)
            {
                error = $"Region list must include at most {nonIoMax} non-IO regions.";
                return false;
            }

            regionCount = explicitRegions.Length;
        }
        else
        {
            if (!TryParseInt(RegionCountText, out regionCount) || regionCount < 0)
            {
                error = "Random region count must be a non-negative integer.";
                return false;
            }

            if (regionCount > nonIoMax)
            {
                error = $"Random region count must be between 0 and {nonIoMax}.";
                return false;
            }
        }

        var neuronMode = SelectedNeuronCountMode.Value;
        var neuronFixed = 0;
        var neuronMin = 0;
        var neuronMax = 0;
        if (neuronMode == RandomCountMode.Fixed)
        {
            if (!TryParseInt(NeuronCountText, out neuronFixed) || neuronFixed < 1)
            {
                error = "Random neurons per region must be a positive integer.";
                return false;
            }

            if (neuronFixed > maxNeuronId)
            {
                error = $"Random neurons per region must be <= {maxNeuronId}.";
                return false;
            }

            neuronMin = neuronFixed;
            neuronMax = neuronFixed;
        }
        else
        {
            if (!TryParseInt(NeuronCountMinText, out neuronMin) || neuronMin < 1)
            {
                error = "Neuron range minimum must be a positive integer.";
                return false;
            }

            if (!TryParseInt(NeuronCountMaxText, out neuronMax) || neuronMax < 1)
            {
                error = "Neuron range maximum must be a positive integer.";
                return false;
            }

            if (neuronMin > neuronMax)
            {
                error = "Neuron range maximum must be >= minimum.";
                return false;
            }

            if (neuronMax > maxNeuronId)
            {
                error = $"Neuron range maximum must be <= {maxNeuronId}.";
                return false;
            }
        }

        var axonMode = SelectedAxonCountMode.Value;
        var axonFixed = 0;
        var axonMin = 0;
        var axonMax = 0;
        if (axonMode == RandomCountMode.Fixed)
        {
            if (!TryParseInt(AxonCountText, out axonFixed) || axonFixed < 0)
            {
                error = "Random axons per neuron must be a non-negative integer.";
                return false;
            }

            if (axonFixed > NbnConstants.MaxAxonsPerNeuron)
            {
                error = $"Random axons per neuron must be <= {NbnConstants.MaxAxonsPerNeuron}.";
                return false;
            }

            axonMin = axonFixed;
            axonMax = axonFixed;
        }
        else
        {
            if (!TryParseInt(AxonCountMinText, out axonMin) || axonMin < 0)
            {
                error = "Axon range minimum must be a non-negative integer.";
                return false;
            }

            if (!TryParseInt(AxonCountMaxText, out axonMax) || axonMax < 0)
            {
                error = "Axon range maximum must be a non-negative integer.";
                return false;
            }

            if (axonMin > axonMax)
            {
                error = "Axon range maximum must be >= minimum.";
                return false;
            }

            if (axonMax > NbnConstants.MaxAxonsPerNeuron)
            {
                error = $"Axon range maximum must be <= {NbnConstants.MaxAxonsPerNeuron}.";
                return false;
            }
        }

        if (!TryParseCountRange(InputNeuronCountText, 1, maxNeuronId, "Input neuron count", out var inputNeuronMin, out var inputNeuronMax, out error))
        {
            return false;
        }

        if (!TryParseCountRange(OutputNeuronCountText, 1, maxNeuronId, "Output neuron count", out var outputNeuronMin, out var outputNeuronMax, out error))
        {
            return false;
        }

        if (!TryParseInt(StrengthMinText, out var strengthMin) || strengthMin < 0)
        {
            error = "Strength minimum must be a non-negative integer.";
            return false;
        }

        if (!TryParseInt(StrengthMaxText, out var strengthMax) || strengthMax < 0)
        {
            error = "Strength maximum must be a non-negative integer.";
            return false;
        }

        if (strengthMin > 31 || strengthMax > 31)
        {
            error = "Strength codes must be between 0 and 31.";
            return false;
        }

        if (strengthMax < strengthMin)
        {
            error = "Strength maximum must be >= minimum.";
            return false;
        }

        var activationFixedId = 0;
        if (SelectedActivationMode.Value == RandomFunctionSelectionMode.Fixed)
        {
            if (!TryParseInt(ActivationFixedIdText, out activationFixedId) || activationFixedId < 0 || activationFixedId > 29)
            {
                error = "Activation function id must be between 0 and 29.";
                return false;
            }
        }
        else
        {
            _ = TryParseInt(ActivationFixedIdText, out activationFixedId);
        }

        var resetFixedId = 0;
        if (SelectedResetMode.Value == RandomFunctionSelectionMode.Fixed)
        {
            if (!TryParseInt(ResetFixedIdText, out resetFixedId) || resetFixedId < 0 || resetFixedId > 60)
            {
                error = "Reset function id must be between 0 and 60.";
                return false;
            }
        }
        else
        {
            _ = TryParseInt(ResetFixedIdText, out resetFixedId);
        }

        var accumulationFixedId = 0;
        if (SelectedAccumulationMode.Value == RandomFunctionSelectionMode.Fixed)
        {
            if (!TryParseInt(AccumulationFixedIdText, out accumulationFixedId) || accumulationFixedId < 0 || accumulationFixedId > 3)
            {
                error = "Accumulation function id must be between 0 and 3.";
                return false;
            }
        }
        else
        {
            _ = TryParseInt(AccumulationFixedIdText, out accumulationFixedId);
        }

        var thresholdMode = SelectedThresholdMode.Value;
        var preActMin = 0;
        var preActMax = 0;
        var actThreshMin = 0;
        var actThreshMax = 0;
        if (thresholdMode == RandomRangeMode.Fixed)
        {
            if (!TryParseCode(PreActivationMinText, "Pre-activation threshold", out preActMin, out error)
                || !TryParseCode(ActivationThresholdMinText, "Activation threshold", out actThreshMin, out error))
            {
                return false;
            }

            preActMax = preActMin;
            actThreshMax = actThreshMin;
        }
        else
        {
            if (!TryParseCodeRange(PreActivationMinText, PreActivationMaxText, "Pre-activation threshold", out preActMin, out preActMax, out error))
            {
                return false;
            }

            if (!TryParseCodeRange(ActivationThresholdMinText, ActivationThresholdMaxText, "Activation threshold", out actThreshMin, out actThreshMax, out error))
            {
                return false;
            }
        }

        var paramMode = SelectedParamMode.Value;
        var paramAMin = 0;
        var paramAMax = 0;
        var paramBMin = 0;
        var paramBMax = 0;
        if (paramMode == RandomRangeMode.Fixed)
        {
            if (!TryParseCode(ParamAMinText, "Param A", out paramAMin, out error)
                || !TryParseCode(ParamBMinText, "Param B", out paramBMin, out error))
            {
                return false;
            }

            paramAMax = paramAMin;
            paramBMax = paramBMin;
        }
        else
        {
            if (!TryParseCodeRange(ParamAMinText, ParamAMaxText, "Param A", out paramAMin, out paramAMax, out error))
            {
                return false;
            }

            if (!TryParseCodeRange(ParamBMinText, ParamBMaxText, "Param B", out paramBMin, out paramBMax, out error))
            {
                return false;
            }
        }

        if (!AllowIntraRegion && !AllowInterRegion && axonMax > 0)
        {
            error = "Enable intra-region and/or inter-region targets when axons per neuron is above zero.";
            return false;
        }

        if (!TryResolveSeed(seedFactory, out var seed, out error))
        {
            return false;
        }

        options = new RandomBrainGenerationOptions(
            regionCount,
            explicitRegions,
            regionSelectionMode,
            neuronMode,
            neuronFixed,
            neuronMin,
            neuronMax,
            axonMode,
            axonFixed,
            axonMin,
            axonMax,
            inputNeuronMin,
            inputNeuronMax,
            outputNeuronMin,
            outputNeuronMax,
            seed,
            AllowSelfLoops,
            AllowIntraRegion,
            AllowInterRegion,
            SelectedTargetBiasMode.Value,
            strengthMin,
            strengthMax,
            SelectedStrengthDistribution.Value,
            SelectedActivationMode.Value,
            activationFixedId,
            SelectedResetMode.Value,
            resetFixedId,
            SelectedAccumulationMode.Value,
            accumulationFixedId,
            thresholdMode,
            preActMin,
            preActMax,
            actThreshMin,
            actThreshMax,
            paramMode,
            paramAMin,
            paramAMax,
            paramBMin,
            paramBMax);
        return true;
    }

    private bool TryResolveSeed(Func<ulong> seedFactory, out ulong seed, out string? error)
    {
        seed = 0;
        error = null;

        if (SelectedSeedMode.Value == RandomSeedMode.Random)
        {
            seed = seedFactory();
            return true;
        }

        if (string.IsNullOrWhiteSpace(SeedText))
        {
            error = "Seed must be provided when mode is Fixed.";
            return false;
        }

        if (!TryParseSeed(SeedText.Trim(), out seed))
        {
            error = "Seed must be a valid unsigned 64-bit integer (decimal or 0x... hex).";
            return false;
        }

        return true;
    }

    private static bool TryParseSeed(string value, out ulong seed)
    {
        if (value.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            return ulong.TryParse(value.AsSpan(2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out seed);
        }

        return ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out seed);
    }

    private static bool TryParseInt(string value, out int parsed)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed);

    private static bool TryParseCountRange(string value, int minAllowed, int maxAllowed, string label, out int min, out int max, out string? error)
    {
        error = null;
        min = 0;
        max = 0;

        if (string.IsNullOrWhiteSpace(value))
        {
            error = $"{label} must be provided.";
            return false;
        }

        var trimmed = value.Trim();
        string[] parts;
        if (trimmed.Contains("..", StringComparison.Ordinal))
        {
            parts = trimmed.Split(new[] { ".." }, 2, StringSplitOptions.RemoveEmptyEntries);
        }
        else if (trimmed.Contains('-', StringComparison.Ordinal))
        {
            parts = trimmed.Split('-', 2, StringSplitOptions.RemoveEmptyEntries);
        }
        else
        {
            parts = new[] { trimmed };
        }

        if (parts.Length == 1)
        {
            if (!TryParseInt(parts[0], out min))
            {
                error = $"{label} must be an integer or range (e.g. 1-10).";
                return false;
            }

            max = min;
        }
        else if (parts.Length == 2)
        {
            if (!TryParseInt(parts[0], out min) || !TryParseInt(parts[1], out max))
            {
                error = $"{label} must be an integer or range (e.g. 1-10).";
                return false;
            }
        }
        else
        {
            error = $"{label} must be an integer or range (e.g. 1-10).";
            return false;
        }

        if (max < min)
        {
            (min, max) = (max, min);
        }

        if (min < minAllowed || max < minAllowed)
        {
            error = $"{label} must be at least {minAllowed}.";
            return false;
        }

        if (min > maxAllowed || max > maxAllowed)
        {
            error = $"{label} must be <= {maxAllowed}.";
            return false;
        }

        return true;
    }

    private static bool TryParseRegionList(string value, out int[] regionIds, out string? error)
    {
        error = null;
        regionIds = Array.Empty<int>();
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        var tokens = value
            .Split(new[] { ',', ';', ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

        var results = new HashSet<int>();
        foreach (var token in tokens)
        {
            var trimmed = token.Trim();
            if (string.IsNullOrWhiteSpace(trimmed))
            {
                continue;
            }

            if (trimmed.Contains('-', StringComparison.Ordinal))
            {
                var parts = trimmed.Split('-', 2);
                if (parts.Length != 2 || !TryParseInt(parts[0], out var start) || !TryParseInt(parts[1], out var end))
                {
                    error = "Region list entries must be ids or ranges like 4-7.";
                    return false;
                }

                if (end < start)
                {
                    (start, end) = (end, start);
                }

                for (var id = start; id <= end; id++)
                {
                    if (!IsValidExplicitRegionId(id, out error))
                    {
                        return false;
                    }

                    results.Add(id);
                }
            }
            else
            {
                if (!TryParseInt(trimmed, out var id))
                {
                    error = "Region list entries must be ids or ranges like 4-7.";
                    return false;
                }

                if (!IsValidExplicitRegionId(id, out error))
                {
                    return false;
                }

                results.Add(id);
            }
        }

        regionIds = results.OrderBy(id => id).ToArray();
        return true;
    }

    private static bool IsValidExplicitRegionId(int id, out string? error)
    {
        error = null;
        if (id == NbnConstants.InputRegionId || id == NbnConstants.OutputRegionId)
        {
            error = "Region list must include only non-IO region ids (1-30).";
            return false;
        }

        if (id < NbnConstants.RegionMinId || id > NbnConstants.RegionMaxId)
        {
            error = $"Region list entries must be between {NbnConstants.RegionMinId} and {NbnConstants.RegionMaxId}.";
            return false;
        }

        return true;
    }

    private static bool TryParseCode(string value, string label, out int parsed, out string? error)
    {
        error = null;
        parsed = 0;
        if (!TryParseInt(value, out parsed) || parsed < 0 || parsed > 63)
        {
            error = $"{label} code must be between 0 and 63.";
            return false;
        }

        return true;
    }

    private static bool TryParseCodeRange(string minText, string maxText, string label, out int min, out int max, out string? error)
    {
        error = null;
        min = 0;
        max = 0;

        if (!TryParseCode(minText, label, out min, out error))
        {
            return false;
        }

        if (!TryParseCode(maxText, label, out max, out error))
        {
            return false;
        }

        if (max < min)
        {
            error = $"{label} maximum must be >= minimum.";
            return false;
        }

        return true;
    }

    private static IReadOnlyList<RandomOptionChoice<RandomSeedMode>> BuildSeedModes()
        => new List<RandomOptionChoice<RandomSeedMode>>
        {
            new("Random", RandomSeedMode.Random),
            new("Fixed", RandomSeedMode.Fixed)
        };

    private static IReadOnlyList<RandomOptionChoice<RandomRegionSelectionMode>> BuildRegionModes()
        => new List<RandomOptionChoice<RandomRegionSelectionMode>>
        {
            new("Random pick", RandomRegionSelectionMode.Random),
            new("Contiguous block", RandomRegionSelectionMode.Contiguous),
            new("Explicit list", RandomRegionSelectionMode.ExplicitList)
        };

    private static IReadOnlyList<RandomOptionChoice<RandomCountMode>> BuildCountModes()
        => new List<RandomOptionChoice<RandomCountMode>>
        {
            new("Fixed", RandomCountMode.Fixed),
            new("Range", RandomCountMode.Range)
        };

    private static IReadOnlyList<RandomOptionChoice<RandomTargetBiasMode>> BuildTargetBiasModes()
        => new List<RandomOptionChoice<RandomTargetBiasMode>>
        {
            new("Uniform", RandomTargetBiasMode.Uniform),
            new("Distance weighted", RandomTargetBiasMode.DistanceWeighted),
            new("Region weighted", RandomTargetBiasMode.RegionWeighted)
        };

    private static IReadOnlyList<RandomOptionChoice<RandomStrengthDistribution>> BuildStrengthDistributions()
        => new List<RandomOptionChoice<RandomStrengthDistribution>>
        {
            new("Uniform", RandomStrengthDistribution.Uniform),
            new("Centered", RandomStrengthDistribution.Centered),
            new("Normal", RandomStrengthDistribution.Normal)
        };

    private static IReadOnlyList<RandomOptionChoice<RandomFunctionSelectionMode>> BuildFunctionModes()
        => new List<RandomOptionChoice<RandomFunctionSelectionMode>>
        {
            new("Fixed", RandomFunctionSelectionMode.Fixed),
            new("Random", RandomFunctionSelectionMode.Random),
            new("Weighted", RandomFunctionSelectionMode.Weighted)
        };

    private static IReadOnlyList<RandomOptionChoice<RandomRangeMode>> BuildRangeModes()
        => new List<RandomOptionChoice<RandomRangeMode>>
        {
            new("Fixed", RandomRangeMode.Fixed),
            new("Range", RandomRangeMode.Range)
        };
}

public readonly record struct RandomBrainGenerationOptions(
    int RegionCount,
    IReadOnlyList<int> ExplicitRegions,
    RandomRegionSelectionMode RegionSelectionMode,
    RandomCountMode NeuronCountMode,
    int NeuronsPerRegion,
    int NeuronCountMin,
    int NeuronCountMax,
    RandomCountMode AxonCountMode,
    int AxonsPerNeuron,
    int AxonCountMin,
    int AxonCountMax,
    int InputNeuronMin,
    int InputNeuronMax,
    int OutputNeuronMin,
    int OutputNeuronMax,
    ulong Seed,
    bool AllowSelfLoops,
    bool AllowIntraRegion,
    bool AllowInterRegion,
    RandomTargetBiasMode TargetBiasMode,
    int StrengthMinCode,
    int StrengthMaxCode,
    RandomStrengthDistribution StrengthDistribution,
    RandomFunctionSelectionMode ActivationMode,
    int ActivationFixedId,
    RandomFunctionSelectionMode ResetMode,
    int ResetFixedId,
    RandomFunctionSelectionMode AccumulationMode,
    int AccumulationFixedId,
    RandomRangeMode ThresholdMode,
    int PreActivationMin,
    int PreActivationMax,
    int ActivationThresholdMin,
    int ActivationThresholdMax,
    RandomRangeMode ParamMode,
    int ParamAMin,
    int ParamAMax,
    int ParamBMin,
    int ParamBMax);

public sealed record RandomOptionChoice<T>(string Label, T Value);

public enum RandomSeedMode
{
    Random,
    Fixed
}

public enum RandomRegionSelectionMode
{
    Random,
    Contiguous,
    ExplicitList
}

public enum RandomCountMode
{
    Fixed,
    Range
}

public enum RandomTargetBiasMode
{
    Uniform,
    DistanceWeighted,
    RegionWeighted
}

public enum RandomStrengthDistribution
{
    Uniform,
    Centered,
    Normal
}

public enum RandomFunctionSelectionMode
{
    Fixed,
    Random,
    Weighted
}

public enum RandomRangeMode
{
    Fixed,
    Range
}
