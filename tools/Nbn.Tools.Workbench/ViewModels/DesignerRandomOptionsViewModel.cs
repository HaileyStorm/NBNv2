using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class RandomBrainOptionsViewModel : ViewModelBase
{
    private string _regionCountText = "3";
    private string _neuronCountText = "64";
    private string _axonCountText = "8";
    private string _inputNeuronCountText = "1";
    private string _outputNeuronCountText = "1";
    private string _seedText = string.Empty;
    private RandomOptionChoice<RandomSeedMode> _selectedSeedMode;
    private RandomOptionChoice<RandomRegionSelectionMode> _selectedRegionSelectionMode;
    private string _regionListText = string.Empty;
    private RandomOptionChoice<RandomCountMode> _selectedNeuronCountMode;
    private string _neuronCountMinText = "64";
    private string _neuronCountMaxText = "64";
    private RandomOptionChoice<RandomCountMode> _selectedAxonCountMode;
    private string _axonCountMinText = "8";
    private string _axonCountMaxText = "8";
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
    private string _preActivationMaxText = "0";
    private string _activationThresholdMinText = "0";
    private string _activationThresholdMaxText = "0";
    private RandomOptionChoice<RandomRangeMode> _selectedParamMode;
    private string _paramAMinText = "0";
    private string _paramAMaxText = "0";
    private string _paramBMinText = "0";
    private string _paramBMaxText = "0";

    public RandomBrainOptionsViewModel()
    {
        SeedModes = new ObservableCollection<RandomOptionChoice<RandomSeedMode>>(BuildSeedModes());
        _selectedSeedMode = SeedModes[0];
        RegionSelectionModes = new ObservableCollection<RandomOptionChoice<RandomRegionSelectionMode>>(BuildRegionModes());
        _selectedRegionSelectionMode = RegionSelectionModes[0];
        NeuronCountModes = new ObservableCollection<RandomOptionChoice<RandomCountMode>>(BuildCountModes());
        _selectedNeuronCountMode = NeuronCountModes[0];
        AxonCountModes = new ObservableCollection<RandomOptionChoice<RandomCountMode>>(BuildCountModes());
        _selectedAxonCountMode = AxonCountModes[0];
        TargetBiasModes = new ObservableCollection<RandomOptionChoice<RandomTargetBiasMode>>(BuildTargetBiasModes());
        _selectedTargetBiasMode = TargetBiasModes[0];
        StrengthDistributions = new ObservableCollection<RandomOptionChoice<RandomStrengthDistribution>>(BuildStrengthDistributions());
        _selectedStrengthDistribution = StrengthDistributions[0];
        ActivationModes = new ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>>(BuildFunctionModes());
        _selectedActivationMode = ActivationModes[0];
        ResetModes = new ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>>(BuildFunctionModes());
        _selectedResetMode = ResetModes[0];
        AccumulationModes = new ObservableCollection<RandomOptionChoice<RandomFunctionSelectionMode>>(BuildFunctionModes());
        _selectedAccumulationMode = AccumulationModes[0];
        ThresholdModes = new ObservableCollection<RandomOptionChoice<RandomRangeMode>>(BuildRangeModes());
        _selectedThresholdMode = ThresholdModes[0];
        ParamModes = new ObservableCollection<RandomOptionChoice<RandomRangeMode>>(BuildRangeModes());
        _selectedParamMode = ParamModes[0];
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

    public bool TryBuildBasicOptions(Func<ulong> seedFactory, out RandomBrainBasicOptions options, out string? error)
    {
        options = default;
        if (!int.TryParse(RegionCountText, out var regionCount) || regionCount < 0)
        {
            error = "Random region count must be a non-negative integer.";
            return false;
        }

        if (!int.TryParse(NeuronCountText, out var neuronsPerRegion) || neuronsPerRegion < 1)
        {
            error = "Random neurons per region must be a positive integer.";
            return false;
        }

        if (!int.TryParse(AxonCountText, out var axonsPerNeuron) || axonsPerNeuron < 0)
        {
            error = "Random axons per neuron must be a non-negative integer.";
            return false;
        }

        if (!int.TryParse(InputNeuronCountText, out var inputNeurons) || inputNeurons < 1)
        {
            error = "Input neuron count must be a positive integer.";
            return false;
        }

        if (!int.TryParse(OutputNeuronCountText, out var outputNeurons) || outputNeurons < 1)
        {
            error = "Output neuron count must be a positive integer.";
            return false;
        }

        if (!int.TryParse(StrengthMinText, out var strengthMin) || strengthMin < 0)
        {
            error = "Strength minimum must be a non-negative integer.";
            return false;
        }

        if (!int.TryParse(StrengthMaxText, out var strengthMax) || strengthMax < 0)
        {
            error = "Strength maximum must be a non-negative integer.";
            return false;
        }

        if (strengthMax < strengthMin)
        {
            error = "Strength maximum must be >= minimum.";
            return false;
        }

        if (!TryResolveSeed(seedFactory, out var seed, out error))
        {
            return false;
        }

        options = new RandomBrainBasicOptions(
            regionCount,
            neuronsPerRegion,
            axonsPerNeuron,
            inputNeurons,
            outputNeurons,
            seed,
            AllowSelfLoops,
            AllowIntraRegion,
            AllowInterRegion,
            strengthMin,
            strengthMax);
        return true;
    }

    public IReadOnlyList<string> GetUnsupportedOptionMessages()
    {
        var pending = new List<string>();

        if (SelectedRegionSelectionMode.Value != RandomRegionSelectionMode.Random)
        {
            pending.Add("region selection");
        }

        if (SelectedNeuronCountMode.Value != RandomCountMode.Fixed)
        {
            pending.Add("neuron count range");
        }

        if (SelectedAxonCountMode.Value != RandomCountMode.Fixed)
        {
            pending.Add("axon count range");
        }

        if (SelectedTargetBiasMode.Value != RandomTargetBiasMode.Uniform)
        {
            pending.Add("target bias");
        }

        if (SelectedStrengthDistribution.Value != RandomStrengthDistribution.Uniform)
        {
            pending.Add("strength distribution");
        }

        if (SelectedActivationMode.Value != RandomFunctionSelectionMode.Fixed)
        {
            pending.Add("activation mix");
        }

        if (SelectedResetMode.Value != RandomFunctionSelectionMode.Fixed)
        {
            pending.Add("reset mix");
        }

        if (SelectedAccumulationMode.Value != RandomFunctionSelectionMode.Fixed)
        {
            pending.Add("accumulation mix");
        }

        if (SelectedThresholdMode.Value != RandomRangeMode.Fixed)
        {
            pending.Add("threshold ranges");
        }

        if (SelectedParamMode.Value != RandomRangeMode.Fixed)
        {
            pending.Add("parameter ranges");
        }

        return pending;
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

public readonly record struct RandomBrainBasicOptions(
    int RegionCount,
    int NeuronsPerRegion,
    int AxonsPerNeuron,
    int InputNeurons,
    int OutputNeurons,
    ulong Seed,
    bool AllowSelfLoops,
    bool AllowIntraRegion,
    bool AllowInterRegion,
    int StrengthMinCode,
    int StrengthMaxCode);

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
