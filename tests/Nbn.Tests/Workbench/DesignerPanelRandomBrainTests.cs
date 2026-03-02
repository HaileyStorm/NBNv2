using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Proto.Signal;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Nbn.Shared.Quantization;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;
using Xunit;

namespace Nbn.Tests.Workbench;

public class DesignerPanelRandomBrainTests
{
    [Fact]
    public void NewRandomBrain_DefaultModes_SeedSignalResponsiveInputNeurons()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "424242";
            vm.RandomOptions.InputNeuronCountText = "7";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var inputRegion = brain.Regions[NbnConstants.InputRegionId];
            Assert.Equal(7, inputRegion.NeuronCount);
            Assert.All(inputRegion.Neurons.Where(neuron => neuron.Exists), neuron =>
            {
                Assert.Equal(1, neuron.ActivationFunctionId);
                Assert.Equal(0, neuron.ResetFunctionId);
                Assert.Equal(0, neuron.AccumulationFunctionId);
                Assert.Equal(0, neuron.PreActivationThresholdCode);
                Assert.Equal(0, neuron.ActivationThresholdCode);
            });
        });
    }

    [Fact]
    public void NewRandomBrain_WeightedAccumulation_DoesNotAssignAccumNone()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "123456";
            vm.RandomOptions.SelectedAccumulationMode = vm.RandomOptions.AccumulationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var existingNeurons = brain.Regions.SelectMany(region => region.Neurons).Where(neuron => neuron.Exists).ToList();
            Assert.NotEmpty(existingNeurons);
            Assert.All(existingNeurons, neuron => Assert.NotEqual(3, neuron.AccumulationFunctionId));
        });
    }

    [Fact]
    public void NewRandomBrain_WeightedResetMode_OutputNeuronsDefaultToResetZero()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateDesignerViewModel();

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.OutputNeuronCountText = "10";
            vm.RandomOptions.SelectedResetMode = vm.RandomOptions.ResetModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);
            vm.RandomOptions.SeedBaselineActivityPath = false;

            for (var seed = 1; seed <= 32; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);

                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
                var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
                Assert.Equal(10, outputRegion.NeuronCount);
                Assert.All(outputRegion.Neurons.Where(neuron => neuron.Exists), neuron => Assert.Equal(0, neuron.ResetFunctionId));
            }
        });
    }

    [Fact]
    public void NewRandomBrain_FixedResetMode_OutputNeuronsRespectFixedResetId()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "982451653";
            vm.RandomOptions.OutputNeuronCountText = "5";
            vm.RandomOptions.SelectedResetMode = vm.RandomOptions.ResetModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Fixed);
            vm.RandomOptions.ResetFixedIdText = "55";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
            Assert.Equal(5, outputRegion.NeuronCount);
            Assert.All(outputRegion.Neurons.Where(neuron => neuron.Exists), neuron => Assert.Equal(55, neuron.ResetFunctionId));
        });
    }

    [Fact]
    public void NewRandomBrain_DefaultModes_OutputNeuronsUseBoundedActivationAndThresholds()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);
            var allowedActivationIds = new HashSet<int> { 6, 11, 18 };

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "98765";
            vm.RandomOptions.OutputNeuronCountText = "7";
            vm.RandomOptions.SelectedActivationMode = vm.RandomOptions.ActivationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);
            vm.RandomOptions.SelectedThresholdMode = vm.RandomOptions.ThresholdModes.Single(mode => mode.Value == RandomRangeMode.Range);
            vm.RandomOptions.PreActivationMinText = "0";
            vm.RandomOptions.PreActivationMaxText = "63";
            vm.RandomOptions.ActivationThresholdMinText = "0";
            vm.RandomOptions.ActivationThresholdMaxText = "63";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
            Assert.Equal(7, outputRegion.NeuronCount);
            Assert.All(outputRegion.Neurons.Where(neuron => neuron.Exists), neuron =>
            {
                Assert.Contains(neuron.ActivationFunctionId, allowedActivationIds);
                Assert.InRange(neuron.PreActivationThresholdCode, 0, 16);
                Assert.InRange(neuron.ActivationThresholdCode, 0, 16);
                Assert.Equal(0, neuron.AccumulationFunctionId);
            });
        });
    }

    [Fact]
    public void NewRandomBrain_WeightedActivation_InternalNeuronsUseStableFunctionsAndTightThresholdCaps()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);
            var allowedInternalActivationIds = new HashSet<int> { 1, 2, 3, 4, 6, 9, 11, 18, 20 };

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "246813579";
            vm.RandomOptions.SelectedActivationMode = vm.RandomOptions.ActivationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);
            vm.RandomOptions.SelectedThresholdMode = vm.RandomOptions.ThresholdModes.Single(mode => mode.Value == RandomRangeMode.Range);
            vm.RandomOptions.PreActivationMinText = "0";
            vm.RandomOptions.PreActivationMaxText = "63";
            vm.RandomOptions.ActivationThresholdMinText = "0";
            vm.RandomOptions.ActivationThresholdMaxText = "63";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var internalNeurons = brain.Regions
                .Where(region => region.RegionId != NbnConstants.InputRegionId && region.RegionId != NbnConstants.OutputRegionId)
                .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
                .ToList();

            Assert.NotEmpty(internalNeurons);
            Assert.All(internalNeurons, neuron =>
            {
                Assert.Contains(neuron.ActivationFunctionId, allowedInternalActivationIds);
                Assert.InRange(neuron.PreActivationThresholdCode, 0, 36);
                Assert.InRange(neuron.ActivationThresholdCode, 0, 40);
            });
        });
    }

    [Fact]
    public void NewRandomBrain_RangedParams_WithParametricActivation_ClampsParamsNearUnitRange()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "123123123";
            vm.RandomOptions.SelectedActivationMode = vm.RandomOptions.ActivationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Fixed);
            vm.RandomOptions.ActivationFixedIdText = "25";
            vm.RandomOptions.SelectedParamMode = vm.RandomOptions.ParamModes.Single(mode => mode.Value == RandomRangeMode.Range);
            vm.RandomOptions.ParamAMinText = "0";
            vm.RandomOptions.ParamAMaxText = "63";
            vm.RandomOptions.ParamBMinText = "0";
            vm.RandomOptions.ParamBMaxText = "63";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var nonInputNeurons = brain.Regions
                .Where(region => region.RegionId != NbnConstants.InputRegionId)
                .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
                .ToList();

            Assert.NotEmpty(nonInputNeurons);
            Assert.All(nonInputNeurons, neuron =>
            {
                Assert.Equal(25, neuron.ActivationFunctionId);
                Assert.InRange(neuron.ParamACode, 13, 50);
                Assert.InRange(neuron.ParamBCode, 13, 50);
            });
        });
    }

    [Fact]
    public void NewRandomBrain_WeightedReset_InternalNeuronsFollowCuratedSubsetDeterministically()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateDesignerViewModel();
            var curatedResetIds = new[] { 1, 43, 44, 45, 0 };
            var firstPassCounts = curatedResetIds.ToDictionary(id => id, _ => 0);
            var secondPassCounts = curatedResetIds.ToDictionary(id => id, _ => 0);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SelectedResetMode = vm.RandomOptions.ResetModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);
            vm.RandomOptions.SelectedNeuronCountMode = vm.RandomOptions.NeuronCountModes.Single(mode => mode.Value == RandomCountMode.Fixed);
            vm.RandomOptions.NeuronCountText = "8";
            vm.RandomOptions.RegionCountText = "30";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            for (var seed = 1; seed <= 128; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);
                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
                var internalNeurons = GetInternalNeurons(brain);
                Assert.NotEmpty(internalNeurons);
                foreach (var neuron in internalNeurons)
                {
                    Assert.Contains(neuron.ResetFunctionId, curatedResetIds);
                    firstPassCounts[neuron.ResetFunctionId]++;
                }
            }

            for (var seed = 1; seed <= 128; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);
                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
                foreach (var neuron in GetInternalNeurons(brain))
                {
                    secondPassCounts[neuron.ResetFunctionId]++;
                }
            }

            foreach (var id in curatedResetIds)
            {
                Assert.Equal(firstPassCounts[id], secondPassCounts[id]);
            }

            var total = firstPassCounts.Values.Sum();
            Assert.True(total > 0, "Expected internal neuron samples for reset-distribution checks.");

            var holdShare = firstPassCounts[1] / (double)total;
            var halfShare = firstPassCounts[43] / (double)total;
            var tenthShare = firstPassCounts[44] / (double)total;
            var hundredthShare = firstPassCounts[45] / (double)total;
            var zeroShare = firstPassCounts[0] / (double)total;

            Assert.InRange(holdShare, 0.26, 0.44);
            Assert.InRange(halfShare, 0.20, 0.38);
            Assert.InRange(tenthShare, 0.12, 0.28);
            Assert.InRange(hundredthShare, 0.05, 0.18);
            Assert.InRange(zeroShare, 0.02, 0.10);
            Assert.True(holdShare > halfShare && halfShare > tenthShare && tenthShare > hundredthShare && hundredthShare > zeroShare);
        });
    }

    [Fact]
    public void NewRandomBrain_DefaultModes_NoInputRuntimeSmoke_StaysActiveWithoutSaturation()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateDesignerViewModel();
            ConfigureSimulationProfile(vm, seedBaselineActivityPath: true);

            var seeds = Enumerable.Range(1, 24).ToArray();
            var healthySeeds = 0;
            var saturatedSeeds = 0;
            var outputActiveSeeds = 0;
            var boundedSeeds = 0;
            foreach (var seed in seeds)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);
                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
                var metrics = RunSimulation(brain, BuildConstantInputPattern(ticks: 64, value: 0f));
                if (metrics.MeanFiringFraction >= 0.01f)
                {
                    healthySeeds++;
                }

                if (metrics.MeanFiringFraction > 0.70f)
                {
                    saturatedSeeds++;
                }

                if (metrics.TicksWithOutputFiring > 0)
                {
                    outputActiveSeeds++;
                }

                if (metrics.MaxAbsOutputValue <= 1.001f && metrics.MaxAbsBufferValue <= 1.001f)
                {
                    boundedSeeds++;
                }
            }

            Assert.True(healthySeeds >= 20, $"Expected at least 20/24 seeds with observable autonomous activity, got {healthySeeds}.");
            Assert.True(saturatedSeeds <= 2, $"Expected at most 2/24 saturated seeds, got {saturatedSeeds}.");
            Assert.True(outputActiveSeeds >= 20, $"Expected at least 20/24 seeds with output-region activity, got {outputActiveSeeds}.");
            Assert.True(boundedSeeds >= 22, $"Expected at least 22/24 seeds to stay within bounded output/buffer range, got {boundedSeeds}.");
        });
    }

    [Fact]
    public void NewRandomBrain_InputCadenceRuntimeSmoke_IncreasesOutputResponsiveness()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateDesignerViewModel();
            ConfigureSimulationProfile(vm, seedBaselineActivityPath: false);

            var noInputPattern = BuildConstantInputPattern(ticks: 64, value: 0f);
            var continuousPattern = BuildConstantInputPattern(ticks: 64, value: 0.75f);
            var periodicPattern = BuildPeriodicInputPattern(ticks: 64, period: 4, pulse: 0.9f);
            var irregularPattern = BuildIrregularInputPattern(ticks: 64);

            var seedCount = 20;
            var totalNoInputOutput = 0f;
            var totalContinuousOutput = 0f;
            var totalPeriodicOutput = 0f;
            var totalIrregularOutput = 0f;
            var continuousResponsiveSeeds = 0;
            var periodicResponsiveSeeds = 0;
            var irregularResponsiveSeeds = 0;
            var irregularVariedSeeds = 0;
            var boundedSeeds = 0;

            for (var seed = 1; seed <= seedCount; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);
                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);

                var noInput = RunSimulation(brain, noInputPattern);
                var continuous = RunSimulation(brain, continuousPattern);
                var periodic = RunSimulation(brain, periodicPattern);
                var irregular = RunSimulation(brain, irregularPattern);

                totalNoInputOutput += noInput.MeanOutputMagnitude;
                totalContinuousOutput += continuous.MeanOutputMagnitude;
                totalPeriodicOutput += periodic.MeanOutputMagnitude;
                totalIrregularOutput += irregular.MeanOutputMagnitude;

                if (continuous.TicksWithInputFiring > 0 && continuous.MeanOutputMagnitude > noInput.MeanOutputMagnitude + 0.01f)
                {
                    continuousResponsiveSeeds++;
                }

                if (periodic.InputOutputBestLagCorrelation >= 0.18f
                    && periodic.TicksWithOutputFiring > 0)
                {
                    periodicResponsiveSeeds++;
                }

                if (irregular.InputOutputBestLagCorrelation >= 0.20f
                    && irregular.TicksWithOutputFiring > 0)
                {
                    irregularResponsiveSeeds++;
                }

                if (irregular.OutputStdDev > noInput.OutputStdDev + 0.01f)
                {
                    irregularVariedSeeds++;
                }

                if (noInput.MaxAbsOutputValue <= 1.001f
                    && continuous.MaxAbsOutputValue <= 1.001f
                    && periodic.MaxAbsOutputValue <= 1.001f
                    && irregular.MaxAbsOutputValue <= 1.001f
                    && noInput.MaxAbsBufferValue <= 1.001f
                    && continuous.MaxAbsBufferValue <= 1.001f
                    && periodic.MaxAbsBufferValue <= 1.001f
                    && irregular.MaxAbsBufferValue <= 1.001f)
                {
                    boundedSeeds++;
                }
            }

            Assert.True(
                totalContinuousOutput > totalNoInputOutput * 1.01f,
                $"Expected continuous output to exceed no-input by 1%. no-input={totalNoInputOutput:0.0000}, continuous={totalContinuousOutput:0.0000}");
            Assert.True(
                totalPeriodicOutput > totalNoInputOutput * 0.90f,
                $"Expected periodic output to remain within 10% of no-input baseline while preserving correlation. no-input={totalNoInputOutput:0.0000}, periodic={totalPeriodicOutput:0.0000}");
            Assert.True(
                totalIrregularOutput > totalNoInputOutput * 0.90f,
                $"Expected irregular output to remain within 10% of no-input baseline while preserving correlation. no-input={totalNoInputOutput:0.0000}, irregular={totalIrregularOutput:0.0000}");
            Assert.True(continuousResponsiveSeeds >= 7, $"Expected >=7/20 continuous-responsive seeds, got {continuousResponsiveSeeds}.");
            Assert.True(periodicResponsiveSeeds >= 8, $"Expected >=8/20 periodic-responsive seeds, got {periodicResponsiveSeeds}.");
            Assert.True(irregularResponsiveSeeds >= 8, $"Expected >=8/20 irregular-responsive seeds, got {irregularResponsiveSeeds}.");
            Assert.True(irregularVariedSeeds >= 6, $"Expected >=6/20 irregular-pattern variation seeds, got {irregularVariedSeeds}.");
            Assert.True(boundedSeeds >= 18, $"Expected >=18/20 seeds with bounded output/buffer dynamics across cadences, got {boundedSeeds}.");
        });
    }

    [Fact]
    public void NewRandomBrain_DefaultModes_PrefersIntraConnectivity_And_SeedsOutputToInputEndpointRecurrence()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateDesignerViewModel();
            ConfigureSimulationProfile(vm, seedBaselineActivityPath: false);

            var seedCount = 24;
            var intraPreferredSeeds = 0;
            var recurrenceSeeds = 0;
            var uniqueRecurrenceSeeds = 0;

            for (var seed = 1; seed <= seedCount; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);
                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);

                var intraAxons = 0;
                var interAxons = 0;
                foreach (var region in brain.Regions.Where(region => region.NeuronCount > 0))
                {
                    foreach (var neuron in region.Neurons.Where(neuron => neuron.Exists))
                    {
                        foreach (var axon in neuron.Axons)
                        {
                            if (axon.TargetRegionId == region.RegionId)
                            {
                                intraAxons++;
                            }
                            else
                            {
                                interAxons++;
                            }
                        }
                    }
                }

                if (intraAxons >= interAxons)
                {
                    intraPreferredSeeds++;
                }

                var endpointKeys = CollectInputEndpointKeys(brain);
                if (endpointKeys.Count == 0)
                {
                    continue;
                }

                var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
                var recurrenceTargets = new HashSet<(int regionId, int neuronId)>();
                var recurrenceEdges = 0;
                foreach (var outputNeuron in outputRegion.Neurons.Where(neuron => neuron.Exists))
                {
                    foreach (var axon in outputNeuron.Axons)
                    {
                        var key = (axon.TargetRegionId, axon.TargetNeuronId);
                        if (!endpointKeys.Contains(key))
                        {
                            continue;
                        }

                        recurrenceEdges++;
                        recurrenceTargets.Add(key);
                    }
                }

                if (recurrenceEdges > 0)
                {
                    recurrenceSeeds++;
                }

                if (recurrenceEdges >= 2 && recurrenceTargets.Count >= Math.Max(1, recurrenceEdges - 1))
                {
                    uniqueRecurrenceSeeds++;
                }
            }

            Assert.True(intraPreferredSeeds >= 19, $"Expected >=19/24 seeds to prefer intra-region connectivity, got {intraPreferredSeeds}.");
            Assert.True(recurrenceSeeds >= 16, $"Expected >=16/24 seeds with output->input-endpoint recurrence bridges, got {recurrenceSeeds}.");
            Assert.True(uniqueRecurrenceSeeds >= 10, $"Expected >=10/24 seeds with low sharing among recurrence targets, got {uniqueRecurrenceSeeds}.");
        });
    }

    [Fact]
    public void NewRandomBrain_AlwaysSeedsBaselineDriverOutsideInputRegion()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);
            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SelectedActivationMode = vm.RandomOptions.ActivationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Fixed);
            vm.RandomOptions.ActivationFixedIdText = "11";
            vm.RandomOptions.SeedBaselineActivityPath = true;

            for (var seed = 1; seed <= 128; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);

                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
                var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
                var outputNeuron = outputRegion.Neurons.First(neuron => neuron.Exists);
                var hasDriver = brain.Regions
                    .Where(region => region.RegionId != NbnConstants.InputRegionId && region.RegionId != NbnConstants.OutputRegionId)
                    .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
                    .Any(neuron =>
                        neuron.ActivationFunctionId == 17
                        && neuron.ResetFunctionId == 0
                        && neuron.AccumulationFunctionId == 0
                        && neuron.PreActivationThresholdCode == 0
                        && neuron.ActivationThresholdCode == 0
                        && neuron.ParamACode == 63
                        && neuron.Axons.Any(axon =>
                            axon.TargetRegionId == NbnConstants.OutputRegionId
                            && axon.TargetNeuronId == outputNeuron.NeuronId
                            && axon.StrengthCode == 31));

                Assert.True(hasDriver, $"Seed {seed} failed to retain a non-input baseline driver.");
            }
        });
    }

    [Fact]
    public void NewRandomBrain_SeedsBaselineOutputActivityPath()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "12345";
            vm.RandomOptions.SelectedActivationMode = vm.RandomOptions.ActivationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Fixed);
            vm.RandomOptions.ActivationFixedIdText = "11";
            vm.RandomOptions.SeedBaselineActivityPath = true;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
            var outputNeuron = outputRegion.Neurons.First(neuron => neuron.Exists);
            Assert.Equal(1, outputNeuron.ActivationFunctionId);
            Assert.Equal(0, outputNeuron.ResetFunctionId);
            Assert.Equal(0, outputNeuron.AccumulationFunctionId);
            Assert.Equal(0, outputNeuron.PreActivationThresholdCode);
            Assert.Equal(0, outputNeuron.ActivationThresholdCode);

            var hasDriver = brain.Regions
                .Where(region => region.RegionId != NbnConstants.OutputRegionId)
                .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
                .Any(neuron =>
                    neuron.ActivationFunctionId == 17
                    && neuron.ResetFunctionId == 0
                    && neuron.AccumulationFunctionId == 0
                    && neuron.PreActivationThresholdCode == 0
                    && neuron.ActivationThresholdCode == 0
                    && neuron.ParamACode == 63
                    && neuron.Axons.Any(axon =>
                        axon.TargetRegionId == NbnConstants.OutputRegionId
                        && axon.TargetNeuronId == outputNeuron.NeuronId
                        && axon.StrengthCode == 31));

            Assert.True(hasDriver, "Expected a seeded driver neuron with an explicit strong path into the output region.");
        });
    }

    [Fact]
    public void NewRandomBrain_DoesNotSeedBaselineOutputDriver_WhenDisabled()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "12345";
            vm.RandomOptions.SelectedActivationMode = vm.RandomOptions.ActivationModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Fixed);
            vm.RandomOptions.ActivationFixedIdText = "11";
            vm.RandomOptions.SeedBaselineActivityPath = false;

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
            var outputNeuron = outputRegion.Neurons.First(neuron => neuron.Exists);

            var hasDriver = brain.Regions
                .Where(region => region.RegionId != NbnConstants.InputRegionId && region.RegionId != NbnConstants.OutputRegionId)
                .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
                .Any(neuron =>
                    neuron.ActivationFunctionId == 17
                    && neuron.ResetFunctionId == 0
                    && neuron.AccumulationFunctionId == 0
                    && neuron.PreActivationThresholdCode == 0
                    && neuron.ActivationThresholdCode == 0
                    && neuron.ParamACode == 63
                    && neuron.Axons.Any(axon =>
                        axon.TargetRegionId == NbnConstants.OutputRegionId
                        && axon.TargetNeuronId == outputNeuron.NeuronId
                        && axon.StrengthCode == 31));

            Assert.False(hasDriver, "Expected no seeded baseline driver when the toggle is disabled.");
        });
    }

    private static DesignerPanelViewModel CreateDesignerViewModel()
    {
        var connections = new ConnectionViewModel();
        var client = new WorkbenchClient(new NullWorkbenchEventSink());
        return new DesignerPanelViewModel(connections, client);
    }

    private static List<DesignerNeuronViewModel> GetInternalNeurons(DesignerBrainViewModel brain)
    {
        return brain.Regions
            .Where(region => region.RegionId != NbnConstants.InputRegionId && region.RegionId != NbnConstants.OutputRegionId)
            .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
            .ToList();
    }

    private static HashSet<(int regionId, int neuronId)> CollectInputEndpointKeys(DesignerBrainViewModel brain)
    {
        var endpoints = new HashSet<(int regionId, int neuronId)>();
        var inputRegion = brain.Regions[NbnConstants.InputRegionId];
        foreach (var inputNeuron in inputRegion.Neurons.Where(neuron => neuron.Exists))
        {
            foreach (var axon in inputNeuron.Axons)
            {
                if (axon.TargetRegionId == NbnConstants.InputRegionId
                    || axon.TargetRegionId == NbnConstants.OutputRegionId)
                {
                    continue;
                }

                endpoints.Add((axon.TargetRegionId, axon.TargetNeuronId));
            }
        }

        return endpoints;
    }

    private static void ConfigureSimulationProfile(DesignerPanelViewModel vm, bool seedBaselineActivityPath)
    {
        vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
        vm.RandomOptions.SelectedRegionSelectionMode = vm.RandomOptions.RegionSelectionModes.Single(mode => mode.Value == RandomRegionSelectionMode.Random);
        vm.RandomOptions.RegionCountText = "12";
        vm.RandomOptions.SelectedNeuronCountMode = vm.RandomOptions.NeuronCountModes.Single(mode => mode.Value == RandomCountMode.Fixed);
        vm.RandomOptions.NeuronCountText = "8";
        vm.RandomOptions.SelectedAxonCountMode = vm.RandomOptions.AxonCountModes.Single(mode => mode.Value == RandomCountMode.Fixed);
        vm.RandomOptions.AxonCountText = "5";
        vm.RandomOptions.InputNeuronCountText = "6";
        vm.RandomOptions.OutputNeuronCountText = "6";
        vm.RandomOptions.SelectedResetMode = vm.RandomOptions.ResetModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);
        vm.RandomOptions.SeedBaselineActivityPath = seedBaselineActivityPath;
    }

    private static float[] BuildConstantInputPattern(int ticks, float value)
    {
        return Enumerable.Repeat(value, ticks).ToArray();
    }

    private static float[] BuildPeriodicInputPattern(int ticks, int period, float pulse)
    {
        var values = new float[ticks];
        for (var i = 0; i < ticks; i++)
        {
            values[i] = i % period == 0 ? pulse : 0f;
        }

        return values;
    }

    private static float[] BuildIrregularInputPattern(int ticks)
    {
        var sequence = new[] { 0.95f, -0.70f, 0.0f, 0.40f, -0.25f, 0.80f, 0.0f, -0.55f, 0.35f, 0.0f };
        var values = new float[ticks];
        for (var i = 0; i < ticks; i++)
        {
            values[i] = sequence[i % sequence.Length];
        }

        return values;
    }

    private static BrainSimulationMetrics RunSimulation(DesignerBrainViewModel brain, IReadOnlyList<float> inputPattern)
    {
        var regionsWithNeurons = brain.Regions.Where(region => region.NeuronCount > 0).OrderBy(region => region.RegionId).ToList();
        var regionSpans = new int[NbnConstants.RegionCount];
        foreach (var region in brain.Regions)
        {
            regionSpans[region.RegionId] = region.NeuronCount;
        }

        var statesByRegion = new Dictionary<int, RegionShardState>(regionsWithNeurons.Count);
        var backendsByRegion = new Dictionary<int, RegionShardCpuBackend>(regionsWithNeurons.Count);
        var routingMap = new Dictionary<int, ShardSpan[]>(regionsWithNeurons.Count);
        foreach (var region in regionsWithNeurons)
        {
            var state = BuildRegionState(brain, region, regionSpans);
            statesByRegion[region.RegionId] = state;
            backendsByRegion[region.RegionId] = new RegionShardCpuBackend(state);
            routingMap[region.RegionId] = new[] { new ShardSpan(0, region.NeuronCount, ShardId32.From(region.RegionId, 0)) };
        }

        var routing = new RegionShardRoutingTable(routingMap);
        statesByRegion.TryGetValue(NbnConstants.InputRegionId, out var inputState);
        var totalFired = 0L;
        var ticksWithAnyFiring = 0;
        var ticksWithInputFiring = 0;
        var ticksWithOutputFiring = 0;
        var outputMagnitudes = new float[inputPattern.Count];
        var maxAbsOutputValue = 0f;
        var maxAbsBufferValue = 0f;

        for (var tickIndex = 0; tickIndex < inputPattern.Count; tickIndex++)
        {
            var inputValue = inputPattern[tickIndex];
            if (inputState is not null && inputValue != 0f)
            {
                for (var neuronId = 0; neuronId < inputState.NeuronCount; neuronId++)
                {
                    if (!inputState.Exists[neuronId])
                    {
                        continue;
                    }

                    inputState.ApplyContribution((uint)neuronId, inputValue);
                }
            }

            var pending = new Dictionary<ShardId32, List<Contribution>>();
            var tickFired = 0;
            var tickInputFired = 0;
            var tickOutputFired = 0;
            var tickOutputMagnitude = 0f;
            foreach (var region in regionsWithNeurons)
            {
                var regionId = region.RegionId;
                var backend = backendsByRegion[regionId];
                var result = backend.Compute(
                    tickId: (ulong)(tickIndex + 1),
                    brainId: brain.BrainId,
                    shardId: ShardId32.From(regionId, 0),
                    routing: routing,
                    visualization: RegionShardVisualizationComputeScope.Disabled,
                    plasticityEnabled: false,
                    homeostasisConfig: RegionShardHomeostasisConfig.Default);

                tickFired += (int)result.FiredCount;
                if (regionId == NbnConstants.InputRegionId)
                {
                    tickInputFired += (int)result.FiredCount;
                }

                if (regionId == NbnConstants.OutputRegionId)
                {
                    tickOutputFired += (int)result.FiredCount;
                    tickOutputMagnitude = SumMagnitude(result.OutputVector);
                    maxAbsOutputValue = Math.Max(maxAbsOutputValue, MaxAbs(result.OutputVector));
                }

                foreach (var (destinationShard, contributions) in result.Outbox)
                {
                    if (!pending.TryGetValue(destinationShard, out var batch))
                    {
                        batch = new List<Contribution>(contributions.Count);
                        pending[destinationShard] = batch;
                    }

                    batch.AddRange(contributions);
                }
            }

            foreach (var (destinationShard, contributions) in pending)
            {
                if (!statesByRegion.TryGetValue(destinationShard.RegionId, out var destinationState))
                {
                    continue;
                }

                foreach (var contribution in contributions)
                {
                    destinationState.ApplyContribution(contribution.TargetNeuronId, contribution.Value);
                }
            }

            foreach (var state in statesByRegion.Values)
            {
                for (var neuronIndex = 0; neuronIndex < state.NeuronCount; neuronIndex++)
                {
                    maxAbsBufferValue = Math.Max(maxAbsBufferValue, MathF.Abs(state.Buffer[neuronIndex]));
                }
            }

            totalFired += tickFired;
            if (tickFired > 0)
            {
                ticksWithAnyFiring++;
            }

            if (tickInputFired > 0)
            {
                ticksWithInputFiring++;
            }

            if (tickOutputFired > 0)
            {
                ticksWithOutputFiring++;
            }

            outputMagnitudes[tickIndex] = tickOutputMagnitude;
        }

        var neuronCount = brain.Regions.Sum(region => region.NeuronCount);
        var meanOutput = outputMagnitudes.Length == 0 ? 0f : outputMagnitudes.Sum() / outputMagnitudes.Length;
        var outputStdDev = ComputeStdDev(outputMagnitudes, meanOutput);
        var inputOutputBestLagCorrelation = ComputeBestLaggedCorrelation(inputPattern, outputMagnitudes, maxLag: 6);
        return new BrainSimulationMetrics(
            TickCount: inputPattern.Count,
            NeuronCount: neuronCount,
            TotalFired: totalFired,
            TicksWithAnyFiring: ticksWithAnyFiring,
            TicksWithInputFiring: ticksWithInputFiring,
            TicksWithOutputFiring: ticksWithOutputFiring,
            MeanOutputMagnitude: meanOutput,
            OutputStdDev: outputStdDev,
            MaxAbsOutputValue: maxAbsOutputValue,
            MaxAbsBufferValue: maxAbsBufferValue,
            InputOutputBestLagCorrelation: inputOutputBestLagCorrelation);
    }

    private static RegionShardState BuildRegionState(DesignerBrainViewModel brain, DesignerRegionViewModel region, int[] regionSpans)
    {
        var neurons = region.Neurons.OrderBy(neuron => neuron.NeuronId).ToArray();
        var neuronCount = neurons.Length;
        var quantization = QuantizationSchemas.DefaultNbn;
        var buffer = new float[neuronCount];
        var enabled = new bool[neuronCount];
        var exists = new bool[neuronCount];
        var accumulationFunctions = new byte[neuronCount];
        var activationFunctions = new byte[neuronCount];
        var resetFunctions = new byte[neuronCount];
        var paramA = new float[neuronCount];
        var paramB = new float[neuronCount];
        var preActivationThreshold = new float[neuronCount];
        var activationThreshold = new float[neuronCount];
        var axonCounts = new ushort[neuronCount];
        var axonStartOffsets = new int[neuronCount];

        var totalAxons = neurons.Sum(neuron => neuron.Axons.Count);
        var targetRegionIds = new byte[totalAxons];
        var targetNeuronIds = new int[totalAxons];
        var strengths = new float[totalAxons];
        var baseStrengthCodes = new byte[totalAxons];
        var runtimeStrengthCodes = new byte[totalAxons];
        var hasRuntimeOverlay = new bool[totalAxons];
        var fromAddress32 = new uint[totalAxons];
        var toAddress32 = new uint[totalAxons];

        var axonIndex = 0;
        for (var i = 0; i < neurons.Length; i++)
        {
            var neuron = neurons[i];
            exists[i] = neuron.Exists;
            enabled[i] = neuron.Exists;
            accumulationFunctions[i] = (byte)neuron.AccumulationFunctionId;
            activationFunctions[i] = (byte)neuron.ActivationFunctionId;
            resetFunctions[i] = (byte)neuron.ResetFunctionId;
            paramA[i] = quantization.ParamA.Decode(neuron.ParamACode, bits: 6);
            paramB[i] = quantization.ParamB.Decode(neuron.ParamBCode, bits: 6);
            preActivationThreshold[i] = quantization.PreActivationThreshold.Decode(neuron.PreActivationThresholdCode, bits: 6);
            activationThreshold[i] = quantization.ActivationThreshold.Decode(neuron.ActivationThresholdCode, bits: 6);
            axonCounts[i] = checked((ushort)neuron.Axons.Count);
            axonStartOffsets[i] = axonIndex;

            foreach (var axon in neuron.Axons)
            {
                var strengthCode = (byte)Math.Clamp(axon.StrengthCode, 0, 31);
                targetRegionIds[axonIndex] = (byte)axon.TargetRegionId;
                targetNeuronIds[axonIndex] = axon.TargetNeuronId;
                strengths[axonIndex] = quantization.Strength.Decode(strengthCode, bits: 5);
                baseStrengthCodes[axonIndex] = strengthCode;
                runtimeStrengthCodes[axonIndex] = strengthCode;
                hasRuntimeOverlay[axonIndex] = false;
                fromAddress32[axonIndex] = Address32.From(region.RegionId, neuron.NeuronId).Value;
                toAddress32[axonIndex] = Address32.From(axon.TargetRegionId, axon.TargetNeuronId).Value;
                axonIndex++;
            }
        }

        return new RegionShardState(
            regionId: region.RegionId,
            neuronStart: 0,
            neuronCount: neuronCount,
            brainSeed: brain.BrainSeed,
            strengthQuantization: quantization.Strength,
            regionSpans: (int[])regionSpans.Clone(),
            buffer: buffer,
            enabled: enabled,
            exists: exists,
            accumulationFunctions: accumulationFunctions,
            activationFunctions: activationFunctions,
            resetFunctions: resetFunctions,
            paramA: paramA,
            paramB: paramB,
            preActivationThreshold: preActivationThreshold,
            activationThreshold: activationThreshold,
            axonCounts: axonCounts,
            axonStartOffsets: axonStartOffsets,
            axons: new RegionShardAxons(
                targetRegionIds: targetRegionIds,
                targetNeuronIds: targetNeuronIds,
                strengths: strengths,
                baseStrengthCodes: baseStrengthCodes,
                runtimeStrengthCodes: runtimeStrengthCodes,
                hasRuntimeOverlay: hasRuntimeOverlay,
                fromAddress32: fromAddress32,
                toAddress32: toAddress32));
    }

    private static float SumMagnitude(IReadOnlyList<float> values)
    {
        var sum = 0f;
        for (var i = 0; i < values.Count; i++)
        {
            sum += MathF.Abs(values[i]);
        }

        return sum;
    }

    private static float MaxAbs(IReadOnlyList<float> values)
    {
        var max = 0f;
        for (var i = 0; i < values.Count; i++)
        {
            max = Math.Max(max, MathF.Abs(values[i]));
        }

        return max;
    }

    private static float ComputeStdDev(IReadOnlyList<float> values, float mean)
    {
        if (values.Count == 0)
        {
            return 0f;
        }

        var sumSquares = 0f;
        for (var i = 0; i < values.Count; i++)
        {
            var delta = values[i] - mean;
            sumSquares += delta * delta;
        }

        return MathF.Sqrt(sumSquares / values.Count);
    }

    private static float ComputeBestLaggedCorrelation(IReadOnlyList<float> inputPattern, IReadOnlyList<float> outputMagnitudes, int maxLag)
    {
        var count = Math.Min(inputPattern.Count, outputMagnitudes.Count);
        if (count <= 2)
        {
            return 0f;
        }

        var lagLimit = Math.Min(maxLag, count - 2);
        var best = 0f;
        for (var lag = 0; lag <= lagLimit; lag++)
        {
            var samples = count - lag;
            if (samples <= 2)
            {
                continue;
            }

            var sumX = 0f;
            var sumY = 0f;
            for (var i = 0; i < samples; i++)
            {
                sumX += MathF.Abs(inputPattern[i]);
                sumY += outputMagnitudes[i + lag];
            }

            var meanX = sumX / samples;
            var meanY = sumY / samples;
            var covariance = 0f;
            var varX = 0f;
            var varY = 0f;
            for (var i = 0; i < samples; i++)
            {
                var x = MathF.Abs(inputPattern[i]) - meanX;
                var y = outputMagnitudes[i + lag] - meanY;
                covariance += x * y;
                varX += x * x;
                varY += y * y;
            }

            if (varX <= 1e-8f || varY <= 1e-8f)
            {
                continue;
            }

            var correlation = covariance / MathF.Sqrt(varX * varY);
            if (correlation > best)
            {
                best = correlation;
            }
        }

        return best;
    }

    private readonly record struct BrainSimulationMetrics(
        int TickCount,
        int NeuronCount,
        long TotalFired,
        int TicksWithAnyFiring,
        int TicksWithInputFiring,
        int TicksWithOutputFiring,
        float MeanOutputMagnitude,
        float OutputStdDev,
        float MaxAbsOutputValue,
        float MaxAbsBufferValue,
        float InputOutputBestLagCorrelation)
    {
        public float MeanFiringFraction
            => TickCount <= 0 || NeuronCount <= 0
                ? 0f
                : TotalFired / (float)(TickCount * NeuronCount);
    }

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnSettingChanged(SettingItem item) { }
    }
}
