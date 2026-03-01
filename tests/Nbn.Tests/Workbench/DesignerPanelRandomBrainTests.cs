using System.Globalization;
using System.Linq;
using Nbn.Shared;
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
    public void NewRandomBrain_WeightedResetMode_OutputNeuronsFavorResetZeroAndAllowDecayResets()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);
            var allowedOutputResetIds = new HashSet<int> { 0, 43, 44, 45, 47, 48, 49, 58 };
            var resetCounts = new Dictionary<int, int>();

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
                foreach (var neuron in outputRegion.Neurons.Where(neuron => neuron.Exists))
                {
                    Assert.Contains(neuron.ResetFunctionId, allowedOutputResetIds);
                    if (!resetCounts.TryAdd(neuron.ResetFunctionId, 1))
                    {
                        resetCounts[neuron.ResetFunctionId]++;
                    }
                }
            }

            var zeroCount = resetCounts.TryGetValue(0, out var count) ? count : 0;
            var nonZeroCount = resetCounts.Where(entry => entry.Key != 0).Sum(entry => entry.Value);
            Assert.True(nonZeroCount > 0, "Expected output reset selection to include non-zero decay reset functions.");
            Assert.True(zeroCount > nonZeroCount, "Expected RESET_ZERO to remain the dominant output reset pick.");
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
                Assert.Equal(2, neuron.AccumulationFunctionId);
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
            var allowedInternalActivationIds = new HashSet<int> { 1, 5, 6, 7, 8, 9, 11, 18, 28 };

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
    public void NewRandomBrain_WeightedReset_AvoidsBufferAmplifyingResetFamilies()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);
            var disallowedResetIds = new HashSet<int>(Enumerable.Range(4, 13)) { 2 };

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SelectedResetMode = vm.RandomOptions.ResetModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);
            vm.RandomOptions.SeedBaselineActivityPath = false;

            for (var seed = 1; seed <= 32; seed++)
            {
                vm.RandomOptions.SeedText = seed.ToString(CultureInfo.InvariantCulture);
                vm.NewRandomBrainCommand.Execute(null);

                var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
                var internalNeurons = brain.Regions
                    .Where(region => region.RegionId != NbnConstants.InputRegionId && region.RegionId != NbnConstants.OutputRegionId)
                    .SelectMany(region => region.Neurons.Where(neuron => neuron.Exists))
                    .ToList();

                Assert.NotEmpty(internalNeurons);
                Assert.All(internalNeurons, neuron => Assert.DoesNotContain(neuron.ResetFunctionId, disallowedResetIds));
            }
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
