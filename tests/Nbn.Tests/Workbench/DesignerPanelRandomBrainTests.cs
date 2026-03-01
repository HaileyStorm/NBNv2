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
    public void NewRandomBrain_DefaultModes_OutputNeuronsUseResetZero()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "78901";
            vm.RandomOptions.OutputNeuronCountText = "6";
            vm.RandomOptions.SelectedResetMode = vm.RandomOptions.ResetModes.Single(mode => mode.Value == RandomFunctionSelectionMode.Weighted);

            vm.NewRandomBrainCommand.Execute(null);

            var brain = Assert.IsType<DesignerBrainViewModel>(vm.Brain);
            var outputRegion = brain.Regions[NbnConstants.OutputRegionId];
            Assert.Equal(6, outputRegion.NeuronCount);
            Assert.All(outputRegion.Neurons.Where(neuron => neuron.Exists), neuron => Assert.Equal(0, neuron.ResetFunctionId));
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
