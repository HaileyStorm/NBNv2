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
    public void NewRandomBrain_SeedsBaselineOutputActivityPath()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var connections = new ConnectionViewModel();
            var client = new WorkbenchClient(new NullWorkbenchEventSink());
            var vm = new DesignerPanelViewModel(connections, client);

            vm.RandomOptions.SelectedSeedMode = vm.RandomOptions.SeedModes.Single(mode => mode.Value == RandomSeedMode.Fixed);
            vm.RandomOptions.SeedText = "12345";

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
