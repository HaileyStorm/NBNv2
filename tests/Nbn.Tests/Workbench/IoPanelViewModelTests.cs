using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using Nbn.Proto.Io;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class IoPanelViewModelTests
{
    [Fact]
    public void BuildSuggestedVector_UsesBoundedBiasedRandomValues()
    {
        var method = typeof(IoPanelViewModel).GetMethod("BuildSuggestedVector", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var raw = Assert.IsType<string>(method!.Invoke(null, new object[] { 32 }));
        var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Equal(32, parts.Length);

        foreach (var part in parts)
        {
            Assert.True(float.TryParse(part, NumberStyles.Float, CultureInfo.InvariantCulture, out var value));
            Assert.InRange(value, -1f, 1f);
            Assert.True(MathF.Abs(value) >= 0.15f, $"Expected |value| >= 0.15 but found {value.ToString(CultureInfo.InvariantCulture)}");
        }
    }

    [Fact]
    public void BuildSuggestedVector_UsesInputWidth()
    {
        var method = typeof(IoPanelViewModel).GetMethod("BuildSuggestedVector", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var raw = Assert.IsType<string>(method!.Invoke(null, new object[] { 300 }));
        var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Equal(300, parts.Length);
    }

    [Fact]
    public void SendVectorCommand_IsDisabled_WhenContinuousAutoSendEnabled()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        Assert.True(vm.SendVectorCommand.CanExecute(null));

        vm.AutoSendInputVectorEveryTick = true;
        Assert.False(vm.SendVectorCommand.CanExecute(null));

        vm.AutoSendInputVectorEveryTick = false;
        Assert.True(vm.SendVectorCommand.CanExecute(null));
    }

    [Fact]
    public void ApplyBrainInfo_DoesNotRegenerateSuggestion_WhenInputAlreadyPopulated()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var method = typeof(IoPanelViewModel).GetMethod("ApplyBrainInfo", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(method);
        var info = new BrainInfo { InputWidth = 8, OutputWidth = 4 };

        method!.Invoke(vm, new object?[] { info });
        var first = vm.InputVectorText;
        Assert.False(string.IsNullOrWhiteSpace(first));

        method.Invoke(vm, new object?[] { info });
        Assert.Equal(first, vm.InputVectorText);
    }

    [Fact]
    public void AutoSendInputVectorEveryTick_DefaultsOff()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        Assert.False(vm.AutoSendInputVectorEveryTick);
    }

    [Fact]
    public async Task AddVectorEvent_AutoSendEnabled_SendsOncePerTick_ForSelectedBrain()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.InputVectorText = "1.0, -0.25, 0.5";
        vm.AutoSendInputVectorEveryTick = true;

        vm.AddVectorEvent(CreateVectorEvent(brainId, tickId: 42));
        vm.AddVectorEvent(CreateVectorEvent(brainId, tickId: 42));
        vm.AddVectorEvent(CreateVectorEvent(brainId, tickId: 43));

        await WaitForAsync(() => client.InputVectorCalls.Count == 2);

        Assert.All(client.InputVectorCalls, call => Assert.Equal(brainId, call.BrainId));
        Assert.Equal(new[] { 1f, -0.25f, 0.5f }, client.InputVectorCalls[0].Values);
        Assert.Equal(new[] { 1f, -0.25f, 0.5f }, client.InputVectorCalls[1].Values);
    }

    [Fact]
    public async Task AddVectorEvent_AutoSendEnabled_WithEmptyVector_DoesNotSend()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.InputVectorText = string.Empty;
        vm.AutoSendInputVectorEveryTick = true;

        vm.AddVectorEvent(CreateVectorEvent(brainId, tickId: 55));
        await WaitForAsync(() => vm.LastOutputTickLabel == "55");

        Assert.Empty(client.InputVectorCalls);
    }

    [Fact]
    public async Task ApplyCostEnergy_Uses_Independent_Flag_Values()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        vm.UpdateActiveBrains(new[] { brainA, brainB });
        vm.CostEnabled = true;
        vm.EnergyEnabled = false;

        vm.ApplyCostEnergyCommand.Execute(null);
        await WaitForAsync(() => client.CostEnergyCalls.Count == 2);

        Assert.Contains(client.CostEnergyCalls, call => call.BrainId == brainA && call.CostEnabled && !call.EnergyEnabled);
        Assert.Contains(client.CostEnergyCalls, call => call.BrainId == brainB && call.CostEnabled && !call.EnergyEnabled);
    }

    [Fact]
    public async Task ApplyCostEnergy_PartialFailure_Reports_Failed_Brain_Ids()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        vm.UpdateActiveBrains(new[] { brainA, brainB });
        vm.CostEnabled = true;
        vm.EnergyEnabled = true;

        client.CostEnergyResults[brainA] = new IoCommandResult(brainA, "set_cost_energy", true, "applied");
        client.CostEnergyResults[brainB] = new IoCommandResult(brainB, "set_cost_energy", false, "brain_not_found");

        vm.ApplyCostEnergyCommand.Execute(null);
        await WaitForAsync(() => vm.BrainInfoSummary.Contains("Failed:", StringComparison.OrdinalIgnoreCase));

        Assert.Contains("1/2 succeeded", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(brainB.ToString("D"), vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("brain_not_found", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ApplyPlasticity_InvalidRate_Shows_Validation_Error()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.UpdateActiveBrains(new[] { Guid.NewGuid() });
        vm.PlasticityRateText = "not-a-number";

        vm.ApplyPlasticityCommand.Execute(null);

        Assert.Equal("Plasticity rate invalid.", vm.BrainInfoSummary);
    }

    [Fact]
    public async Task ApplyEnergyRate_AllSuccess_Shows_Success_Count()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        vm.UpdateActiveBrains(new[] { brainA, brainB });
        vm.EnergyRateText = "9";

        vm.ApplyEnergyRateCommand.Execute(null);
        await WaitForAsync(() => vm.BrainInfoSummary.Contains("applied to", StringComparison.OrdinalIgnoreCase));

        Assert.Contains("Energy rate: applied to 2 brain(s).", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
    }

    private static IoPanelViewModel CreateViewModel(WorkbenchClient client)
    {
        var dispatcher = new UiDispatcher();
        return new IoPanelViewModel(client, dispatcher);
    }

    private static OutputVectorEventItem CreateVectorEvent(Guid brainId, ulong tickId)
    {
        var now = DateTimeOffset.UtcNow;
        return new OutputVectorEventItem(now, now.ToString("g", CultureInfo.InvariantCulture), brainId.ToString("D"), "1, -0.25, 0.5", AllZero: false, tickId);
    }

    private static async Task WaitForAsync(Func<bool> predicate, int timeoutMs = 2000)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.ElapsedMilliseconds < timeoutMs)
        {
            if (predicate())
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.True(predicate());
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public List<(Guid BrainId, bool CostEnabled, bool EnergyEnabled)> CostEnergyCalls { get; } = new();
        public Dictionary<Guid, IoCommandResult> CostEnergyResults { get; } = new();
        public List<(Guid BrainId, float[] Values)> InputVectorCalls { get; } = new();

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<IoCommandResult> SendEnergyCreditAsync(Guid brainId, long amount)
        {
            return Task.FromResult(new IoCommandResult(brainId, "energy_credit", true, "applied"));
        }

        public override Task<IoCommandResult> SendEnergyRateAsync(Guid brainId, long unitsPerSecond)
        {
            return Task.FromResult(new IoCommandResult(brainId, "energy_rate", true, "applied"));
        }

        public override Task<IoCommandResult> SetCostEnergyAsync(Guid brainId, bool costEnabled, bool energyEnabled)
        {
            CostEnergyCalls.Add((brainId, costEnabled, energyEnabled));
            if (CostEnergyResults.TryGetValue(brainId, out var result))
            {
                return Task.FromResult(result);
            }

            return Task.FromResult(new IoCommandResult(brainId, "set_cost_energy", true, "applied"));
        }

        public override Task<IoCommandResult> SetPlasticityAsync(Guid brainId, bool enabled, float rate, bool probabilistic)
        {
            return Task.FromResult(new IoCommandResult(
                brainId,
                "set_plasticity",
                true,
                "applied",
                new BrainEnergyState
                {
                    PlasticityEnabled = enabled,
                    PlasticityRate = rate,
                    PlasticityProbabilisticUpdates = probabilistic
                }));
        }

        public override void SendInputVector(Guid brainId, IReadOnlyList<float> values)
        {
            InputVectorCalls.Add((brainId, values.ToArray()));
        }
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
