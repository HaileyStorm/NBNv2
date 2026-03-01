using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Shared;
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
    public void Plasticity_Defaults_To_Enabled_Probabilistic_With_PositiveRate()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());

        Assert.True(vm.PlasticityEnabled);
        Assert.True(vm.SelectedPlasticityMode.Probabilistic);
        Assert.True(float.TryParse(vm.PlasticityRateText, NumberStyles.Float, CultureInfo.InvariantCulture, out var rate));
        Assert.True(rate > 0f);
    }

    [Fact]
    public async Task AddVectorEvent_AutoSendEnabled_DoesNotSend()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.InputVectorText = "1.0, -0.25, 0.5";
        vm.AutoSendInputVectorEveryTick = true;

        vm.AddVectorEvent(CreateVectorEvent(brainId, tickId: 42));
        vm.AddVectorEvent(CreateVectorEvent(brainId, tickId: 43));
        await WaitForAsync(() => vm.LastOutputTickLabel == "43");

        Assert.Empty(client.InputVectorCalls);
    }

    [Fact]
    public async Task ObserveTick_AutoSendEnabled_WithEmptyVector_DoesNotSend()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.InputVectorText = string.Empty;
        vm.AutoSendInputVectorEveryTick = true;

        vm.ObserveTick(tickId: 55);
        await WaitForAsync(() => vm.BrainInfoSummary.Contains("Vector is empty.", StringComparison.OrdinalIgnoreCase));

        Assert.Empty(client.InputVectorCalls);
    }

    [Fact]
    public async Task ObserveTick_AutoSendEnabled_SendsOncePerTick_ForSelectedBrain()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.InputVectorText = "0.9,0.8,0.7";
        vm.AutoSendInputVectorEveryTick = true;

        vm.ObserveTick(tickId: 77);
        vm.ObserveTick(tickId: 77);
        vm.ObserveTick(tickId: 78);

        await WaitForAsync(() => client.InputVectorCalls.Count == 2);

        Assert.All(client.InputVectorCalls, call => Assert.Equal(brainId, call.BrainId));
        Assert.Equal(new[] { 0.9f, 0.8f, 0.7f }, client.InputVectorCalls[0].Values);
        Assert.Equal(new[] { 0.9f, 0.8f, 0.7f }, client.InputVectorCalls[1].Values);
    }

    [Fact]
    public async Task ObserveTick_AutoSendEnabled_WithWidthMismatch_DoesNotSend()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        ApplyBrainInfo(vm, new BrainInfo { InputWidth = 4, OutputWidth = 1 });
        vm.InputVectorText = "1.0, -0.25, 0.5";
        vm.AutoSendInputVectorEveryTick = true;

        vm.ObserveTick(tickId: 56);
        await WaitForAsync(() => vm.BrainInfoSummary.Contains("expected 4, got 3", StringComparison.OrdinalIgnoreCase));

        Assert.Empty(client.InputVectorCalls);
        Assert.Contains("expected 4, got 3", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SendVector_InvalidToken_ShowsValidationError()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.InputVectorText = "1.0, nope, 0.5";

        vm.SendVectorCommand.Execute(null);

        Assert.Empty(client.InputVectorCalls);
        Assert.Contains("Vector value #2 is invalid.", vm.BrainInfoSummary, StringComparison.Ordinal);
    }

    [Fact]
    public void SendVector_WidthMismatch_ShowsValidationError()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        ApplyBrainInfo(vm, new BrainInfo { InputWidth = 5, OutputWidth = 1 });
        vm.InputVectorText = "1,1,1";

        vm.SendVectorCommand.Execute(null);

        Assert.Empty(client.InputVectorCalls);
        Assert.Contains("expected 5, got 3", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CostEnergyCheckbox_AutoApplies_System_Setting()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        vm.SystemCostEnergyEnabledDraft = false;

        await WaitForAsync(() => client.SettingCalls.Count == 1);

        Assert.Contains(client.SettingCalls, call => call.Key == CostEnergySettingsKeys.SystemEnabledKey && call.Value == "false");
        Assert.False(vm.SystemCostEnergyEnabled);
        Assert.False(vm.SystemCostEnergyEnabledDraft);
        Assert.Contains("disabled", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PlasticityCheckbox_AutoApplies_System_Setting()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        vm.SystemPlasticityEnabledDraft = false;

        await WaitForAsync(() => client.SettingCalls.Count == 1);

        Assert.Contains(client.SettingCalls, call => call.Key == PlasticitySettingsKeys.SystemEnabledKey && call.Value == "false");
        Assert.False(vm.SystemPlasticityEnabled);
    }

    [Fact]
    public void SuppressedFlags_Track_Inverse_Enabled_State()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());

        vm.CostEnergyEnabled = true;
        Assert.False(vm.CostEnergySuppressed);
        vm.CostEnergySuppressed = true;
        Assert.False(vm.CostEnergyEnabled);

        vm.PlasticityEnabled = true;
        Assert.False(vm.PlasticitySuppressed);
        vm.PlasticitySuppressed = true;
        Assert.False(vm.PlasticityEnabled);
    }

    [Fact]
    public void ApplySetting_Updates_SystemPolicyIndicators()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());

        Assert.True(vm.ApplySetting(new SettingItem(CostEnergySettingsKeys.SystemEnabledKey, "false", string.Empty)));
        Assert.False(vm.SystemCostEnergyEnabled);
        Assert.False(vm.SystemCostEnergyEnabledDraft);
        Assert.False(vm.CostEnergyOverrideAvailable);
        Assert.True(vm.CostEnergyOverrideUnavailable);

        Assert.True(vm.ApplySetting(new SettingItem(PlasticitySettingsKeys.SystemEnabledKey, "false", string.Empty)));
        Assert.False(vm.SystemPlasticityEnabled);
        Assert.False(vm.SystemPlasticityEnabledDraft);
        Assert.False(vm.PlasticityOverrideAvailable);
        Assert.True(vm.PlasticityOverrideUnavailable);

        Assert.True(vm.ApplySetting(new SettingItem(PlasticitySettingsKeys.SystemRateKey, "0.25", string.Empty)));
        Assert.Equal("0.25", vm.SystemPlasticityRateText);
        Assert.Equal("0.25", vm.SystemPlasticityRateTextDraft);

        Assert.True(vm.ApplySetting(new SettingItem(PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, "false", string.Empty)));
        Assert.False(vm.SystemPlasticityProbabilisticUpdates);
        Assert.False(vm.SystemPlasticityProbabilisticUpdatesDraft);
        Assert.False(vm.SelectedSystemPlasticityModeDraft.Probabilistic);
    }

    [Fact]
    public async Task ApplyCostEnergy_SettingsUnavailable_Shows_Error()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        client.ReturnNullOnSetSetting = true;
        vm.SystemCostEnergyEnabledDraft = false;

        await WaitForAsync(() => vm.BrainInfoSummary.Contains("settings unavailable", StringComparison.OrdinalIgnoreCase));

        Assert.True(vm.SystemCostEnergyEnabled);
    }

    [Fact]
    public async Task ApplyPlasticity_UnexpectedResponseKey_Shows_Error_AndPreservesState()
    {
        var client = new FakeWorkbenchClient
        {
            SetSettingResponseKeyOverride = "unexpected.key"
        };
        var vm = CreateViewModel(client);
        vm.SystemPlasticityEnabledDraft = false;

        await WaitForAsync(() => vm.BrainInfoSummary.Contains("unexpected key", StringComparison.OrdinalIgnoreCase));

        Assert.True(vm.SystemPlasticityEnabled);
    }

    [Fact]
    public async Task ApplyPlasticityModeRate_Writes_System_Settings()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        vm.UpdateActiveBrains(new[] { brainA, brainB });
        client.BrainInfoById[brainA] = new BrainInfo { PlasticityEnabled = true };
        client.BrainInfoById[brainB] = new BrainInfo { PlasticityEnabled = false };
        vm.SystemPlasticityRateTextDraft = "0.2";
        vm.SelectedSystemPlasticityModeDraft = vm.PlasticityModes.First(mode => !mode.Probabilistic);

        vm.ApplySystemPlasticityModeRateCommand.Execute(null);
        await WaitForAsync(() => client.SettingCalls.Count == 2 && client.PlasticityCalls.Count == 2);

        Assert.Contains(client.SettingCalls, call => call.Key == PlasticitySettingsKeys.SystemRateKey && call.Value == "0.2");
        Assert.Contains(client.SettingCalls, call => call.Key == PlasticitySettingsKeys.SystemProbabilisticUpdatesKey && call.Value == "false");
        Assert.Contains(client.PlasticityCalls, call => call.BrainId == brainA && call.Enabled && Math.Abs(call.Rate - 0.2f) < 0.000001f && !call.Probabilistic);
        Assert.Contains(client.PlasticityCalls, call => call.BrainId == brainB && !call.Enabled && Math.Abs(call.Rate - 0.2f) < 0.000001f && !call.Probabilistic);
        Assert.Equal("0.2", vm.SystemPlasticityRateText);
        Assert.Equal("0.2", vm.SystemPlasticityRateTextDraft);
        Assert.False(vm.SystemPlasticityProbabilisticUpdates);
        Assert.False(vm.SystemPlasticityProbabilisticUpdatesDraft);
    }

    [Fact]
    public async Task ApplyPlasticityModeRate_InvalidRate_Shows_Validation_Error()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.SystemPlasticityRateTextDraft = "not-a-number";

        vm.ApplySystemPlasticityModeRateCommand.Execute(null);
        await WaitForAsync(() => vm.BrainInfoSummary.Contains("rate invalid", StringComparison.OrdinalIgnoreCase));

        Assert.Contains("rate invalid", vm.BrainInfoSummary, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OverrideCheckboxes_ApplySelectedBrainFlagsWithoutButtons()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainId = Guid.NewGuid();
        vm.SelectBrain(brainId);
        vm.CostEnergyEnabled = true;
        vm.PlasticityEnabled = true;
        vm.PlasticityRateText = "0.001";
        vm.SelectedPlasticityMode = vm.PlasticityModes.First(mode => mode.Probabilistic);

        vm.CostEnergySuppressed = true;
        vm.PlasticitySuppressed = true;
        await WaitForAsync(() => client.CostEnergyCalls.Count == 1 && client.PlasticityCalls.Count == 1);

        Assert.Contains(client.CostEnergyCalls, call => call.BrainId == brainId && !call.CostEnabled && !call.EnergyEnabled);
        Assert.Contains(client.PlasticityCalls, call => call.BrainId == brainId && !call.Enabled && Math.Abs(call.Rate - 0.001f) < 0.000001f && call.Probabilistic);
    }

    [Fact]
    public async Task UpdateActiveBrains_Populates_KnownBrainDropdown()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();

        vm.UpdateActiveBrains(new[] { brainA, brainB });
        await WaitForAsync(() => vm.KnownBrains.Count == 2);

        Assert.Equal(2, vm.KnownBrains.Count);
        Assert.NotNull(vm.SelectedFeedbackBrain);
    }

    [Fact]
    public void ApplyBrainInfo_Uses_RuntimePlasticity_Mode_And_Rate()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        ApplyBrainInfo(vm, new BrainInfo
        {
            InputWidth = 4,
            OutputWidth = 1,
            PlasticityEnabled = true,
            PlasticityRate = 0.001f,
            PlasticityProbabilisticUpdates = true
        });

        Assert.True(vm.PlasticityEnabled);
        Assert.True(vm.SelectedPlasticityMode.Probabilistic);
        Assert.Equal("0.001", vm.PlasticityRateText);
    }

    [Fact]
    public void ApplyHomeostasis_InvalidProbability_Shows_Validation_Error()
    {
        var vm = CreateViewModel(new FakeWorkbenchClient());
        vm.UpdateActiveBrains(new[] { Guid.NewGuid() });
        vm.HomeostasisBaseProbabilityText = "1.1";

        vm.ApplyHomeostasisCommand.Execute(null);

        Assert.Equal("Homeostasis probability invalid.", vm.BrainInfoSummary);
    }

    [Fact]
    public async Task ApplyHomeostasis_Sends_Configured_Values()
    {
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(client);
        var brainA = Guid.NewGuid();
        var brainB = Guid.NewGuid();
        vm.UpdateActiveBrains(new[] { brainA, brainB });
        vm.HomeostasisEnabled = true;
        vm.HomeostasisBaseProbabilityText = "0.3";
        vm.HomeostasisMinStepCodesText = "2";
        vm.HomeostasisEnergyCouplingEnabled = true;
        vm.HomeostasisEnergyTargetScaleText = "0.5";
        vm.HomeostasisEnergyProbabilityScaleText = "1.5";

        vm.ApplyHomeostasisCommand.Execute(null);
        await WaitForAsync(() => client.HomeostasisCalls.Count == 2);

        Assert.Contains(client.HomeostasisCalls, call => call.BrainId == brainA
                                                         && call.Enabled
                                                         && call.TargetMode == HomeostasisTargetMode.HomeostasisTargetZero
                                                         && call.UpdateMode == HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
                                                         && Math.Abs(call.BaseProbability - 0.3f) < 0.000001f
                                                         && call.MinStepCodes == 2
                                                         && call.EnergyCouplingEnabled
                                                         && Math.Abs(call.EnergyTargetScale - 0.5f) < 0.000001f
                                                         && Math.Abs(call.EnergyProbabilityScale - 1.5f) < 0.000001f);
        Assert.Contains(client.HomeostasisCalls, call => call.BrainId == brainB);
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

    private static void ApplyBrainInfo(IoPanelViewModel vm, BrainInfo info)
    {
        var method = typeof(IoPanelViewModel).GetMethod("ApplyBrainInfo", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(method);
        method!.Invoke(vm, new object?[] { info });
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
        public List<(Guid BrainId, bool Enabled, float Rate, bool Probabilistic)> PlasticityCalls { get; } = new();
        public Dictionary<Guid, IoCommandResult> CostEnergyResults { get; } = new();
        public Dictionary<Guid, BrainInfo> BrainInfoById { get; } = new();
        public List<(Guid BrainId, float[] Values)> InputVectorCalls { get; } = new();
        public List<(Guid BrainId, bool Enabled, HomeostasisTargetMode TargetMode, HomeostasisUpdateMode UpdateMode, float BaseProbability, uint MinStepCodes, bool EnergyCouplingEnabled, float EnergyTargetScale, float EnergyProbabilityScale)> HomeostasisCalls { get; } = new();
        public List<(string Key, string Value)> SettingCalls { get; } = new();
        public bool ReturnNullOnSetSetting { get; set; }
        public string? SetSettingResponseKeyOverride { get; set; }
        public string? SetSettingResponseValueOverride { get; set; }

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
            PlasticityCalls.Add((brainId, enabled, rate, probabilistic));
            return Task.FromResult(new IoCommandResult(
                brainId,
                "set_plasticity",
                true,
                "applied",
                new BrainEnergyState
                {
                    PlasticityEnabled = enabled,
                    PlasticityRate = rate,
                    PlasticityProbabilisticUpdates = probabilistic,
                    HomeostasisEnabled = true,
                    HomeostasisTargetMode = HomeostasisTargetMode.HomeostasisTargetZero,
                    HomeostasisUpdateMode = HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                    HomeostasisBaseProbability = 0.01f,
                    HomeostasisMinStepCodes = 1,
                    HomeostasisEnergyCouplingEnabled = false,
                    HomeostasisEnergyTargetScale = 1f,
                    HomeostasisEnergyProbabilityScale = 1f
                }));
        }

        public override Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
        {
            if (BrainInfoById.TryGetValue(brainId, out var info))
            {
                return Task.FromResult<BrainInfo?>(info);
            }

            return Task.FromResult<BrainInfo?>(null);
        }

        public override Task<SettingValue?> SetSettingAsync(string key, string value)
        {
            SettingCalls.Add((key, value));
            if (ReturnNullOnSetSetting)
            {
                return Task.FromResult<SettingValue?>(null);
            }

            return Task.FromResult<SettingValue?>(new SettingValue
            {
                Key = SetSettingResponseKeyOverride ?? key,
                Value = SetSettingResponseValueOverride ?? value,
                UpdatedMs = 1UL
            });
        }

        public override Task<IoCommandResult> SetHomeostasisAsync(
            Guid brainId,
            bool enabled,
            HomeostasisTargetMode targetMode,
            HomeostasisUpdateMode updateMode,
            float baseProbability,
            uint minStepCodes,
            bool energyCouplingEnabled,
            float energyTargetScale,
            float energyProbabilityScale)
        {
            HomeostasisCalls.Add((brainId, enabled, targetMode, updateMode, baseProbability, minStepCodes, energyCouplingEnabled, energyTargetScale, energyProbabilityScale));
            return Task.FromResult(new IoCommandResult(
                brainId,
                "set_homeostasis",
                true,
                "applied",
                new BrainEnergyState
                {
                    HomeostasisEnabled = enabled,
                    HomeostasisTargetMode = targetMode,
                    HomeostasisUpdateMode = updateMode,
                    HomeostasisBaseProbability = baseProbability,
                    HomeostasisMinStepCodes = minStepCodes,
                    HomeostasisEnergyCouplingEnabled = energyCouplingEnabled,
                    HomeostasisEnergyTargetScale = energyTargetScale,
                    HomeostasisEnergyProbabilityScale = energyProbabilityScale
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
