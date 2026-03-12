using System.Diagnostics;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public sealed class ShellViewModelTests
{
    [Fact]
    public async Task MultiBrain_SelectionChange_UpdatesVizScope_And_InputRouting()
    {
        var client = new FakeWorkbenchClient();
        var brainA = new BrainListItem(Guid.NewGuid(), "A", true);
        var brainB = new BrainListItem(Guid.NewGuid(), "B", true);
        client.BrainInfoById[brainA.BrainId] = new BrainInfo { InputWidth = 1, OutputWidth = 1 };
        client.BrainInfoById[brainB.BrainId] = new BrainInfo { InputWidth = 1, OutputWidth = 1 };

        await using var shell = new ShellViewModel(client, autoConnect: false);
        shell.Viz.SetBrains(new[] { brainA, brainB });

        shell.Viz.SelectedBrain = brainA;
        shell.OnVizEvent(CreateVizEvent(
            brainA.BrainId,
            tickId: 1,
            eventId: "a1"));
        shell.OnVizEvent(CreateVizEvent(
            brainB.BrainId,
            tickId: 1,
            eventId: "b1"));

        await WaitForAsync(() =>
            shell.Viz.VizEvents.Any(item => item.EventId == "a1"));

        Assert.DoesNotContain(shell.Viz.VizEvents, item => item.EventId == "b1");

        shell.Io.InputVectorText = "1";
        shell.Io.SendVectorCommand.Execute(null);

        shell.Viz.SelectedBrain = brainB;
        await WaitForAsync(() => shell.Io.BrainIdText == brainB.BrainId.ToString("D"));

        shell.OnVizEvent(CreateVizEvent(
            brainA.BrainId,
            tickId: 2,
            eventId: "a2"));
        shell.OnVizEvent(CreateVizEvent(
            brainB.BrainId,
            tickId: 2,
            eventId: "b2"));

        await WaitForAsync(() =>
            shell.Viz.VizEvents.Any(item => item.EventId == "b2"));

        Assert.DoesNotContain(shell.Viz.VizEvents, item => item.EventId == "a2");
        Assert.Contains(shell.Viz.VizEvents, item => item.EventId == "b2");

        shell.Io.InputVectorText = "1";
        shell.Io.SendVectorCommand.Execute(null);

        shell.Io.AutoSendInputVectorEveryTick = true;
        shell.OnOutputEvent(CreateOutputEvent(brainB.BrainId, tickId: 10, outputIndex: 0, value: 0.25f));
        shell.OnOutputEvent(CreateOutputEvent(brainB.BrainId, tickId: 10, outputIndex: 1, value: 0.5f));
        shell.OnOutputEvent(CreateOutputEvent(brainB.BrainId, tickId: 11, outputIndex: 0, value: 0.75f));

        await WaitForAsync(() => client.InputVectorCalls.Count == 4);

        Assert.Equal(
            new[] { brainA.BrainId, brainB.BrainId, brainB.BrainId, brainB.BrainId },
            client.InputVectorCalls.Select(call => call.BrainId).ToArray());
    }

    private static VizEventItem CreateVizEvent(Guid brainId, ulong tickId, string eventId)
    {
        return new VizEventItem(
            DateTimeOffset.UtcNow,
            Nbn.Proto.Viz.VizEventType.VizNeuronFired.ToString(),
            brainId.ToString("D"),
            tickId,
            Region: "0",
            Source: "1",
            Target: string.Empty,
            Value: 0.5f,
            Strength: 0f,
            EventId: eventId);
    }

    private static OutputEventItem CreateOutputEvent(Guid brainId, ulong tickId, uint outputIndex, float value)
    {
        var now = DateTimeOffset.UtcNow;
        return new OutputEventItem(
            now,
            now.ToString("g"),
            brainId.ToString("D"),
            outputIndex,
            value,
            tickId);
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
        public Dictionary<Guid, BrainInfo> BrainInfoById { get; } = new();
        public List<(Guid BrainId, float[] Values)> InputVectorCalls { get; } = new();

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
        {
            if (BrainInfoById.TryGetValue(brainId, out var info))
            {
                return Task.FromResult<BrainInfo?>(info);
            }

            return Task.FromResult<BrainInfo?>(null);
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
