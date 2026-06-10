using System.Reflection;
using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Ppo;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Proto;

namespace Nbn.Tests.Tools;

public class WorkbenchClientPpoTests
{
    [Fact]
    public async Task GetPpoStatusAsync_Offline_Returns_ServiceUnavailable()
    {
        var sink = new RecordingWorkbenchEventSink();
        var client = new WorkbenchClient(sink);

        var response = await client.GetPpoStatusAsync();

        Assert.Equal(PpoFailureReason.PpoFailureServiceUnavailable, response.FailureReason);
        Assert.Contains("IO gateway is not connected", response.FailureDetail, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PpoMethods_Forward_To_Io_And_Preserve_Responses()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var probe = new PpoGatewayProbe();
        var gateway = root.Spawn(Props.FromProducer(() => probe));
        var client = new WorkbenchClient(new RecordingWorkbenchEventSink());
        SetPrivateField(client, "_system", system);
        SetPrivateField(client, "_root", root);
        SetPrivateField(client, "_ioGatewayPid", gateway);

        try
        {
            var status = await client.GetPpoStatusAsync();
            Assert.Equal(PpoFailureReason.PpoFailureNone, status.FailureReason);
            Assert.True(status.Dependencies.IoAvailable);
            await probe.StatusRequest.Task.WaitAsync(TimeSpan.FromSeconds(2));

            var runId = "ppo-test-run";
            var start = await client.StartPpoRunAsync(new PpoStartRunRequest
            {
                RunId = runId,
                ObjectiveName = "fitness",
                Hyperparameters = new PpoHyperparameters { RolloutTickCount = 4, RolloutBatchCount = 2 }
            });
            Assert.True(start.Accepted);
            Assert.Equal(runId, start.Run.RunId);
            Assert.Equal(PpoRunState.Running, start.Run.State);
            Assert.Equal(runId, (await probe.StartRequest.Task.WaitAsync(TimeSpan.FromSeconds(2))).RunId);

            var stop = await client.StopPpoRunAsync(new PpoStopRunRequest
            {
                RunId = runId,
                Reason = "test"
            });
            Assert.True(stop.Stopped);
            Assert.Equal(PpoRunState.Cancelled, stop.Run.State);
            Assert.Equal("test", (await probe.StopRequest.Task.WaitAsync(TimeSpan.FromSeconds(2))).Reason);

            var reward = await client.RecordPpoRewardsAsync(new PpoRecordRewardsRequest
            {
                ObjectiveName = "fitness",
                RewardSignal = "output.reward"
            });
            Assert.True(reward.Accepted);
            Assert.Equal(7UL, reward.Update.UpdateIndex);
            Assert.Equal("output.reward", (await probe.RecordRequest.Task.WaitAsync(TimeSpan.FromSeconds(2))).RewardSignal);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    private static void SetPrivateField(WorkbenchClient client, string name, object value)
    {
        var field = typeof(WorkbenchClient).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        field.SetValue(client, value);
    }

    private sealed class PpoGatewayProbe : IActor
    {
        public TaskCompletionSource<PpoStatusRequest> StatusRequest { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<PpoStartRunRequest> StartRequest { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<PpoStopRunRequest> StopRequest { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<PpoRecordRewardsRequest> RecordRequest { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case PpoStatus message:
                    StatusRequest.TrySetResult(message.Request ?? new PpoStatusRequest());
                    context.Respond(new PpoStatusResult
                    {
                        Response = new PpoStatusResponse
                        {
                            FailureReason = PpoFailureReason.PpoFailureNone,
                            Dependencies = new PpoDependencyStatus
                            {
                                IoAvailable = true,
                                ReproductionAvailable = true,
                                SpeciationAvailable = true
                            }
                        }
                    });
                    break;
                case PpoStartRun message:
                    var startRequest = message.Request ?? new PpoStartRunRequest();
                    StartRequest.TrySetResult(startRequest);
                    context.Respond(new PpoStartRunResult
                    {
                        Response = new PpoStartRunResponse
                        {
                            FailureReason = PpoFailureReason.PpoFailureNone,
                            Accepted = true,
                            Run = new PpoRunDescriptor
                            {
                                RunId = startRequest.RunId,
                                State = PpoRunState.Running,
                                ObjectiveName = startRequest.ObjectiveName
                            }
                        }
                    });
                    break;
                case PpoStopRun message:
                    var stopRequest = message.Request ?? new PpoStopRunRequest();
                    StopRequest.TrySetResult(stopRequest);
                    context.Respond(new PpoStopRunResult
                    {
                        Response = new PpoStopRunResponse
                        {
                            FailureReason = PpoFailureReason.PpoFailureNone,
                            Stopped = true,
                            Run = new PpoRunDescriptor
                            {
                                RunId = stopRequest.RunId,
                                State = PpoRunState.Cancelled,
                                StatusDetail = stopRequest.Reason
                            }
                        }
                    });
                    break;
                case PpoRecordRewards message:
                    var rewardRequest = message.Request ?? new PpoRecordRewardsRequest();
                    RecordRequest.TrySetResult(rewardRequest);
                    context.Respond(new PpoRecordRewardsResult
                    {
                        Response = new PpoRecordRewardsResponse
                        {
                            FailureReason = PpoFailureReason.PpoFailureNone,
                            Accepted = true,
                            Update = new PpoPolicyUpdateReport
                            {
                                UpdateIndex = 7,
                                AcceptedSampleCount = 1,
                                MeanReward = 0.75f,
                                MaxReward = 0.75f
                            }
                        }
                    });
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RecordingWorkbenchEventSink : IWorkbenchEventSink
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
