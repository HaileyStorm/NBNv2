using System.Reflection;
using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Proto;

namespace Nbn.Tests.Tools;

public class WorkbenchClientDirectRuntimeRewardControlTests
{
    [Fact]
    public async Task ApplyDirectRuntimeRewardControlAsync_Offline_Returns_Status_Response()
    {
        var brainId = Guid.NewGuid();
        var sink = new RecordingWorkbenchEventSink();
        var client = new WorkbenchClient(sink);

        var response = await client.ApplyDirectRuntimeRewardControlAsync(new DirectRuntimeRewardControlRequest
        {
            BrainId = brainId.ToProtoUuid(),
            ControllerId = "workbench",
            ActionId = "action-1",
            ObjectiveName = "stability",
            RewardSignal = "operator",
            ObservationTickId = 0,
            ActionTickId = 1,
            Surface = DirectRuntimeRewardControlSurface.PlasticityRate,
            Reward = 1f,
            ControlValue = 0.2f
        });

        Assert.False(response.Accepted);
        Assert.Equal("workbench_offline", response.FailureReasonCode);
        Assert.True(response.BrainId.TryToGuid(out var responseBrainId));
        Assert.Equal(brainId, responseBrainId);
        Assert.Equal("workbench", response.ControllerId);
        Assert.Equal("action-1", response.ActionId);
        Assert.Equal(DirectRuntimeRewardControlSurface.PlasticityRate, response.Surface);
        Assert.Equal(1UL, response.AppliedTickFloor);
        Assert.Equal(1f, response.Reward);
        Assert.Equal(0.2f, response.ControlValue);
        Assert.Equal("Direct runtime reward-control failed: IO gateway is not connected.", sink.IoStatus);
        Assert.False(sink.IoConnected);
    }

    [Fact]
    public async Task ApplyDirectRuntimeRewardControlAsync_Forwards_To_Io_And_Preserves_Response()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var forwarded = new TaskCompletionSource<DirectRuntimeRewardControlRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        var gateway = root.Spawn(Props.FromProducer(() => new DirectRuntimeRewardControlGatewayProbe(forwarded)));
        var client = new WorkbenchClient(new RecordingWorkbenchEventSink());
        SetPrivateField(client, "_system", system);
        SetPrivateField(client, "_root", root);
        SetPrivateField(client, "_ioGatewayPid", gateway);

        try
        {
            var response = await client.ApplyDirectRuntimeRewardControlAsync(new DirectRuntimeRewardControlRequest
            {
                BrainId = brainId.ToProtoUuid(),
                ControllerId = "workbench",
                ActionId = "action-1",
                ObjectiveName = "stability",
                RewardSignal = "operator",
                ObservationTickId = 0,
                ActionTickId = 1,
                Surface = DirectRuntimeRewardControlSurface.PlasticityRate,
                Reward = 1f,
                ControlValue = 0.2f
            });

            Assert.True(response.Accepted);
            Assert.Equal("action-1", response.ActionId);
            Assert.Equal(1UL, response.AppliedTickFloor);
            Assert.Equal(0.2f, response.ControlValue);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var request = await forwarded.Task.WaitAsync(cts.Token);
            Assert.True(request.BrainId.TryToGuid(out var forwardedBrainId));
            Assert.Equal(brainId, forwardedBrainId);
            Assert.Equal("workbench", request.ControllerId);
            Assert.Equal("action-1", request.ActionId);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ApplyDirectRuntimeRewardControlAsync_Empty_Io_Response_Reports_Status()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        var gateway = root.Spawn(Props.FromProducer(() => new EmptyDirectRuntimeRewardControlGatewayProbe()));
        var sink = new RecordingWorkbenchEventSink();
        var client = new WorkbenchClient(sink);
        SetPrivateField(client, "_system", system);
        SetPrivateField(client, "_root", root);
        SetPrivateField(client, "_ioGatewayPid", gateway);

        try
        {
            var response = await client.ApplyDirectRuntimeRewardControlAsync(new DirectRuntimeRewardControlRequest
            {
                BrainId = brainId.ToProtoUuid(),
                ControllerId = "workbench",
                ActionId = "action-1",
                ObjectiveName = "stability",
                RewardSignal = "operator",
                ObservationTickId = 0,
                ActionTickId = 1,
                Surface = DirectRuntimeRewardControlSurface.PlasticityRate,
                Reward = 1f,
                ControlValue = 0.2f
            });

            Assert.False(response.Accepted);
            Assert.Equal("empty_response", response.FailureReasonCode);
            Assert.Equal("Direct runtime reward-control failed: IO gateway returned an empty response.", sink.IoStatus);
            Assert.False(sink.IoConnected);
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

    private sealed class DirectRuntimeRewardControlGatewayProbe : IActor
    {
        private readonly TaskCompletionSource<DirectRuntimeRewardControlRequest> _request;

        public DirectRuntimeRewardControlGatewayProbe(TaskCompletionSource<DirectRuntimeRewardControlRequest> request)
        {
            _request = request;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ApplyDirectRuntimeRewardControl message)
            {
                var request = message.Request ?? new DirectRuntimeRewardControlRequest();
                _request.TrySetResult(request);
                context.Respond(new ApplyDirectRuntimeRewardControlResult
                {
                    Response = new DirectRuntimeRewardControlResponse
                    {
                        Accepted = true,
                        BrainId = request.BrainId?.Clone(),
                        ControllerId = request.ControllerId,
                        ActionId = request.ActionId,
                        Surface = request.Surface,
                        AppliedTickFloor = request.ActionTickId,
                        Reward = request.Reward,
                        ControlValue = request.ControlValue
                    }
                });
            }

            return Task.CompletedTask;
        }
    }

    private sealed class EmptyDirectRuntimeRewardControlGatewayProbe : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is ApplyDirectRuntimeRewardControl)
            {
                context.Respond(new ApplyDirectRuntimeRewardControlResult());
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RecordingWorkbenchEventSink : IWorkbenchEventSink
    {
        public string IoStatus { get; private set; } = string.Empty;
        public bool IoConnected { get; private set; }

        public void OnOutputEvent(OutputEventItem item)
        {
        }

        public void OnOutputVectorEvent(OutputVectorEventItem item)
        {
        }

        public void OnDebugEvent(DebugEventItem item)
        {
        }

        public void OnVizEvent(VizEventItem item)
        {
        }

        public void OnBrainTerminated(BrainTerminatedItem item)
        {
        }

        public void OnIoStatus(string status, bool connected)
        {
            IoStatus = status;
            IoConnected = connected;
        }

        public void OnObsStatus(string status, bool connected)
        {
        }

        public void OnSettingsStatus(string status, bool connected)
        {
        }

        public void OnHiveMindStatus(string status, bool connected)
        {
        }

        public void OnSettingChanged(SettingItem item)
        {
        }
    }
}
