using Nbn.Proto.Io;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private async Task HandleDirectRuntimeRewardControlAsync(IContext context, ApplyDirectRuntimeRewardControl message)
    {
        var request = message.Request ?? new ProtoControl.DirectRuntimeRewardControlRequest();
        if (_hiveMindPid is null)
        {
            context.Respond(new ApplyDirectRuntimeRewardControlResult
            {
                Response = CreateDirectRuntimeRewardControlFailure(
                    request,
                    "hivemind_unavailable",
                    "HiveMind is not configured for direct runtime reward-control.")
            });
            return;
        }

        try
        {
            var response = await context.RequestAsync<ProtoControl.DirectRuntimeRewardControlResponse>(
                    _hiveMindPid,
                    request,
                    DefaultRequestTimeout)
                .ConfigureAwait(false);
            context.Respond(new ApplyDirectRuntimeRewardControlResult
            {
                Response = response ?? CreateDirectRuntimeRewardControlFailure(
                    request,
                    "empty_response",
                    "HiveMind returned an empty direct runtime reward-control response.")
            });
        }
        catch (Exception ex)
        {
            context.Respond(new ApplyDirectRuntimeRewardControlResult
            {
                Response = CreateDirectRuntimeRewardControlFailure(
                    request,
                    "request_failed",
                    $"Direct runtime reward-control request failed: {ex.GetBaseException().Message}")
            });
        }
    }

    private static ProtoControl.DirectRuntimeRewardControlResponse CreateDirectRuntimeRewardControlFailure(
        ProtoControl.DirectRuntimeRewardControlRequest request,
        string reason,
        string message)
        => new()
        {
            Accepted = false,
            FailureReasonCode = reason,
            Message = message,
            BrainId = request.BrainId?.Clone() ?? new Nbn.Proto.Uuid(),
            ControllerId = request.ControllerId ?? string.Empty,
            ActionId = request.ActionId ?? string.Empty,
            Surface = request.Surface,
            AppliedTickFloor = request.ActionTickId,
            Reward = request.Reward,
            ControlValue = request.ControlValue
        };
}
