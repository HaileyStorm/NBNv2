using Nbn.Proto.Io;
using Proto;
using ProtoPpo = Nbn.Proto.Ppo;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private void HandlePpoStatus(IContext context, PpoStatus message)
    {
        var request = message.Request ?? new ProtoPpo.PpoStatusRequest();
        ForwardPpoRequest(
            context,
            request,
            static response => new PpoStatusResult { Response = response },
            static (reason, detail) => new ProtoPpo.PpoStatusResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Dependencies = new ProtoPpo.PpoDependencyStatus()
            },
            operationName: nameof(PpoStatus));
    }

    private void HandlePpoStartRun(IContext context, PpoStartRun message)
    {
        var request = message.Request ?? new ProtoPpo.PpoStartRunRequest();
        ForwardPpoRequest(
            context,
            request,
            static response => new PpoStartRunResult { Response = response },
            static (reason, detail) => new ProtoPpo.PpoStartRunResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Accepted = false,
                Run = new ProtoPpo.PpoRunDescriptor()
            },
            operationName: nameof(PpoStartRun));
    }

    private void HandlePpoStopRun(IContext context, PpoStopRun message)
    {
        var request = message.Request ?? new ProtoPpo.PpoStopRunRequest();
        ForwardPpoRequest(
            context,
            request,
            static response => new PpoStopRunResult { Response = response },
            static (reason, detail) => new ProtoPpo.PpoStopRunResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Stopped = false,
                Run = new ProtoPpo.PpoRunDescriptor()
            },
            operationName: nameof(PpoStopRun));
    }

    private void ForwardPpoRequest<TRequest, TResponse, TResult>(
        IContext context,
        TRequest request,
        Func<TResponse, TResult> wrapResponse,
        Func<ProtoPpo.PpoFailureReason, string, TResponse> createFailureResponse,
        string operationName)
        where TRequest : class
        where TResponse : class
        where TResult : class
    {
        if (_ppoPid is null)
        {
            context.Respond(
                wrapResponse(
                    createFailureResponse(
                        ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                        $"{operationName} failed: PPO manager endpoint is not configured.")));
            return;
        }

        var requestTask = context.RequestAsync<TResponse>(_ppoPid, request, PpoRequestTimeout);
        context.ReenterAfter(requestTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var response = completed.Result;
                if (response is null)
                {
                    context.Respond(
                        wrapResponse(
                            createFailureResponse(
                                ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                                $"{operationName} failed: PPO manager returned an empty response.")));
                    return Task.CompletedTask;
                }

                context.Respond(wrapResponse(response));
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"{operationName} failed: {detail}");
            context.Respond(
                wrapResponse(
                    createFailureResponse(
                        ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                        $"{operationName} failed: forwarding request to PPO manager failed ({detail}).")));
            return Task.CompletedTask;
        });
    }
}
