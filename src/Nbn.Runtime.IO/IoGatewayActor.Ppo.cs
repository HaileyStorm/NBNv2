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
        var requestTask = RequestPpoAsync(
            context,
            request,
            wrapResponse,
            createFailureResponse,
            operationName);
        context.ReenterAfter(requestTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                context.Respond(completed.Result);
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

    private async Task<TResult> RequestPpoAsync<TRequest, TResponse, TResult>(
        IContext context,
        TRequest request,
        Func<TResponse, TResult> wrapResponse,
        Func<ProtoPpo.PpoFailureReason, string, TResponse> createFailureResponse,
        string operationName)
        where TRequest : class
        where TResponse : class
        where TResult : class
    {
        var ppoPid = _ppoPid;
        if (ppoPid is null)
        {
            await TryRefreshPpoEndpointAsync(context, operationName).ConfigureAwait(false);
            ppoPid = _ppoPid;
            if (ppoPid is null)
            {
                return wrapResponse(
                    createFailureResponse(
                        ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                        $"{operationName} failed: PPO manager endpoint is not configured."));
            }
        }

        try
        {
            var response = await context.RequestAsync<TResponse>(ppoPid, request, PpoRequestTimeout).ConfigureAwait(false);
            if (response is null)
            {
                return wrapResponse(
                    createFailureResponse(
                        ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                        $"{operationName} failed: PPO manager returned an empty response."));
            }

            return wrapResponse(response);
        }
        catch (Exception ex) when (IsStaleServiceEndpointFailure(ex))
        {
            var refreshed = await TryRefreshPpoEndpointAsync(context, operationName, ex).ConfigureAwait(false);
            var retryPid = _ppoPid;
            if (!refreshed || retryPid is null)
            {
                return wrapResponse(
                    createFailureResponse(
                        ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                        $"{operationName} failed: forwarding request to PPO manager failed ({ex.GetBaseException().Message})."));
            }

            Console.WriteLine(
                $"[WARN] IO retrying {operationName} after PPO endpoint refresh: previous={PidLabel(ppoPid)} current={PidLabel(retryPid)} failure={ex.GetBaseException().Message}");
            var response = await context.RequestAsync<TResponse>(retryPid, request, PpoRequestTimeout).ConfigureAwait(false);
            return response is null
                ? wrapResponse(
                    createFailureResponse(
                        ProtoPpo.PpoFailureReason.PpoFailureServiceUnavailable,
                        $"{operationName} failed: PPO manager returned an empty response."))
                : wrapResponse(response);
        }
    }
}
