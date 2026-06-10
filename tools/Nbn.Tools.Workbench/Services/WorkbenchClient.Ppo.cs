using System;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Io;
using Nbn.Proto.Ppo;
using Proto;

namespace Nbn.Tools.Workbench.Services;

public partial class WorkbenchClient
{
    private static readonly TimeSpan PpoRequestTimeout = TimeSpan.FromSeconds(45);

    public virtual Task<PpoStatusResponse> GetPpoStatusAsync(CancellationToken cancellationToken = default)
    {
        return RequestPpoStatusCoreAsync(cancellationToken);
    }

    public virtual Task<PpoStartRunResponse> StartPpoRunAsync(
        PpoStartRunRequest request,
        CancellationToken cancellationToken = default)
    {
        return StartPpoRunCoreAsync(request ?? new PpoStartRunRequest(), cancellationToken);
    }

    public virtual Task<PpoStopRunResponse> StopPpoRunAsync(
        PpoStopRunRequest request,
        CancellationToken cancellationToken = default)
    {
        return StopPpoRunCoreAsync(request ?? new PpoStopRunRequest(), cancellationToken);
    }

    public virtual Task<PpoRecordRewardsResponse> RecordPpoRewardsAsync(
        PpoRecordRewardsRequest request,
        CancellationToken cancellationToken = default)
    {
        return RecordPpoRewardsCoreAsync(request ?? new PpoRecordRewardsRequest(), cancellationToken);
    }

    private async Task<PpoStatusResponse> RequestPpoStatusCoreAsync(CancellationToken cancellationToken)
    {
        if (TryCreatePpoUnavailable(cancellationToken, "PPO status failed", out var detail))
        {
            return new PpoStatusResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = detail,
                Dependencies = new PpoDependencyStatus()
            };
        }

        try
        {
            var result = await _root!.RequestAsync<PpoStatusResult>(
                    _ioGatewayPid!,
                    new PpoStatus { Request = new PpoStatusRequest() },
                    PpoRequestTimeout)
                .ConfigureAwait(false);
            return result?.Response ?? new PpoStatusResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = "PPO status failed: IO gateway returned an empty response.",
                Dependencies = new PpoDependencyStatus()
            };
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"PPO status failed: {ex.Message}", false);
            return new PpoStatusResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = $"PPO status failed: {ex.Message}",
                Dependencies = new PpoDependencyStatus()
            };
        }
    }

    private async Task<PpoStartRunResponse> StartPpoRunCoreAsync(PpoStartRunRequest request, CancellationToken cancellationToken)
    {
        if (TryCreatePpoUnavailable(cancellationToken, "PPO run submit failed", out var detail))
        {
            return new PpoStartRunResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = detail,
                Accepted = false,
                Run = new PpoRunDescriptor()
            };
        }

        try
        {
            var result = await _root!.RequestAsync<PpoStartRunResult>(
                    _ioGatewayPid!,
                    new PpoStartRun { Request = request },
                    PpoRequestTimeout)
                .ConfigureAwait(false);
            return result?.Response ?? new PpoStartRunResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = "PPO run submit failed: IO gateway returned an empty response.",
                Accepted = false,
                Run = new PpoRunDescriptor()
            };
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"PPO run submit failed: {ex.Message}", false);
            return new PpoStartRunResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = $"PPO run submit failed: {ex.Message}",
                Accepted = false,
                Run = new PpoRunDescriptor()
            };
        }
    }

    private async Task<PpoStopRunResponse> StopPpoRunCoreAsync(PpoStopRunRequest request, CancellationToken cancellationToken)
    {
        if (TryCreatePpoUnavailable(cancellationToken, "PPO cancel failed", out var detail))
        {
            return new PpoStopRunResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = detail,
                Stopped = false,
                Run = new PpoRunDescriptor()
            };
        }

        try
        {
            var result = await _root!.RequestAsync<PpoStopRunResult>(
                    _ioGatewayPid!,
                    new PpoStopRun { Request = request },
                    PpoRequestTimeout)
                .ConfigureAwait(false);
            return result?.Response ?? new PpoStopRunResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = "PPO cancel failed: IO gateway returned an empty response.",
                Stopped = false,
                Run = new PpoRunDescriptor()
            };
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"PPO cancel failed: {ex.Message}", false);
            return new PpoStopRunResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = $"PPO cancel failed: {ex.Message}",
                Stopped = false,
                Run = new PpoRunDescriptor()
            };
        }
    }

    private async Task<PpoRecordRewardsResponse> RecordPpoRewardsCoreAsync(
        PpoRecordRewardsRequest request,
        CancellationToken cancellationToken)
    {
        if (TryCreatePpoUnavailable(cancellationToken, "PPO reward record failed", out var detail))
        {
            return new PpoRecordRewardsResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = detail,
                Accepted = false,
                Update = new PpoPolicyUpdateReport()
            };
        }

        try
        {
            var result = await _root!.RequestAsync<PpoRecordRewardsResult>(
                    _ioGatewayPid!,
                    new PpoRecordRewards { Request = request },
                    PpoRequestTimeout)
                .ConfigureAwait(false);
            return result?.Response ?? new PpoRecordRewardsResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = "PPO reward record failed: IO gateway returned an empty response.",
                Accepted = false,
                Update = new PpoPolicyUpdateReport()
            };
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"PPO reward record failed: {ex.Message}", false);
            return new PpoRecordRewardsResponse
            {
                FailureReason = PpoFailureReason.PpoFailureServiceUnavailable,
                FailureDetail = $"PPO reward record failed: {ex.Message}",
                Accepted = false,
                Update = new PpoPolicyUpdateReport()
            };
        }
    }

    private bool TryCreatePpoUnavailable(CancellationToken cancellationToken, string failurePrefix, out string detail)
    {
        if (_root is null || _ioGatewayPid is null)
        {
            detail = $"{failurePrefix}: IO gateway is not connected.";
            return true;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            detail = $"{failurePrefix}: request canceled.";
            return true;
        }

        detail = string.Empty;
        return false;
    }
}
