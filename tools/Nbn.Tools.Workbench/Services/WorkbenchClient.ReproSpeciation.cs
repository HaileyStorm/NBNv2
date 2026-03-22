using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Speciation;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Proto.Control;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

namespace Nbn.Tools.Workbench.Services;

public partial class WorkbenchClient
{
    public virtual async Task<Nbn.Proto.Repro.ReproduceResult?> ReproduceByBrainIdsAsync(ReproduceByBrainIdsRequest request)
    {
        if (_root is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        if (_ioGatewayPid is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        try
        {
            var result = await _root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
                    _ioGatewayPid,
                    new ReproduceByBrainIds { Request = request },
                    ReproRequestTimeout)
                .ConfigureAwait(false);
            return result?.Result ?? BuildReproFailureResult("repro_empty_response");
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"Repro failed: {ex.Message}", false);
            return BuildReproFailureResult("repro_request_failed");
        }
    }

    public virtual async Task<Nbn.Proto.Repro.ReproduceResult?> ReproduceByArtifactsAsync(ReproduceByArtifactsRequest request)
    {
        if (_root is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        if (_ioGatewayPid is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        try
        {
            var result = await _root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
                    _ioGatewayPid,
                    new ReproduceByArtifacts { Request = request },
                    ReproRequestTimeout)
                .ConfigureAwait(false);
            return result?.Result ?? BuildReproFailureResult("repro_empty_response");
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"Repro failed: {ex.Message}", false);
            return BuildReproFailureResult("repro_request_failed");
        }
    }

    public virtual Task<SpeciationStatusResponse> GetSpeciationStatusAsync(CancellationToken cancellationToken = default)
    {
        return RequestSpeciationAsync<SpeciationStatusResult, SpeciationStatusResponse>(
            new SpeciationStatus { Request = new SpeciationStatusRequest() },
            static result => result?.Response,
            static (reason, detail) => new SpeciationStatusResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Status = new SpeciationStatusSnapshot(),
                CurrentEpoch = new SpeciationEpochInfo(),
                Config = CreateDefaultSpeciationConfig()
            },
            "Speciation status failed",
            cancellationToken);
    }

    public virtual Task<SpeciationGetConfigResponse> GetSpeciationConfigAsync(CancellationToken cancellationToken = default)
    {
        return RequestSpeciationAsync<SpeciationGetConfigResult, SpeciationGetConfigResponse>(
            new SpeciationGetConfig { Request = new SpeciationGetConfigRequest() },
            static result => result?.Response,
            static (reason, detail) => new SpeciationGetConfigResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Config = CreateDefaultSpeciationConfig(),
                CurrentEpoch = new SpeciationEpochInfo()
            },
            "Speciation get-config failed",
            cancellationToken);
    }

    public virtual Task<SpeciationSetConfigResponse> SetSpeciationConfigAsync(
        SpeciationRuntimeConfig config,
        bool startNewEpoch,
        long? applyTimeMs = null,
        CancellationToken cancellationToken = default)
    {
        var request = new SpeciationSetConfigRequest
        {
            Config = config ?? CreateDefaultSpeciationConfig(),
            StartNewEpoch = startNewEpoch
        };

        if (applyTimeMs.HasValue && applyTimeMs.Value > 0)
        {
            request.HasApplyTimeMs = true;
            request.ApplyTimeMs = (ulong)applyTimeMs.Value;
        }

        return RequestSpeciationAsync<SpeciationSetConfigResult, SpeciationSetConfigResponse>(
            new SpeciationSetConfig { Request = request },
            static result => result?.Response,
            static (reason, detail) => new SpeciationSetConfigResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Config = CreateDefaultSpeciationConfig(),
                PreviousEpoch = new SpeciationEpochInfo(),
                CurrentEpoch = new SpeciationEpochInfo()
            },
            "Speciation set-config failed",
            cancellationToken);
    }

    public virtual Task<SpeciationResetAllResponse> ResetSpeciationHistoryAsync(
        long? applyTimeMs = null,
        CancellationToken cancellationToken = default)
    {
        var request = new SpeciationResetAllRequest();
        if (applyTimeMs.HasValue && applyTimeMs.Value > 0)
        {
            request.HasApplyTimeMs = true;
            request.ApplyTimeMs = (ulong)applyTimeMs.Value;
        }

        return RequestSpeciationAsync<SpeciationResetAllResult, SpeciationResetAllResponse>(
            new SpeciationResetAll { Request = request },
            static result => result?.Response,
            static (reason, detail) => new SpeciationResetAllResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                PreviousEpoch = new SpeciationEpochInfo(),
                CurrentEpoch = new SpeciationEpochInfo(),
                Config = CreateDefaultSpeciationConfig(),
                DeletedEpochCount = 0,
                DeletedMembershipCount = 0,
                DeletedSpeciesCount = 0,
                DeletedDecisionCount = 0,
                DeletedLineageEdgeCount = 0
            },
            "Speciation reset-all failed",
            cancellationToken);
    }

    public virtual Task<SpeciationDeleteEpochResponse> DeleteSpeciationEpochAsync(
        long epochId,
        CancellationToken cancellationToken = default)
    {
        var request = new SpeciationDeleteEpochRequest
        {
            EpochId = epochId > 0 ? (ulong)epochId : 0UL
        };

        return RequestSpeciationAsync<SpeciationDeleteEpochResult, SpeciationDeleteEpochResponse>(
            new SpeciationDeleteEpoch { Request = request },
            static result => result?.Response,
            static (reason, detail) => new SpeciationDeleteEpochResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                EpochId = 0,
                Deleted = false,
                DeletedMembershipCount = 0,
                DeletedSpeciesCount = 0,
                DeletedDecisionCount = 0,
                DeletedLineageEdgeCount = 0,
                CurrentEpoch = new SpeciationEpochInfo()
            },
            "Speciation delete-epoch failed",
            cancellationToken);
    }

    public virtual Task<SpeciationListMembershipsResponse> ListSpeciationMembershipsAsync(
        long? epochId = null,
        CancellationToken cancellationToken = default)
    {
        var request = new SpeciationListMembershipsRequest();
        if (epochId.HasValue && epochId.Value > 0)
        {
            request.HasEpochId = true;
            request.EpochId = (ulong)epochId.Value;
        }

        return RequestSpeciationAsync<SpeciationListMembershipsResult, SpeciationListMembershipsResponse>(
            new SpeciationListMemberships { Request = request },
            static result => result?.Response,
            static (reason, detail) => new SpeciationListMembershipsResponse
            {
                FailureReason = reason,
                FailureDetail = detail
            },
            "Speciation list-memberships failed",
            cancellationToken);
    }

    public virtual Task<SpeciationListHistoryResponse> ListSpeciationHistoryAsync(
        long? epochId = null,
        Guid? brainId = null,
        uint limit = 256,
        uint offset = 0,
        CancellationToken cancellationToken = default)
    {
        var request = new SpeciationListHistoryRequest
        {
            Limit = Math.Max(1u, limit),
            Offset = offset
        };
        if (epochId.HasValue && epochId.Value > 0)
        {
            request.HasEpochId = true;
            request.EpochId = (ulong)epochId.Value;
        }

        if (brainId.HasValue && brainId.Value != Guid.Empty)
        {
            request.HasBrainId = true;
            request.BrainId = brainId.Value.ToProtoUuid();
        }

        return RequestSpeciationAsync<SpeciationListHistoryResult, SpeciationListHistoryResponse>(
            new SpeciationListHistory { Request = request },
            static result => result?.Response,
            static (reason, detail) => new SpeciationListHistoryResponse
            {
                FailureReason = reason,
                FailureDetail = detail
            },
            "Speciation list-history failed",
            cancellationToken);
    }

    private static Nbn.Proto.Repro.ReproduceResult BuildReproFailureResult(string reasonCode)
    {
        return new Nbn.Proto.Repro.ReproduceResult
        {
            Report = new SimilarityReport
            {
                Compatible = false,
                AbortReason = string.IsNullOrWhiteSpace(reasonCode) ? "repro_request_failed" : reasonCode.Trim(),
                SimilarityScore = 0f,
                RegionSpanScore = 0f,
                FunctionScore = 0f,
                ConnectivityScore = 0f
            },
            Summary = new MutationSummary(),
            Spawned = false
        };
    }

    private async Task<TResponse> RequestSpeciationAsync<TResult, TResponse>(
        object requestMessage,
        Func<TResult?, TResponse?> resolveResponse,
        Func<SpeciationFailureReason, string, TResponse> createFailureResponse,
        string failurePrefix,
        CancellationToken cancellationToken)
        where TResult : class
        where TResponse : class
    {
        if (_root is null || _ioGatewayPid is null)
        {
            return createFailureResponse(
                SpeciationFailureReason.SpeciationFailureServiceUnavailable,
                "IO gateway is not connected.");
        }

        try
        {
            var requestTask = _root.RequestAsync<TResult>(_ioGatewayPid, requestMessage, SpeciationRequestTimeout);
            var result = cancellationToken.CanBeCanceled
                ? await requestTask.WaitAsync(cancellationToken).ConfigureAwait(false)
                : await requestTask.ConfigureAwait(false);
            var response = resolveResponse(result);
            if (response is not null)
            {
                return response;
            }

            return createFailureResponse(
                SpeciationFailureReason.SpeciationFailureEmptyResponse,
                "Speciation returned an empty response.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return createFailureResponse(
                SpeciationFailureReason.SpeciationFailureRequestFailed,
                "Speciation request canceled.");
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"{failurePrefix}: {ex.Message}", false);
            return createFailureResponse(
                SpeciationFailureReason.SpeciationFailureRequestFailed,
                ex.GetBaseException().Message);
        }
    }

    private static SpeciationRuntimeConfig CreateDefaultSpeciationConfig()
    {
        return new SpeciationRuntimeConfig
        {
            PolicyVersion = "default",
            ConfigSnapshotJson = "{}",
            DefaultSpeciesId = "species.default",
            DefaultSpeciesDisplayName = "Default species",
            StartupReconcileDecisionReason = "startup_reconcile"
        };
    }
}
