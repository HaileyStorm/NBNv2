using Nbn.Proto.Ppo;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.Ppo;

/// <summary>
/// Owns PPO optimization lifecycle and dependency readiness for Reproduction and Speciation.
/// </summary>
public sealed partial class PpoManagerActor : IActor
{
    private readonly PID? _configuredReproductionPid;
    private readonly PID? _configuredSpeciationPid;
    private PID? _reproductionPid;
    private PID? _speciationPid;
    private PpoRunDescriptor? _activeRun;
    private ulong _completedRunCount;

    public PpoManagerActor(PID? reproductionPid = null, PID? speciationPid = null)
    {
        _configuredReproductionPid = reproductionPid;
        _configuredSpeciationPid = speciationPid;
        _reproductionPid = reproductionPid;
        _speciationPid = speciationPid;
    }

    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case PpoStatusRequest:
                context.Respond(CreateStatusResponse());
                break;
            case PpoStartRunRequest message:
                context.Respond(StartRun(message));
                break;
            case PpoStopRunRequest message:
                context.Respond(StopRun(message));
                break;
            case DiscoverySnapshotApplied snapshot:
                ApplyDiscoverySnapshot(snapshot);
                break;
            case EndpointStateObserved observed:
                ApplyObservedEndpoint(observed.Observation);
                break;
        }

        return Task.CompletedTask;
    }

    private PpoStatusResponse CreateStatusResponse()
        => new()
        {
            FailureReason = PpoFailureReason.PpoFailureNone,
            Dependencies = CreateDependencyStatus(),
            ActiveRun = _activeRun?.Clone(),
            CompletedRunCount = _completedRunCount
        };

    private PpoStartRunResponse StartRun(PpoStartRunRequest request)
    {
        if (_activeRun is not null && _activeRun.State == PpoRunState.Running)
        {
            return CreateStartFailure(PpoFailureReason.PpoFailureRunAlreadyActive, "ppo_run_already_active");
        }

        if (_reproductionPid is null)
        {
            return CreateStartFailure(PpoFailureReason.PpoFailureReproductionUnavailable, "ppo_reproduction_unavailable");
        }

        if (_speciationPid is null)
        {
            return CreateStartFailure(PpoFailureReason.PpoFailureSpeciationUnavailable, "ppo_speciation_unavailable");
        }

        if (!TryValidateHyperparameters(request.Hyperparameters, out var validationFailure))
        {
            return CreateStartFailure(PpoFailureReason.PpoFailureInvalidRequest, validationFailure);
        }

        var runId = string.IsNullOrWhiteSpace(request.RunId)
            ? Guid.NewGuid().ToString("N")
            : request.RunId.Trim();

        _activeRun = new PpoRunDescriptor
        {
            RunId = runId,
            State = PpoRunState.Running,
            StartedMs = CurrentUnixTimeMs(),
            Hyperparameters = request.Hyperparameters.Clone(),
            ObjectiveName = request.ObjectiveName.Trim(),
            MetadataJson = request.MetadataJson.Trim(),
            StatusDetail = "running"
        };

        return new PpoStartRunResponse
        {
            FailureReason = PpoFailureReason.PpoFailureNone,
            Accepted = true,
            Run = _activeRun.Clone()
        };
    }

    private PpoStopRunResponse StopRun(PpoStopRunRequest request)
    {
        if (_activeRun is null || _activeRun.State != PpoRunState.Running)
        {
            return CreateStopFailure(PpoFailureReason.PpoFailureRunNotActive, "ppo_run_not_active");
        }

        if (!string.IsNullOrWhiteSpace(request.RunId)
            && !string.Equals(_activeRun.RunId, request.RunId.Trim(), StringComparison.Ordinal))
        {
            return CreateStopFailure(PpoFailureReason.PpoFailureInvalidRequest, "ppo_run_id_mismatch");
        }

        var stopped = _activeRun.Clone();
        stopped.State = PpoRunState.Cancelled;
        stopped.CompletedMs = CurrentUnixTimeMs();
        stopped.StatusDetail = string.IsNullOrWhiteSpace(request.Reason)
            ? "stopped"
            : request.Reason.Trim();

        _completedRunCount++;
        _activeRun = null;

        return new PpoStopRunResponse
        {
            FailureReason = PpoFailureReason.PpoFailureNone,
            Stopped = true,
            Run = stopped
        };
    }

    private PpoDependencyStatus CreateDependencyStatus()
        => new()
        {
            ReproductionAvailable = _reproductionPid is not null,
            SpeciationAvailable = _speciationPid is not null,
            ReproductionEndpoint = PidLabel(_reproductionPid),
            SpeciationEndpoint = PidLabel(_speciationPid)
        };

    private static bool TryValidateHyperparameters(PpoHyperparameters? hyperparameters, out string failureReason)
    {
        failureReason = string.Empty;
        if (hyperparameters is null)
        {
            failureReason = "ppo_missing_hyperparameters";
            return false;
        }

        if (hyperparameters.RolloutTickCount == 0)
        {
            failureReason = "ppo_rollout_tick_count_invalid";
            return false;
        }

        if (hyperparameters.RolloutBatchCount == 0)
        {
            failureReason = "ppo_rollout_batch_count_invalid";
            return false;
        }

        if (hyperparameters.ClipEpsilon <= 0 || hyperparameters.ClipEpsilon > 1)
        {
            failureReason = "ppo_clip_epsilon_invalid";
            return false;
        }

        if (hyperparameters.DiscountGamma <= 0 || hyperparameters.DiscountGamma > 1)
        {
            failureReason = "ppo_discount_gamma_invalid";
            return false;
        }

        if (hyperparameters.GaeLambda <= 0 || hyperparameters.GaeLambda > 1)
        {
            failureReason = "ppo_gae_lambda_invalid";
            return false;
        }

        if (hyperparameters.LearningRate <= 0)
        {
            failureReason = "ppo_learning_rate_invalid";
            return false;
        }

        if (hyperparameters.OptimizationEpochCount == 0)
        {
            failureReason = "ppo_optimization_epoch_count_invalid";
            return false;
        }

        if (hyperparameters.MinibatchSize == 0)
        {
            failureReason = "ppo_minibatch_size_invalid";
            return false;
        }

        return true;
    }

    private static PpoStartRunResponse CreateStartFailure(PpoFailureReason reason, string detail)
        => new()
        {
            FailureReason = reason,
            FailureDetail = detail,
            Accepted = false
        };

    private static PpoStopRunResponse CreateStopFailure(PpoFailureReason reason, string detail)
        => new()
        {
            FailureReason = reason,
            FailureDetail = detail,
            Stopped = false
        };

    private static ulong CurrentUnixTimeMs()
        => (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static string PidLabel(PID? pid)
        => pid is null
            ? string.Empty
            : string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
}
