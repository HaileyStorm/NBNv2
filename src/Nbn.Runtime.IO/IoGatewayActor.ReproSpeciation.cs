using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Proto;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private void HandleReproduceByBrainIds(IContext context, ReproduceByBrainIds message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateReproFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateReproFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"ReproduceByBrainIds failed: {detail}");
            context.Respond(CreateReproFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleReproduceByArtifacts(IContext context, ReproduceByArtifacts message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateReproFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateReproFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new Nbn.Proto.Io.ReproduceResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"ReproduceByArtifacts failed: {detail}");
            context.Respond(CreateReproFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleAssessCompatibilityByBrainIds(IContext context, AssessCompatibilityByBrainIds message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateAssessmentFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateAssessmentFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new AssessCompatibilityResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"AssessCompatibilityByBrainIds failed: {detail}");
            context.Respond(CreateAssessmentFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleAssessCompatibilityByArtifacts(IContext context, AssessCompatibilityByArtifacts message)
    {
        if (_reproPid is null)
        {
            context.Respond(CreateAssessmentFailure("repro_unavailable"));
            return;
        }

        var reproTask = context.RequestAsync<Nbn.Proto.Repro.ReproduceResult>(_reproPid, message.Request, ReproRequestTimeout);
        context.ReenterAfter(reproTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                var result = completed.Result;
                if (result is null)
                {
                    context.Respond(CreateAssessmentFailure("repro_empty_response"));
                    return Task.CompletedTask;
                }

                result.Report ??= CreateAbortReport("repro_missing_report");
                EnsureSimilarityScore(result);
                context.Respond(new AssessCompatibilityResult { Result = result });
                return Task.CompletedTask;
            }

            var detail = completed.Exception?.GetBaseException().Message ?? "request canceled";
            Console.WriteLine($"AssessCompatibilityByArtifacts failed: {detail}");
            context.Respond(CreateAssessmentFailure("repro_request_failed"));
            return Task.CompletedTask;
        });
    }

    private void HandleSpeciationStatus(IContext context, SpeciationStatus message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationStatusRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationStatusResult { Response = response },
            static (reason, detail) => CreateSpeciationStatusFailure(reason, detail),
            operationName: nameof(SpeciationStatus));
    }

    private void HandleSpeciationGetConfig(IContext context, SpeciationGetConfig message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationGetConfigRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationGetConfigResult { Response = response },
            static (reason, detail) => CreateSpeciationGetConfigFailure(reason, detail),
            operationName: nameof(SpeciationGetConfig));
    }

    private void HandleSpeciationSetConfig(IContext context, SpeciationSetConfig message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationSetConfigRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationSetConfigResult { Response = response },
            static (reason, detail) => CreateSpeciationSetConfigFailure(reason, detail),
            operationName: nameof(SpeciationSetConfig));
    }

    private void HandleSpeciationResetAll(IContext context, SpeciationResetAll message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationResetAllRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationResetAllResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationResetAllResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                PreviousEpoch = new ProtoSpec.SpeciationEpochInfo(),
                CurrentEpoch = new ProtoSpec.SpeciationEpochInfo(),
                Config = CreateDefaultSpeciationConfig(),
                DeletedEpochCount = 0,
                DeletedMembershipCount = 0,
                DeletedSpeciesCount = 0,
                DeletedDecisionCount = 0,
                DeletedLineageEdgeCount = 0
            },
            operationName: nameof(SpeciationResetAll));
    }

    private void HandleSpeciationDeleteEpoch(IContext context, SpeciationDeleteEpoch message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationDeleteEpochRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationDeleteEpochResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationDeleteEpochResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                EpochId = 0,
                Deleted = false,
                DeletedMembershipCount = 0,
                DeletedSpeciesCount = 0,
                DeletedDecisionCount = 0,
                DeletedLineageEdgeCount = 0,
                CurrentEpoch = new ProtoSpec.SpeciationEpochInfo()
            },
            operationName: nameof(SpeciationDeleteEpoch));
    }

    private void HandleSpeciationEvaluate(IContext context, SpeciationEvaluate message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationEvaluateRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationEvaluateResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationEvaluateResponse
            {
                Decision = CreateSpeciationDecisionFailure(
                    ProtoSpec.SpeciationApplyMode.DryRun,
                    reason,
                    detail)
            },
            operationName: nameof(SpeciationEvaluate));
    }

    private void HandleSpeciationAssign(IContext context, SpeciationAssign message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationAssignRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationAssignResult { Response = response },
            (reason, detail) => new ProtoSpec.SpeciationAssignResponse
            {
                Decision = CreateSpeciationDecisionFailure(
                    NormalizeSpeciationApplyMode(request.ApplyMode),
                    reason,
                    detail)
            },
            operationName: nameof(SpeciationAssign));
    }

    private void HandleSpeciationBatchEvaluateApply(IContext context, SpeciationBatchEvaluateApply message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationBatchEvaluateApplyRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationBatchEvaluateApplyResult { Response = response },
            (reason, detail) => new ProtoSpec.SpeciationBatchEvaluateApplyResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                ApplyMode = NormalizeSpeciationApplyMode(request.ApplyMode),
                RequestedCount = (uint)request.Items.Count,
                ProcessedCount = 0,
                CommittedCount = 0
            },
            operationName: nameof(SpeciationBatchEvaluateApply));
    }

    private void HandleSpeciationListMemberships(IContext context, SpeciationListMemberships message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationListMembershipsRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationListMembershipsResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationListMembershipsResponse
            {
                FailureReason = reason,
                FailureDetail = detail
            },
            operationName: nameof(SpeciationListMemberships));
    }

    private void HandleSpeciationQueryMembership(IContext context, SpeciationQueryMembership message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationQueryMembershipRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationQueryMembershipResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationQueryMembershipResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                Found = false
            },
            operationName: nameof(SpeciationQueryMembership));
    }

    private void HandleSpeciationListHistory(IContext context, SpeciationListHistory message)
    {
        var request = message.Request ?? new ProtoSpec.SpeciationListHistoryRequest();
        ForwardSpeciationRequest(
            context,
            request,
            static response => new SpeciationListHistoryResult { Response = response },
            static (reason, detail) => new ProtoSpec.SpeciationListHistoryResponse
            {
                FailureReason = reason,
                FailureDetail = detail,
                TotalRecords = 0
            },
            operationName: nameof(SpeciationListHistory));
    }

    private void ForwardSpeciationRequest<TRequest, TResponse, TResult>(
        IContext context,
        TRequest request,
        Func<TResponse, TResult> wrapResponse,
        Func<ProtoSpec.SpeciationFailureReason, string, TResponse> createFailureResponse,
        string operationName)
        where TRequest : class
        where TResponse : class
        where TResult : class
    {
        if (_speciationPid is null)
        {
            context.Respond(
                wrapResponse(
                    createFailureResponse(
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureServiceUnavailable,
                        $"{operationName} failed: speciation manager endpoint is not configured.")));
            return;
        }

        var requestTask = context.RequestAsync<TResponse>(_speciationPid, request, SpeciationRequestTimeout);
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
                                ProtoSpec.SpeciationFailureReason.SpeciationFailureEmptyResponse,
                                $"{operationName} failed: speciation manager returned an empty response.")));
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
                        ProtoSpec.SpeciationFailureReason.SpeciationFailureRequestFailed,
                        $"{operationName} failed: forwarding request to speciation manager failed ({detail}).")));
            return Task.CompletedTask;
        });
    }

    private static ProtoSpec.SpeciationStatusResponse CreateSpeciationStatusFailure(
        ProtoSpec.SpeciationFailureReason reason,
        string detail)
    {
        return new ProtoSpec.SpeciationStatusResponse
        {
            FailureReason = reason,
            FailureDetail = detail,
            Status = new ProtoSpec.SpeciationStatusSnapshot(),
            CurrentEpoch = new ProtoSpec.SpeciationEpochInfo(),
            Config = CreateDefaultSpeciationConfig()
        };
    }

    private static ProtoSpec.SpeciationGetConfigResponse CreateSpeciationGetConfigFailure(
        ProtoSpec.SpeciationFailureReason reason,
        string detail)
    {
        return new ProtoSpec.SpeciationGetConfigResponse
        {
            FailureReason = reason,
            FailureDetail = detail,
            Config = CreateDefaultSpeciationConfig(),
            CurrentEpoch = new ProtoSpec.SpeciationEpochInfo()
        };
    }

    private static ProtoSpec.SpeciationSetConfigResponse CreateSpeciationSetConfigFailure(
        ProtoSpec.SpeciationFailureReason reason,
        string detail)
    {
        return new ProtoSpec.SpeciationSetConfigResponse
        {
            FailureReason = reason,
            FailureDetail = detail,
            Config = CreateDefaultSpeciationConfig(),
            PreviousEpoch = new ProtoSpec.SpeciationEpochInfo(),
            CurrentEpoch = new ProtoSpec.SpeciationEpochInfo()
        };
    }

    private static ProtoSpec.SpeciationRuntimeConfig CreateDefaultSpeciationConfig()
    {
        return new ProtoSpec.SpeciationRuntimeConfig
        {
            PolicyVersion = "unknown",
            ConfigSnapshotJson = "{}",
            DefaultSpeciesId = "default",
            DefaultSpeciesDisplayName = "Default",
            StartupReconcileDecisionReason = "startup_reconcile"
        };
    }

    private static ProtoSpec.SpeciationDecision CreateSpeciationDecisionFailure(
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail)
    {
        return new ProtoSpec.SpeciationDecision
        {
            ApplyMode = applyMode,
            CandidateMode = ProtoSpec.SpeciationCandidateMode.Unknown,
            Success = false,
            Created = false,
            ImmutableConflict = false,
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            SpeciesId = string.Empty,
            SpeciesDisplayName = string.Empty,
            DecisionReason = string.Empty,
            DecisionMetadataJson = "{}",
            Committed = false
        };
    }

    private static ProtoSpec.SpeciationApplyMode NormalizeSpeciationApplyMode(ProtoSpec.SpeciationApplyMode applyMode)
    {
        return applyMode == ProtoSpec.SpeciationApplyMode.Commit
            ? ProtoSpec.SpeciationApplyMode.Commit
            : ProtoSpec.SpeciationApplyMode.DryRun;
    }

    private static Nbn.Proto.Io.ReproduceResult CreateReproFailure(string reason)
        => new()
        {
            Result = new Nbn.Proto.Repro.ReproduceResult
            {
                Report = CreateAbortReport(reason),
                Summary = new MutationSummary(),
                Spawned = false
            }
        };

    private static AssessCompatibilityResult CreateAssessmentFailure(string reason)
        => new()
        {
            Result = new Nbn.Proto.Repro.ReproduceResult
            {
                Report = CreateAbortReport(reason),
                Summary = new MutationSummary(),
                Spawned = false
            }
        };

    private static SimilarityReport CreateAbortReport(string reason)
        => new()
        {
            Compatible = false,
            AbortReason = reason,
            SimilarityScore = 0f,
            RegionSpanScore = 0f,
            FunctionScore = 0f,
            ConnectivityScore = 0f
        };

    private static void EnsureSimilarityScore(Nbn.Proto.Repro.ReproduceResult? result)
    {
        var report = result?.Report;
        if (report is null || report.SimilarityScore > 0f)
        {
            return;
        }

        var total = 0f;
        var count = 0;

        if (report.RegionSpanScore > 0f)
        {
            total += report.RegionSpanScore;
            count++;
        }

        if (report.FunctionScore > 0f)
        {
            total += report.FunctionScore;
            count++;
        }

        if (report.ConnectivityScore > 0f)
        {
            total += report.ConnectivityScore;
            count++;
        }

        if (count > 0)
        {
            report.SimilarityScore = Clamp01(total / count);
            return;
        }

        report.SimilarityScore = report.Compatible ? 1f : 0f;
    }

    private static float Clamp01(float value)
    {
        if (value < 0f)
        {
            return 0f;
        }

        if (value > 1f)
        {
            return 1f;
        }

        return value;
    }
}
