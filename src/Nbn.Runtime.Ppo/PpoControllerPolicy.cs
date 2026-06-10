using System.Globalization;
using System.Text.Json;
using Nbn.Proto;
using Nbn.Proto.Ppo;
using Nbn.Shared;
using ProtoRepro = Nbn.Proto.Repro;

namespace Nbn.Runtime.Ppo;

internal sealed class PpoControllerPolicy
{
    private const float MinimumLogProbability = 0.000001f;
    private const float MaximumProbability = 0.60f;
    private const float ExplorationLogScale = 0.65f;
    private readonly Dictionary<string, PendingAction> _pending = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private ControllerWeights _weights = ControllerWeights.Default;
    private ulong _updateIndex;
    private PpoPolicyUpdateReport? _lastUpdate;

    public PpoPolicyApplication Apply(
        string runId,
        PpoStartRunRequest request,
        ProtoRepro.ReproduceConfig config,
        uint rolloutIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(config);

        lock (_gate)
        {
            var baseline = _weights.ToProbabilities(config);
            var probabilities = SampleActionProbabilities(baseline, runId, rolloutIndex, request.Hyperparameters);
            ApplyProbabilities(config, probabilities);
            var actionJson = CreateActionJson(probabilities);
            var logProbability = EstimateActionLogProbability(probabilities);
            var valueEstimate = _weights.ValueBias;
            var action = new PendingAction(
                runId,
                probabilities,
                logProbability,
                valueEstimate,
                actionJson,
                request.ObjectiveName?.Trim() ?? string.Empty,
                request.Hyperparameters?.RewardSignal?.Trim() ?? string.Empty,
                string.Empty,
                request.Hyperparameters?.Clone());

            return new PpoPolicyApplication(action, SerializePolicyStateLocked());
        }
    }

    public void RegisterCandidates(string runId, IReadOnlyList<PpoManagerActor.CandidateArtifact> candidates, PpoPolicyApplication application)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        ArgumentNullException.ThrowIfNull(application);

        lock (_gate)
        {
            foreach (var candidate in candidates)
            {
                _pending[BuildKey(runId, candidate.RunIndex)] = application.Action with
                {
                    ChildDefSha = TryArtifactSha(candidate.ChildDef)
                };
            }
        }
    }

    public PpoRecordRewardsResponse RecordRewards(PpoRecordRewardsRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Samples.Count == 0)
        {
            return CreateRecordFailure("ppo_reward_samples_required");
        }

        var samples = new List<RewardSample>(request.Samples.Count);
        foreach (var sample in request.Samples)
        {
            if (!TryValidateRewardSample(sample, out var failure))
            {
                return CreateRecordFailure(failure);
            }

            samples.Add(new RewardSample(
                sample.RunId.Trim(),
                sample.RunIndex,
                sample.Reward,
                TryArtifactSha(sample.ChildDef)));
        }

        lock (_gate)
        {
            var accepted = new List<(RewardSample Sample, PendingAction Action)>(samples.Count);
            foreach (var sample in samples)
            {
                if (_pending.TryGetValue(BuildKey(sample.RunId, sample.RunIndex), out var action)
                    && RewardSampleMatchesPendingAction(sample, action, request))
                {
                    accepted.Add((sample, action));
                }
            }

            if (accepted.Count == 0)
            {
                return CreateRecordFailure("ppo_reward_samples_unmatched");
            }

            var hyperparameters = request.Hyperparameters ?? accepted[^1].Action.Hyperparameters ?? new PpoHyperparameters
            {
                ClipEpsilon = 0.2f,
                DiscountGamma = 0.99f,
                GaeLambda = 0.95f,
                LearningRate = 0.0003f,
                OptimizationEpochCount = 1,
                MinibatchSize = 1,
                RolloutBatchCount = 1,
                RolloutTickCount = 1
            };

            var report = UpdatePolicyLocked(accepted, hyperparameters);
            foreach (var item in accepted)
            {
                _pending.Remove(BuildKey(item.Sample.RunId, item.Sample.RunIndex));
            }

            _lastUpdate = report.Clone();
            return new PpoRecordRewardsResponse
            {
                FailureReason = PpoFailureReason.PpoFailureNone,
                Accepted = true,
                Update = report
            };
        }
    }

    public PpoPolicyUpdateReport? LastUpdate
    {
        get
        {
            lock (_gate)
            {
                return _lastUpdate?.Clone();
            }
        }
    }

    private PpoPolicyUpdateReport UpdatePolicyLocked(
        IReadOnlyList<(RewardSample Sample, PendingAction Action)> samples,
        PpoHyperparameters hyperparameters)
    {
        var rewards = samples.Select(static item => item.Sample.Reward).ToArray();
        var meanReward = rewards.Average();
        var maxReward = rewards.Max();
        var gammaLambda = Math.Clamp(hyperparameters.DiscountGamma * hyperparameters.GaeLambda, 0.01f, 1f);
        var advantages = ComputeAdvantages(rewards, samples.Select(static item => item.Action.ValueEstimate).ToArray(), gammaLambda);
        var meanAdvantage = advantages.Average();
        var learningRate = Math.Clamp(hyperparameters.LearningRate, 0.00001f, 0.05f);
        var clip = Math.Clamp(hyperparameters.ClipEpsilon, 0.01f, 1f);
        var epochs = Math.Clamp(hyperparameters.OptimizationEpochCount, 1u, 32u);
        var scale = learningRate * epochs;
        var delta = new ControllerWeights();
        var policyLoss = 0f;
        var valueLoss = 0f;
        var approximateKl = 0f;

        for (var i = 0; i < samples.Count; i++)
        {
            var action = samples[i].Action;
            var advantage = Math.Clamp(advantages[i], -clip, clip);
            policyLoss -= advantage;
            var valueError = rewards[i] - action.ValueEstimate;
            valueLoss += valueError * valueError;
            var rewardCentered = rewards[i] - meanReward;
            delta = delta.Add(action.Probabilities.AsGradient().Scale(advantage + (0.25f * rewardCentered)));
            approximateKl += Math.Abs(EstimateActionLogProbability(action.Probabilities) - action.OldLogProbability);
        }

        var sampleCount = Math.Max(1, samples.Count);
        _weights = _weights
            .Add(delta.Scale(scale / sampleCount))
            .WithValueBias(_weights.ValueBias + (scale * (meanReward - _weights.ValueBias)));
        _updateIndex++;

        return new PpoPolicyUpdateReport
        {
            UpdateIndex = _updateIndex,
            AcceptedSampleCount = (uint)samples.Count,
            MeanReward = meanReward,
            MaxReward = maxReward,
            MeanAdvantage = meanAdvantage,
            PolicyLoss = policyLoss / sampleCount,
            ValueLoss = valueLoss / sampleCount,
            Entropy = _weights.ToProbabilities(new ProtoRepro.ReproduceConfig()).Entropy,
            ApproximateKl = approximateKl / sampleCount,
            PolicyStateJson = SerializePolicyStateLocked()
        };
    }

    private static float[] ComputeAdvantages(float[] rewards, float[] values, float gammaLambda)
    {
        var advantages = new float[rewards.Length];
        var nextAdvantage = 0f;
        for (var i = rewards.Length - 1; i >= 0; i--)
        {
            var nextValue = i + 1 < values.Length ? values[i + 1] : 0f;
            var delta = rewards[i] + (gammaLambda * nextValue) - values[i];
            nextAdvantage = delta + (gammaLambda * nextAdvantage);
            advantages[i] = nextAdvantage;
        }

        if (advantages.Length <= 1)
        {
            return advantages;
        }

        var mean = advantages.Average();
        var variance = advantages.Select(value => (value - mean) * (value - mean)).Average();
        var std = MathF.Sqrt(Math.Max(variance, 0.000001f));
        for (var i = 0; i < advantages.Length; i++)
        {
            advantages[i] = (advantages[i] - mean) / std;
        }

        return advantages;
    }

    private static bool TryValidateRewardSample(PpoRewardSample sample, out string failure)
    {
        if (string.IsNullOrWhiteSpace(sample.RunId))
        {
            failure = "ppo_reward_run_id_required";
            return false;
        }

        if (!float.IsFinite(sample.Reward))
        {
            failure = "ppo_reward_non_finite";
            return false;
        }

        if (!float.IsFinite(sample.Accuracy))
        {
            failure = "ppo_reward_accuracy_non_finite";
            return false;
        }

        if (!float.IsFinite(sample.Fitness))
        {
            failure = "ppo_reward_fitness_non_finite";
            return false;
        }

        failure = string.Empty;
        return true;
    }

    private static bool RewardSampleMatchesPendingAction(
        RewardSample sample,
        PendingAction action,
        PpoRecordRewardsRequest request)
    {
        if (!string.IsNullOrWhiteSpace(action.ChildDefSha)
            && !string.Equals(sample.ChildDefSha, action.ChildDefSha, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(action.ObjectiveName)
            && !string.IsNullOrWhiteSpace(request.ObjectiveName)
            && !string.Equals(action.ObjectiveName, request.ObjectiveName.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(action.RewardSignal)
            && !string.IsNullOrWhiteSpace(request.RewardSignal)
            && !string.Equals(action.RewardSignal, request.RewardSignal.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        return true;
    }

    private static ActionProbabilities SampleActionProbabilities(
        ActionProbabilities baseline,
        string runId,
        uint rolloutIndex,
        PpoHyperparameters? hyperparameters)
    {
        if ((hyperparameters?.RolloutBatchCount ?? 1) <= 1)
        {
            return baseline;
        }

        var state = CreateExplorationSeed(runId, rolloutIndex, hyperparameters);
        return new ActionProbabilities(
            PerturbProbability(baseline.ParameterMutation, ref state),
            PerturbProbability(baseline.StrengthMutation, ref state),
            PerturbProbability(baseline.FunctionMutation, ref state),
            PerturbProbability(baseline.AddAxon, ref state),
            PerturbProbability(baseline.RemoveAxon, ref state),
            PerturbProbability(baseline.RerouteAxon, ref state),
            PerturbProbability(baseline.DisableNeuron, ref state),
            PerturbProbability(baseline.ReactivateNeuron, ref state),
            PerturbProbability(baseline.AddNeuronToEmptyRegion, ref state),
            PerturbProbability(baseline.RemoveLastNeuronFromRegion, ref state),
            PerturbProbability(baseline.RerouteInboundAxonOnDelete, ref state));
    }

    private static ulong CreateExplorationSeed(string runId, uint rolloutIndex, PpoHyperparameters? hyperparameters)
    {
        unchecked
        {
            var state = 1469598103934665603UL;
            foreach (var ch in runId.AsSpan())
            {
                state ^= ch;
                state *= 1099511628211UL;
            }

            state ^= hyperparameters?.Seed ?? 0UL;
            state *= 1099511628211UL;
            state ^= rolloutIndex + 0x9E3779B9UL;
            state *= 1099511628211UL;
            return state == 0UL ? 0xD1B54A32D192ED03UL : state;
        }
    }

    private static float PerturbProbability(float baseline, ref ulong state)
    {
        if (baseline <= 0f || !float.IsFinite(baseline))
        {
            return 0f;
        }

        var noise = (NextUnitFloat(ref state) * 2f) - 1f;
        return ClampProbability(baseline * MathF.Exp(noise * ExplorationLogScale));
    }

    private static float NextUnitFloat(ref ulong state)
    {
        unchecked
        {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            var value = state * 2685821657736338717UL;
            return (value >> 40) / (float)(1u << 24);
        }
    }

    private static void ApplyProbabilities(ProtoRepro.ReproduceConfig config, ActionProbabilities probabilities)
    {
        config.ProbMutate = BlendProbability(config.ProbMutate, probabilities.ParameterMutation);
        if (config.StrengthTransformEnabled)
        {
            config.ProbStrengthMutate = BlendProbability(config.ProbStrengthMutate, probabilities.StrengthMutation);
        }

        config.ProbMutateFunc = BlendProbability(config.ProbMutateFunc, probabilities.FunctionMutation);
        config.ProbAddAxon = BlendProbability(config.ProbAddAxon, probabilities.AddAxon);
        config.ProbRemoveAxon = BlendProbability(config.ProbRemoveAxon, probabilities.RemoveAxon);
        config.ProbRerouteAxon = BlendProbability(config.ProbRerouteAxon, probabilities.RerouteAxon);
        config.ProbDisableNeuron = BlendProbability(config.ProbDisableNeuron, probabilities.DisableNeuron);
        config.ProbReactivateNeuron = BlendProbability(config.ProbReactivateNeuron, probabilities.ReactivateNeuron);
        config.ProbAddNeuronToEmptyRegion = BlendProbability(config.ProbAddNeuronToEmptyRegion, probabilities.AddNeuronToEmptyRegion);
        config.ProbRemoveLastNeuronFromRegion = BlendProbability(config.ProbRemoveLastNeuronFromRegion, probabilities.RemoveLastNeuronFromRegion);
        config.ProbRerouteInboundAxonOnDelete = BlendProbability(config.ProbRerouteInboundAxonOnDelete, probabilities.RerouteInboundAxonOnDelete);
    }

    private static float BlendProbability(float configured, float policy)
        => configured <= 0f ? 0f : ClampProbability((0.35f * configured) + (0.65f * policy));

    private static float ClampProbability(float value)
        => Math.Clamp(float.IsFinite(value) ? value : 0f, 0f, MaximumProbability);

    private static float ClampLogProbability(float value)
        => Math.Clamp(float.IsFinite(value) ? value : MinimumLogProbability, MinimumLogProbability, MaximumProbability);

    private static float EstimateActionLogProbability(ActionProbabilities probabilities)
        => MathF.Log(ClampLogProbability(probabilities.ParameterMutation))
           + MathF.Log(ClampLogProbability(probabilities.StrengthMutation))
           + MathF.Log(ClampLogProbability(probabilities.FunctionMutation))
           + MathF.Log(ClampLogProbability(probabilities.AddAxon))
           + MathF.Log(ClampLogProbability(probabilities.RemoveAxon))
           + MathF.Log(ClampLogProbability(probabilities.RerouteAxon))
           + MathF.Log(ClampLogProbability(probabilities.DisableNeuron))
           + MathF.Log(ClampLogProbability(probabilities.ReactivateNeuron))
           + MathF.Log(ClampLogProbability(probabilities.AddNeuronToEmptyRegion))
           + MathF.Log(ClampLogProbability(probabilities.RemoveLastNeuronFromRegion))
           + MathF.Log(ClampLogProbability(probabilities.RerouteInboundAxonOnDelete));

    private string SerializePolicyStateLocked()
        => JsonSerializer.Serialize(new
        {
            update_index = _updateIndex,
            value_bias = _weights.ValueBias,
            probabilities = _weights.ToProbabilities(new ProtoRepro.ReproduceConfig()).ToSerializable()
        });

    private static string CreateActionJson(ActionProbabilities probabilities)
        => JsonSerializer.Serialize(probabilities.ToSerializable());

    private static string BuildKey(string runId, uint runIndex)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{runId.Trim()}:{runIndex}");

    private static string TryArtifactSha(ArtifactRef? artifact)
        => artifact is not null && artifact.TryToSha256Hex(out var sha)
            ? sha
            : string.Empty;

    private static PpoRecordRewardsResponse CreateRecordFailure(string detail)
        => new()
        {
            FailureReason = PpoFailureReason.PpoFailureInvalidRequest,
            FailureDetail = detail,
            Accepted = false,
            Update = new PpoPolicyUpdateReport()
        };

    internal sealed record PpoPolicyApplication(PendingAction Action, string PolicyStateJson);

    internal sealed record PendingAction(
        string RunId,
        ActionProbabilities Probabilities,
        float OldLogProbability,
        float ValueEstimate,
        string ActionJson,
        string ObjectiveName,
        string RewardSignal,
        string ChildDefSha,
        PpoHyperparameters? Hyperparameters);

    private sealed record RewardSample(string RunId, uint RunIndex, float Reward, string ChildDefSha);

    internal readonly record struct ControllerWeights(
        float ParameterMutation,
        float StrengthMutation,
        float FunctionMutation,
        float AddAxon,
        float RemoveAxon,
        float RerouteAxon,
        float DisableNeuron,
        float ReactivateNeuron,
        float AddNeuronToEmptyRegion,
        float RemoveLastNeuronFromRegion,
        float RerouteInboundAxonOnDelete,
        float ValueBias)
    {
        public static ControllerWeights Default => new(0f, 0f, -0.05f, 0f, -0.15f, 0f, -0.10f, 0f, -0.05f, -0.20f, 0f, 0f);

        public ControllerWeights Add(ControllerWeights other)
            => new(
                ParameterMutation + other.ParameterMutation,
                StrengthMutation + other.StrengthMutation,
                FunctionMutation + other.FunctionMutation,
                AddAxon + other.AddAxon,
                RemoveAxon + other.RemoveAxon,
                RerouteAxon + other.RerouteAxon,
                DisableNeuron + other.DisableNeuron,
                ReactivateNeuron + other.ReactivateNeuron,
                AddNeuronToEmptyRegion + other.AddNeuronToEmptyRegion,
                RemoveLastNeuronFromRegion + other.RemoveLastNeuronFromRegion,
                RerouteInboundAxonOnDelete + other.RerouteInboundAxonOnDelete,
                ValueBias + other.ValueBias);

        public ControllerWeights Scale(float scale)
            => new(
                ParameterMutation * scale,
                StrengthMutation * scale,
                FunctionMutation * scale,
                AddAxon * scale,
                RemoveAxon * scale,
                RerouteAxon * scale,
                DisableNeuron * scale,
                ReactivateNeuron * scale,
                AddNeuronToEmptyRegion * scale,
                RemoveLastNeuronFromRegion * scale,
                RerouteInboundAxonOnDelete * scale,
                ValueBias * scale);

        public ControllerWeights WithValueBias(float valueBias)
            => this with { ValueBias = Math.Clamp(valueBias, -1f, 1f) };

        public ActionProbabilities ToProbabilities(ProtoRepro.ReproduceConfig config)
            => new(
                ResolveProbability(config.ProbMutate, ParameterMutation, 0.045f),
                ResolveProbability(config.ProbStrengthMutate, StrengthMutation, 0.045f),
                ResolveProbability(config.ProbMutateFunc, FunctionMutation, 0.025f),
                ResolveProbability(config.ProbAddAxon, AddAxon, 0.035f),
                ResolveProbability(config.ProbRemoveAxon, RemoveAxon, 0.018f),
                ResolveProbability(config.ProbRerouteAxon, RerouteAxon, 0.035f),
                ResolveProbability(config.ProbDisableNeuron, DisableNeuron, 0.012f),
                ResolveProbability(config.ProbReactivateNeuron, ReactivateNeuron, 0.018f),
                ResolveProbability(config.ProbAddNeuronToEmptyRegion, AddNeuronToEmptyRegion, 0.012f),
                ResolveProbability(config.ProbRemoveLastNeuronFromRegion, RemoveLastNeuronFromRegion, 0.008f),
                ResolveProbability(config.ProbRerouteInboundAxonOnDelete, RerouteInboundAxonOnDelete, 0.030f));

        private static float ResolveProbability(float configured, float weight, float fallback)
        {
            _ = fallback;
            if (configured <= 0f || !float.IsFinite(configured))
            {
                return 0f;
            }

            var baseline = configured;
            var shifted = baseline * MathF.Exp(Math.Clamp(weight, -2.5f, 2.5f));
            return ClampProbability(shifted);
        }
    }

    internal readonly record struct ActionProbabilities(
        float ParameterMutation,
        float StrengthMutation,
        float FunctionMutation,
        float AddAxon,
        float RemoveAxon,
        float RerouteAxon,
        float DisableNeuron,
        float ReactivateNeuron,
        float AddNeuronToEmptyRegion,
        float RemoveLastNeuronFromRegion,
        float RerouteInboundAxonOnDelete)
    {
        public float Entropy
            => BernoulliEntropy(ParameterMutation)
               + BernoulliEntropy(StrengthMutation)
               + BernoulliEntropy(FunctionMutation)
               + BernoulliEntropy(AddAxon)
               + BernoulliEntropy(RemoveAxon)
               + BernoulliEntropy(RerouteAxon)
               + BernoulliEntropy(DisableNeuron)
               + BernoulliEntropy(ReactivateNeuron)
               + BernoulliEntropy(AddNeuronToEmptyRegion)
               + BernoulliEntropy(RemoveLastNeuronFromRegion)
               + BernoulliEntropy(RerouteInboundAxonOnDelete);

        public ControllerWeights AsGradient()
            => new(
                ParameterMutation,
                StrengthMutation,
                FunctionMutation,
                AddAxon,
                RemoveAxon,
                RerouteAxon,
                DisableNeuron,
                ReactivateNeuron,
                AddNeuronToEmptyRegion,
                RemoveLastNeuronFromRegion,
                RerouteInboundAxonOnDelete,
                0f);

        public object ToSerializable()
            => new
            {
                parameter_mutation = ParameterMutation,
                strength_mutation = StrengthMutation,
                function_mutation = FunctionMutation,
                add_axon = AddAxon,
                remove_axon = RemoveAxon,
                reroute_axon = RerouteAxon,
                disable_neuron = DisableNeuron,
                reactivate_neuron = ReactivateNeuron,
                add_neuron_to_empty_region = AddNeuronToEmptyRegion,
                remove_last_neuron_from_region = RemoveLastNeuronFromRegion,
                reroute_inbound_axon_on_delete = RerouteInboundAxonOnDelete
            };

        private static float BernoulliEntropy(float p)
        {
            p = ClampProbability(p);
            if (p <= 0f || p >= 1f)
            {
                return 0f;
            }

            return (-p * MathF.Log(p)) - ((1f - p) * MathF.Log(1f - p));
        }
    }
}
