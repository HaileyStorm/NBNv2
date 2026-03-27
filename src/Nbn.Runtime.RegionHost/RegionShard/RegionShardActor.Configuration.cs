using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.RegionHost;

public sealed partial class RegionShardActor
{
    private void HandleUpdateOutputSink(UpdateShardOutputSink message)
    {
        if (!MatchesShardMessage(message.BrainId, message.RegionId, message.ShardIndex))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(message.OutputPid))
        {
            SetOutputSink(outputSink: null, rawPid: null);
            return;
        }

        if (TryParsePid(message.OutputPid, out var pid))
        {
            SetOutputSink(pid, message.OutputPid);
        }
    }

    private void HandleUpdateVisualization(UpdateShardVisualization message)
    {
        if (!MatchesShardMessage(message.BrainId, message.RegionId, message.ShardIndex))
        {
            return;
        }

        _vizEnabled = message.Enabled;
        _vizFocusRegionId = message.Enabled && message.HasFocusRegion
            ? message.FocusRegionId
            : null;
        _vizStreamMinIntervalMs = NormalizeVisualizationMinIntervalMs(message.VizStreamMinIntervalMs);
        LogVisualizationConfigUpdate();
    }

    private void HandleUpdateRuntimeConfig(UpdateShardRuntimeConfig message)
    {
        if (!MatchesShardMessage(message.BrainId, message.RegionId, message.ShardIndex))
        {
            return;
        }

        ApplyCostConfig(message);
        ApplyPlasticityConfig(message);
        ApplyHomeostasisConfig(message);
        ApplyObservabilityConfig(message);
    }

    private void SetOutputSink(PID? outputSink, string? rawPid)
    {
        _outputSink = outputSink;
        if (!LogOutput || !_state.IsOutputRegion)
        {
            return;
        }

        if (outputSink is null)
        {
            Console.WriteLine($"[RegionShard] Output sink cleared for brain={_brainId} shard={_shardId}.");
            return;
        }

        Console.WriteLine($"[RegionShard] Output sink set for brain={_brainId} shard={_shardId} sink={rawPid ?? FormatPidLabel(outputSink)}.");
    }

    private void LogVisualizationConfigUpdate()
    {
        if (!LogViz && !LogVizDiagnostics)
        {
            return;
        }

        var focusLabel = _vizFocusRegionId.HasValue ? _vizFocusRegionId.Value.ToString() : "all";
        Console.WriteLine(
            $"[RegionShard] Viz config updated brain={_brainId} shard={_shardId} enabled={_vizEnabled} focus={focusLabel} streamMinIntervalMs={_vizStreamMinIntervalMs} hub={FormatPidLabel(_vizHub)}.");
    }

    private void ApplyCostConfig(UpdateShardRuntimeConfig message)
    {
        _costEnergyEnabled = message.CostEnabled && message.EnergyEnabled;
        _remoteCostEnabled = message.RemoteCostEnabled;
        _remoteCostPerBatch = Math.Max(0L, message.RemoteCostPerBatch);
        _remoteCostPerContribution = Math.Max(0L, message.RemoteCostPerContribution);
        _costTierAMultiplier = NormalizeTierMultiplier(message.CostTierAMultiplier);
        _costTierBMultiplier = NormalizeTierMultiplier(message.CostTierBMultiplier);
        _costTierCMultiplier = NormalizeTierMultiplier(message.CostTierCMultiplier);
    }

    private void ApplyPlasticityConfig(UpdateShardRuntimeConfig message)
    {
        _plasticityEnabled = message.PlasticityEnabled;
        _plasticityRate = message.PlasticityRate;
        _plasticityProbabilisticUpdates = message.ProbabilisticUpdates;
        _plasticityDelta = message.PlasticityDelta;
        _plasticityRebaseThreshold = message.PlasticityRebaseThreshold;
        _plasticityRebaseThresholdPct = message.PlasticityRebaseThresholdPct;
        _plasticityEnergyCostModulationEnabled = message.PlasticityEnergyCostModulationEnabled;
        _plasticityEnergyCostReferenceTickCost = Math.Max(1L, message.PlasticityEnergyCostReferenceTickCost);
        _plasticityEnergyCostResponseStrength = NormalizeFiniteInRange(message.PlasticityEnergyCostResponseStrength, 0f, 8f, RegionShardPlasticityEnergyCostConfig.Default.ResponseStrength);
        _plasticityEnergyCostMinScale = NormalizeFiniteInRange(message.PlasticityEnergyCostMinScale, 0f, 1f, RegionShardPlasticityEnergyCostConfig.Default.MinScale);
        _plasticityEnergyCostMaxScale = NormalizeFiniteInRange(message.PlasticityEnergyCostMaxScale, 0f, 1f, RegionShardPlasticityEnergyCostConfig.Default.MaxScale);
        if (_plasticityEnergyCostMaxScale < _plasticityEnergyCostMinScale)
        {
            _plasticityEnergyCostMaxScale = _plasticityEnergyCostMinScale;
        }
    }

    private void ApplyHomeostasisConfig(UpdateShardRuntimeConfig message)
    {
        _homeostasisEnabled = message.HomeostasisEnabled;
        _homeostasisTargetMode = message.HomeostasisTargetMode;
        _homeostasisUpdateMode = message.HomeostasisUpdateMode;
        _homeostasisBaseProbability = message.HomeostasisBaseProbability;
        _homeostasisMinStepCodes = message.HomeostasisMinStepCodes;
        _homeostasisEnergyCouplingEnabled = message.HomeostasisEnergyCouplingEnabled;
        _homeostasisEnergyTargetScale = message.HomeostasisEnergyTargetScale;
        _homeostasisEnergyProbabilityScale = message.HomeostasisEnergyProbabilityScale;
    }

    private void ApplyObservabilityConfig(UpdateShardRuntimeConfig message)
    {
        _outputVectorSource = NormalizeOutputVectorSource(message.OutputVectorSource);
        _debugEnabled = message.DebugEnabled;
        _debugMinSeverity = message.DebugMinSeverity;
    }

    private bool MatchesRegionMessage(Nbn.Proto.Uuid? brainId, uint regionId)
        => brainId is not null
           && brainId.TryToGuid(out var guid)
           && guid == _brainId
           && regionId == (uint)_state.RegionId;

    private bool MatchesShardMessage(Nbn.Proto.Uuid? brainId, uint regionId, uint shardIndex)
        => MatchesRegionMessage(brainId, regionId)
           && shardIndex == (uint)_shardId.ShardIndex;

    private static float NormalizeTierMultiplier(float value)
    {
        return float.IsFinite(value) && value > 0f
            ? value
            : 1f;
    }

    private static OutputVectorSource NormalizeOutputVectorSource(OutputVectorSource source)
    {
        return source switch
        {
            OutputVectorSource.Buffer => source,
            _ => OutputVectorSource.Potential
        };
    }

    private static float NormalizeFiniteInRange(float value, float min, float max, float fallback)
    {
        if (!float.IsFinite(value))
        {
            return fallback;
        }

        return Math.Clamp(value, min, max);
    }

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }

    private static string FormatPidLabel(PID? pid)
    {
        if (pid is null)
        {
            return "(null)";
        }

        return string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
    }
}
