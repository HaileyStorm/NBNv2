using Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed class BrainEnergyState
{
    private const float MinScale = 0f;
    private const float MaxScale = 4f;
    private const float DefaultPlasticityRate = 0.001f;
    private const float DefaultPlasticityDelta = DefaultPlasticityRate;
    private const long DefaultPlasticityEnergyCostReferenceTickCost = 100;
    private const float DefaultPlasticityEnergyCostResponseStrength = 1f;
    private const float DefaultPlasticityEnergyCostMinScale = 0.1f;
    private const float DefaultPlasticityEnergyCostMaxScale = 1f;
    private const float DefaultHomeostasisBaseProbability = 0.01f;
    private const uint DefaultHomeostasisMinStepCodes = 1;
    private const float DefaultHomeostasisTargetScale = 1f;
    private const float DefaultHomeostasisProbabilityScale = 1f;

    private DateTimeOffset _lastUpdate;
    private double _carry;

    public BrainEnergyState(
        long energyRemaining = 0,
        long energyRateUnitsPerSecond = 0,
        bool costEnabled = false,
        bool energyEnabled = false,
        bool plasticityEnabled = true,
        float plasticityRate = DefaultPlasticityRate,
        bool plasticityProbabilisticUpdates = true,
        float plasticityDelta = DefaultPlasticityDelta,
        uint plasticityRebaseThreshold = 0,
        float plasticityRebaseThresholdPct = 0,
        bool plasticityEnergyCostModulationEnabled = false,
        long plasticityEnergyCostReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost,
        float plasticityEnergyCostResponseStrength = DefaultPlasticityEnergyCostResponseStrength,
        float plasticityEnergyCostMinScale = DefaultPlasticityEnergyCostMinScale,
        float plasticityEnergyCostMaxScale = DefaultPlasticityEnergyCostMaxScale,
        bool homeostasisEnabled = true,
        HomeostasisTargetMode homeostasisTargetMode = HomeostasisTargetMode.HomeostasisTargetZero,
        HomeostasisUpdateMode homeostasisUpdateMode = HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
        float homeostasisBaseProbability = DefaultHomeostasisBaseProbability,
        uint homeostasisMinStepCodes = DefaultHomeostasisMinStepCodes,
        bool homeostasisEnergyCouplingEnabled = false,
        float homeostasisEnergyTargetScale = DefaultHomeostasisTargetScale,
        float homeostasisEnergyProbabilityScale = DefaultHomeostasisProbabilityScale)
    {
        EnergyRemaining = energyRemaining;
        EnergyRateUnitsPerSecond = energyRateUnitsPerSecond;
        var combinedCostEnergyEnabled = costEnabled && energyEnabled;
        CostEnabled = combinedCostEnergyEnabled;
        EnergyEnabled = combinedCostEnergyEnabled;
        PlasticityEnabled = plasticityEnabled;
        PlasticityRate = plasticityRate;
        PlasticityProbabilisticUpdates = plasticityProbabilisticUpdates;
        PlasticityDelta = plasticityDelta;
        PlasticityRebaseThreshold = plasticityRebaseThreshold;
        PlasticityRebaseThresholdPct = plasticityRebaseThresholdPct;
        SetPlasticityEnergyCostModulation(
            plasticityEnergyCostModulationEnabled,
            plasticityEnergyCostReferenceTickCost,
            plasticityEnergyCostResponseStrength,
            plasticityEnergyCostMinScale,
            plasticityEnergyCostMaxScale);
        SetHomeostasis(
            homeostasisEnabled,
            homeostasisTargetMode,
            homeostasisUpdateMode,
            homeostasisBaseProbability,
            homeostasisMinStepCodes,
            homeostasisEnergyCouplingEnabled,
            homeostasisEnergyTargetScale,
            homeostasisEnergyProbabilityScale);
        _lastUpdate = DateTimeOffset.UtcNow;
    }

    public bool CostEnabled { get; private set; }
    public bool EnergyEnabled { get; private set; }
    public bool PlasticityEnabled { get; private set; }
    public float PlasticityRate { get; private set; }
    public bool PlasticityProbabilisticUpdates { get; private set; }
    public float PlasticityDelta { get; private set; }
    public uint PlasticityRebaseThreshold { get; private set; }
    public float PlasticityRebaseThresholdPct { get; private set; }
    public bool PlasticityEnergyCostModulationEnabled { get; private set; }
    public long PlasticityEnergyCostReferenceTickCost { get; private set; }
    public float PlasticityEnergyCostResponseStrength { get; private set; }
    public float PlasticityEnergyCostMinScale { get; private set; }
    public float PlasticityEnergyCostMaxScale { get; private set; }
    public bool HomeostasisEnabled { get; private set; }
    public HomeostasisTargetMode HomeostasisTargetMode { get; private set; }
    public HomeostasisUpdateMode HomeostasisUpdateMode { get; private set; }
    public float HomeostasisBaseProbability { get; private set; }
    public uint HomeostasisMinStepCodes { get; private set; }
    public bool HomeostasisEnergyCouplingEnabled { get; private set; }
    public float HomeostasisEnergyTargetScale { get; private set; }
    public float HomeostasisEnergyProbabilityScale { get; private set; }

    public long EnergyRemaining { get; private set; }
    public long EnergyRateUnitsPerSecond { get; private set; }
    public long LastTickCost { get; private set; }

    public void ApplyCredit(long amount)
    {
        Accrue();
        EnergyRemaining += amount;
    }

    public void SetEnergyRate(long unitsPerSecond)
    {
        Accrue();
        EnergyRateUnitsPerSecond = unitsPerSecond;
    }

    public void SetCostEnergyEnabled(bool costEnabled, bool energyEnabled)
    {
        var combinedEnabled = costEnabled && energyEnabled;
        CostEnabled = combinedEnabled;
        EnergyEnabled = combinedEnabled;
    }

    public void SetPlasticity(
        bool enabled,
        float rate,
        bool probabilisticUpdates,
        float delta,
        uint rebaseThreshold,
        float rebaseThresholdPct,
        bool energyCostModulationEnabled,
        long energyCostReferenceTickCost,
        float energyCostResponseStrength,
        float energyCostMinScale,
        float energyCostMaxScale)
    {
        PlasticityEnabled = enabled;
        PlasticityRate = rate;
        PlasticityProbabilisticUpdates = probabilisticUpdates;
        PlasticityDelta = delta;
        PlasticityRebaseThreshold = rebaseThreshold;
        PlasticityRebaseThresholdPct = rebaseThresholdPct;
        SetPlasticityEnergyCostModulation(
            energyCostModulationEnabled,
            energyCostReferenceTickCost,
            energyCostResponseStrength,
            energyCostMinScale,
            energyCostMaxScale);
    }

    public void SetHomeostasis(
        bool enabled,
        HomeostasisTargetMode targetMode,
        HomeostasisUpdateMode updateMode,
        float baseProbability,
        uint minStepCodes,
        bool energyCouplingEnabled,
        float energyTargetScale,
        float energyProbabilityScale)
    {
        HomeostasisEnabled = enabled;
        HomeostasisTargetMode = NormalizeHomeostasisTargetMode(targetMode);
        HomeostasisUpdateMode = NormalizeHomeostasisUpdateMode(updateMode);
        HomeostasisBaseProbability = ClampFinite(baseProbability, 0f, 1f, DefaultHomeostasisBaseProbability);
        HomeostasisMinStepCodes = minStepCodes == 0 ? DefaultHomeostasisMinStepCodes : minStepCodes;
        HomeostasisEnergyCouplingEnabled = energyCouplingEnabled;
        HomeostasisEnergyTargetScale = ClampFinite(energyTargetScale, MinScale, MaxScale, DefaultHomeostasisTargetScale);
        HomeostasisEnergyProbabilityScale = ClampFinite(energyProbabilityScale, MinScale, MaxScale, DefaultHomeostasisProbabilityScale);
    }

    public void SetRuntimeConfig(
        bool costEnabled,
        bool energyEnabled,
        bool plasticityEnabled,
        float plasticityRate,
        bool plasticityProbabilisticUpdates,
        float plasticityDelta,
        uint plasticityRebaseThreshold,
        float plasticityRebaseThresholdPct,
        bool plasticityEnergyCostModulationEnabled,
        long plasticityEnergyCostReferenceTickCost,
        float plasticityEnergyCostResponseStrength,
        float plasticityEnergyCostMinScale,
        float plasticityEnergyCostMaxScale,
        bool homeostasisEnabled,
        HomeostasisTargetMode homeostasisTargetMode,
        HomeostasisUpdateMode homeostasisUpdateMode,
        float homeostasisBaseProbability,
        uint homeostasisMinStepCodes,
        bool homeostasisEnergyCouplingEnabled,
        float homeostasisEnergyTargetScale,
        float homeostasisEnergyProbabilityScale,
        long lastTickCost)
    {
        var combinedCostEnergyEnabled = costEnabled && energyEnabled;
        CostEnabled = combinedCostEnergyEnabled;
        EnergyEnabled = combinedCostEnergyEnabled;
        PlasticityEnabled = plasticityEnabled;
        PlasticityRate = plasticityRate;
        PlasticityProbabilisticUpdates = plasticityProbabilisticUpdates;
        PlasticityDelta = plasticityDelta;
        PlasticityRebaseThreshold = plasticityRebaseThreshold;
        PlasticityRebaseThresholdPct = plasticityRebaseThresholdPct;
        SetPlasticityEnergyCostModulation(
            plasticityEnergyCostModulationEnabled,
            plasticityEnergyCostReferenceTickCost,
            plasticityEnergyCostResponseStrength,
            plasticityEnergyCostMinScale,
            plasticityEnergyCostMaxScale);
        SetHomeostasis(
            homeostasisEnabled,
            homeostasisTargetMode,
            homeostasisUpdateMode,
            homeostasisBaseProbability,
            homeostasisMinStepCodes,
            homeostasisEnergyCouplingEnabled,
            homeostasisEnergyTargetScale,
            homeostasisEnergyProbabilityScale);
        LastTickCost = lastTickCost;
    }

    public void ApplyTickCost(long tickCost)
    {
        Accrue();
        LastTickCost = tickCost;
        EnergyRemaining -= tickCost;
    }

    public void ResetFrom(BrainEnergyState state)
    {
        if (state is null)
        {
            return;
        }

        EnergyRemaining = state.EnergyRemaining;
        EnergyRateUnitsPerSecond = state.EnergyRateUnitsPerSecond;
        var combinedCostEnergyEnabled = state.CostEnabled && state.EnergyEnabled;
        CostEnabled = combinedCostEnergyEnabled;
        EnergyEnabled = combinedCostEnergyEnabled;
        PlasticityEnabled = state.PlasticityEnabled;
        PlasticityRate = state.PlasticityRate;
        PlasticityProbabilisticUpdates = state.PlasticityProbabilisticUpdates;
        PlasticityDelta = state.PlasticityDelta;
        PlasticityRebaseThreshold = state.PlasticityRebaseThreshold;
        PlasticityRebaseThresholdPct = state.PlasticityRebaseThresholdPct;
        SetPlasticityEnergyCostModulation(
            state.PlasticityEnergyCostModulationEnabled,
            state.PlasticityEnergyCostReferenceTickCost,
            state.PlasticityEnergyCostResponseStrength,
            state.PlasticityEnergyCostMinScale,
            state.PlasticityEnergyCostMaxScale);
        HomeostasisEnabled = state.HomeostasisEnabled;
        HomeostasisTargetMode = state.HomeostasisTargetMode;
        HomeostasisUpdateMode = state.HomeostasisUpdateMode;
        HomeostasisBaseProbability = state.HomeostasisBaseProbability;
        HomeostasisMinStepCodes = state.HomeostasisMinStepCodes;
        HomeostasisEnergyCouplingEnabled = state.HomeostasisEnergyCouplingEnabled;
        HomeostasisEnergyTargetScale = state.HomeostasisEnergyTargetScale;
        HomeostasisEnergyProbabilityScale = state.HomeostasisEnergyProbabilityScale;
        LastTickCost = state.LastTickCost;
        _carry = 0;
        _lastUpdate = DateTimeOffset.UtcNow;
    }

    public void ResetFrom(Nbn.Proto.Io.BrainEnergyState state)
    {
        if (state is null)
        {
            return;
        }

        EnergyRemaining = state.EnergyRemaining;
        EnergyRateUnitsPerSecond = state.EnergyRateUnitsPerSecond;
        var combinedCostEnergyEnabled = state.CostEnabled && state.EnergyEnabled;
        CostEnabled = combinedCostEnergyEnabled;
        EnergyEnabled = combinedCostEnergyEnabled;
        PlasticityEnabled = state.PlasticityEnabled;
        PlasticityRate = state.PlasticityRate;
        PlasticityProbabilisticUpdates = state.PlasticityProbabilisticUpdates;
        PlasticityDelta = state.PlasticityDelta;
        PlasticityRebaseThreshold = state.PlasticityRebaseThreshold;
        PlasticityRebaseThresholdPct = state.PlasticityRebaseThresholdPct;
        SetPlasticityEnergyCostModulation(
            state.PlasticityEnergyCostModulationEnabled,
            state.PlasticityEnergyCostReferenceTickCost,
            state.PlasticityEnergyCostResponseStrength,
            state.PlasticityEnergyCostMinScale,
            state.PlasticityEnergyCostMaxScale);
        SetHomeostasis(
            state.HomeostasisEnabled,
            state.HomeostasisTargetMode,
            state.HomeostasisUpdateMode,
            state.HomeostasisBaseProbability,
            state.HomeostasisMinStepCodes,
            state.HomeostasisEnergyCouplingEnabled,
            state.HomeostasisEnergyTargetScale,
            state.HomeostasisEnergyProbabilityScale);
        LastTickCost = state.LastTickCost;
        _carry = 0;
        _lastUpdate = DateTimeOffset.UtcNow;
    }

    public void Accrue()
    {
        var now = DateTimeOffset.UtcNow;
        var elapsedSeconds = (now - _lastUpdate).TotalSeconds;
        if (elapsedSeconds <= 0)
        {
            return;
        }

        var delta = elapsedSeconds * EnergyRateUnitsPerSecond + _carry;
        var whole = (long)Math.Truncate(delta);
        _carry = delta - whole;
        if (whole != 0)
        {
            EnergyRemaining += whole;
        }

        _lastUpdate = now;
    }

    private static HomeostasisTargetMode NormalizeHomeostasisTargetMode(HomeostasisTargetMode mode)
    {
        return mode switch
        {
            HomeostasisTargetMode.HomeostasisTargetZero => HomeostasisTargetMode.HomeostasisTargetZero,
            HomeostasisTargetMode.HomeostasisTargetFixed => HomeostasisTargetMode.HomeostasisTargetFixed,
            _ => HomeostasisTargetMode.HomeostasisTargetZero
        };
    }

    private static HomeostasisUpdateMode NormalizeHomeostasisUpdateMode(HomeostasisUpdateMode mode)
    {
        return mode switch
        {
            HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep => HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
            _ => HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep
        };
    }

    private static float ClampFinite(float value, float min, float max, float fallback)
    {
        if (!float.IsFinite(value))
        {
            return fallback;
        }

        return Math.Clamp(value, min, max);
    }

    private void SetPlasticityEnergyCostModulation(
        bool enabled,
        long referenceTickCost,
        float responseStrength,
        float minScale,
        float maxScale)
    {
        PlasticityEnergyCostModulationEnabled = enabled;
        if (!enabled)
        {
            var hasExplicitConfiguration = referenceTickCost > 0
                                           || (float.IsFinite(responseStrength) && responseStrength > 0f)
                                           || (float.IsFinite(minScale) && minScale > 0f)
                                           || (float.IsFinite(maxScale) && maxScale > 0f);
            if (!hasExplicitConfiguration)
            {
                PlasticityEnergyCostReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost;
                PlasticityEnergyCostResponseStrength = DefaultPlasticityEnergyCostResponseStrength;
                PlasticityEnergyCostMinScale = DefaultPlasticityEnergyCostMinScale;
                PlasticityEnergyCostMaxScale = DefaultPlasticityEnergyCostMaxScale;
                return;
            }

            PlasticityEnergyCostReferenceTickCost = referenceTickCost > 0
                ? referenceTickCost
                : DefaultPlasticityEnergyCostReferenceTickCost;
            PlasticityEnergyCostResponseStrength = float.IsFinite(responseStrength) && responseStrength >= 0f
                ? Math.Clamp(responseStrength, 0f, 8f)
                : DefaultPlasticityEnergyCostResponseStrength;

            var hasExplicitScale = float.IsFinite(minScale)
                                   && float.IsFinite(maxScale)
                                   && (minScale > 0f || maxScale > 0f);
            if (hasExplicitScale)
            {
                PlasticityEnergyCostMinScale = Math.Clamp(minScale, 0f, 1f);
                PlasticityEnergyCostMaxScale = Math.Clamp(maxScale, 0f, 1f);
            }
            else
            {
                PlasticityEnergyCostMinScale = DefaultPlasticityEnergyCostMinScale;
                PlasticityEnergyCostMaxScale = DefaultPlasticityEnergyCostMaxScale;
            }
        }
        else
        {
            PlasticityEnergyCostReferenceTickCost = Math.Max(1L, referenceTickCost);
            PlasticityEnergyCostResponseStrength = ClampFinite(responseStrength, 0f, 8f, DefaultPlasticityEnergyCostResponseStrength);
            PlasticityEnergyCostMinScale = ClampFinite(minScale, 0f, 1f, DefaultPlasticityEnergyCostMinScale);
            PlasticityEnergyCostMaxScale = ClampFinite(maxScale, 0f, 1f, DefaultPlasticityEnergyCostMaxScale);
        }

        if (PlasticityEnergyCostMaxScale < PlasticityEnergyCostMinScale)
        {
            PlasticityEnergyCostMaxScale = PlasticityEnergyCostMinScale;
        }
    }
}
