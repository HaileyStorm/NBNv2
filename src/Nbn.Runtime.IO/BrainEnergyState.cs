namespace Nbn.Runtime.IO;

public sealed class BrainEnergyState
{
    private DateTimeOffset _lastUpdate;
    private double _carry;

    public BrainEnergyState(
        long energyRemaining = 0,
        long energyRateUnitsPerSecond = 0,
        bool costEnabled = false,
        bool energyEnabled = false,
        bool plasticityEnabled = false,
        float plasticityRate = 0,
        bool plasticityProbabilisticUpdates = false)
    {
        EnergyRemaining = energyRemaining;
        EnergyRateUnitsPerSecond = energyRateUnitsPerSecond;
        CostEnabled = costEnabled;
        EnergyEnabled = energyEnabled;
        PlasticityEnabled = plasticityEnabled;
        PlasticityRate = plasticityRate;
        PlasticityProbabilisticUpdates = plasticityProbabilisticUpdates;
        _lastUpdate = DateTimeOffset.UtcNow;
    }

    public bool CostEnabled { get; private set; }
    public bool EnergyEnabled { get; private set; }
    public bool PlasticityEnabled { get; private set; }
    public float PlasticityRate { get; private set; }
    public bool PlasticityProbabilisticUpdates { get; private set; }

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
        CostEnabled = costEnabled;
        EnergyEnabled = energyEnabled;
    }

    public void SetPlasticity(bool enabled, float rate, bool probabilisticUpdates)
    {
        PlasticityEnabled = enabled;
        PlasticityRate = rate;
        PlasticityProbabilisticUpdates = probabilisticUpdates;
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
        CostEnabled = state.CostEnabled;
        EnergyEnabled = state.EnergyEnabled;
        PlasticityEnabled = state.PlasticityEnabled;
        PlasticityRate = state.PlasticityRate;
        PlasticityProbabilisticUpdates = state.PlasticityProbabilisticUpdates;
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
        CostEnabled = state.CostEnabled;
        EnergyEnabled = state.EnergyEnabled;
        PlasticityEnabled = state.PlasticityEnabled;
        PlasticityRate = state.PlasticityRate;
        PlasticityProbabilisticUpdates = state.PlasticityProbabilisticUpdates;
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
}
