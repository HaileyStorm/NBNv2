using Nbn.Shared;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Reproduction;

public sealed class ReproductionSettingsTests
{
    [Fact]
    public void CreateConfigFromSettings_UsesDefaultsWhenSettingsMissing()
    {
        var config = ReproductionSettings.CreateConfigFromSettings(settings: null);

        Assert.Equal(ReproductionSettings.DefaultProbMutate, config.ProbMutate, 3);
        Assert.Equal(ReproductionSettings.DefaultProbMutateFunc, config.ProbMutateFunc, 3);
        Assert.Equal(ReproductionSettings.DefaultMaxRegionSpanDiffRatio, config.MaxRegionSpanDiffRatio, 3);
        Assert.Equal(ReproductionSettings.DefaultSpawnChildPolicy, config.SpawnChild);
    }

    [Fact]
    public void CreateConfigFromSettings_AppliesConfiguredValues()
    {
        var settings = new Dictionary<string, string?>
        {
            [ReproductionSettingsKeys.MaxRegionSpanDiffRatioKey] = "0.42",
            [ReproductionSettingsKeys.ProbMutateKey] = "0.19",
            [ReproductionSettingsKeys.ProbMutateFuncKey] = "0.11",
            [ReproductionSettingsKeys.PrunePolicyKey] = "prune_random",
            [ReproductionSettingsKeys.PerRegionOutDegreeCapsKey] = "1:10.5,2:6.25",
            [ReproductionSettingsKeys.SpawnChildKey] = "spawn_child_never"
        };

        var config = ReproductionSettings.CreateConfigFromSettings(settings);

        Assert.Equal(0.42f, config.MaxRegionSpanDiffRatio, 3);
        Assert.Equal(0.19f, config.ProbMutate, 3);
        Assert.Equal(0.11f, config.ProbMutateFunc, 3);
        Assert.Equal(Repro.PrunePolicy.PruneRandom, config.PrunePolicy);
        Assert.Equal(Repro.SpawnChildPolicy.SpawnChildNever, config.SpawnChild);
        Assert.Collection(
            config.PerRegionOutDegreeCaps,
            first =>
            {
                Assert.Equal((uint)1, first.RegionId);
                Assert.Equal(10.5f, first.MaxAvgOutDegree, 3);
            },
            second =>
            {
                Assert.Equal((uint)2, second.RegionId);
                Assert.Equal(6.25f, second.MaxAvgOutDegree, 3);
            });
    }
}
