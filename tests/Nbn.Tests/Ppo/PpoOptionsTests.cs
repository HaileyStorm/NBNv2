using Nbn.Runtime.Ppo;

namespace Nbn.Tests.Ppo;

public sealed class PpoOptionsTests
{
    [Fact]
    public void FromArgs_UsesCoreOptionalServiceDefaults()
    {
        var options = PpoOptions.FromArgs(Array.Empty<string>());

        Assert.Equal(Nbn.Shared.NetworkAddressDefaults.DefaultBindHost, options.BindHost);
        Assert.Equal(12090, options.Port);
        Assert.Equal(PpoNames.Manager, options.ManagerName);
        Assert.Equal("nbn.ppo", options.ServiceName);
        Assert.Equal("127.0.0.1", options.SettingsHost);
        Assert.Equal(12010, options.SettingsPort);
        Assert.Equal("SettingsMonitor", options.SettingsName);
        Assert.Null(options.IoAddress);
        Assert.Equal("IoGateway", options.IoName);
        Assert.Null(options.ReproductionAddress);
        Assert.Equal("ReproductionManager", options.ReproductionName);
        Assert.Null(options.SpeciationAddress);
        Assert.Equal("SpeciationManager", options.SpeciationName);
    }

    [Fact]
    public void FromArgs_ParsesDependencyAndNetworkOptions()
    {
        var options = PpoOptions.FromArgs(
        [
            "--bind-host", "0.0.0.0",
            "--port", "13090",
            "--advertise-host", "10.20.30.40",
            "--advertise-port", "23090",
            "--manager-name", "ppo-custom",
            "--server-name", "nbn.ppo.tests",
            "--settings-host", "10.20.30.10",
            "--settings-port", "13010",
            "--settings-name", "settings-custom",
            "--io-address", "10.20.30.19:12050",
            "--io-name", "io-custom",
            "--repro-address", "10.20.30.20:12070",
            "--repro-name", "repro-custom",
            "--speciation-address", "10.20.30.21:12080",
            "--speciation-name", "spec-custom"
        ]);

        Assert.Equal("0.0.0.0", options.BindHost);
        Assert.Equal(13090, options.Port);
        Assert.Equal("10.20.30.40", options.AdvertisedHost);
        Assert.Equal(23090, options.AdvertisedPort);
        Assert.Equal("ppo-custom", options.ManagerName);
        Assert.Equal("nbn.ppo.tests", options.ServiceName);
        Assert.Equal("10.20.30.10", options.SettingsHost);
        Assert.Equal(13010, options.SettingsPort);
        Assert.Equal("settings-custom", options.SettingsName);
        Assert.Equal("10.20.30.19:12050", options.IoAddress);
        Assert.Equal("io-custom", options.IoName);
        Assert.Equal("10.20.30.20:12070", options.ReproductionAddress);
        Assert.Equal("repro-custom", options.ReproductionName);
        Assert.Equal("10.20.30.21:12080", options.SpeciationAddress);
        Assert.Equal("spec-custom", options.SpeciationName);
    }
}
