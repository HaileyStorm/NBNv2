using Nbn.Shared;
using Proto;

namespace Nbn.Tests.Shared;

public sealed class ServiceEndpointSettingsTests
{
    [Fact]
    public void EncodeValue_TrimsSegmentsAndRoundTrips()
    {
        var encoded = ServiceEndpointSettings.EncodeValue(" 127.0.0.1:12020 ", " HiveMind ");

        Assert.Equal("127.0.0.1:12020/HiveMind", encoded);
        Assert.True(ServiceEndpointSettings.TryParseValue(encoded, out var endpoint));
        Assert.Equal("127.0.0.1:12020", endpoint.HostPort);
        Assert.Equal("HiveMind", endpoint.ActorName);

        var pid = endpoint.ToPid();
        Assert.Equal("127.0.0.1:12020", pid.Address);
        Assert.Equal("HiveMind", pid.Id);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("127.0.0.1:12020")]
    [InlineData("/HiveMind")]
    [InlineData("127.0.0.1:12020/")]
    public void TryParseValue_RejectsMalformedValues(string? value)
    {
        Assert.False(ServiceEndpointSettings.TryParseValue(value, out _));
    }

    [Fact]
    public void TryParseSetting_TrimsKeyAndClampsUpdatedMs()
    {
        var encoded = ServiceEndpointSettings.EncodeValue("127.0.0.1:12020", "HiveMind");

        var parsed = ServiceEndpointSettings.TryParseSetting(
            $" {ServiceEndpointSettings.HiveMindKey} ",
            encoded,
            ulong.MaxValue,
            out var registration);

        Assert.True(parsed);
        Assert.Equal(ServiceEndpointSettings.HiveMindKey, registration.Key);
        Assert.Equal("127.0.0.1:12020", registration.Endpoint.HostPort);
        Assert.Equal("HiveMind", registration.Endpoint.ActorName);
        Assert.Equal(long.MaxValue, registration.UpdatedMs);
    }

    [Fact]
    public void IsKnownKey_RequiresCanonicalKey()
    {
        Assert.True(ServiceEndpointSettings.IsKnownKey(ServiceEndpointSettings.HiveMindKey));
        Assert.True(ServiceEndpointSettings.IsKnownKey(ServiceEndpointSettings.PpoManagerKey));
        Assert.False(ServiceEndpointSettings.IsKnownKey(" service.endpoint.hivemind "));
        Assert.False(ServiceEndpointSettings.IsKnownKey("service.endpoint.unknown"));
        Assert.False(ServiceEndpointSettings.IsKnownKey(null));
    }

    [Fact]
    public void Create_ReturnsNull_WhenSettingsLocationIsIncomplete()
    {
        var system = new ActorSystem();

        Assert.Null(ServiceEndpointDiscoveryClient.Create(null, "127.0.0.1", 12010, "SettingsMonitor"));
        Assert.Null(ServiceEndpointDiscoveryClient.Create(system, null, 12010, "SettingsMonitor"));
        Assert.Null(ServiceEndpointDiscoveryClient.Create(system, "127.0.0.1", 0, "SettingsMonitor"));
        Assert.Null(ServiceEndpointDiscoveryClient.Create(system, "127.0.0.1", 12010, ""));
    }

    [Fact]
    public async Task TryPublishAsync_ReturnsFalse_WhenSettingsLocationIsIncomplete()
    {
        var published = await ServiceEndpointDiscoveryClient.TryPublishAsync(
            system: null,
            settingsHost: "127.0.0.1",
            settingsPort: 12010,
            settingsName: "SettingsMonitor",
            settingKey: ServiceEndpointSettings.HiveMindKey,
            endpoint: new ServiceEndpoint("127.0.0.1:12020", "HiveMind"));

        Assert.False(published);
    }
}
