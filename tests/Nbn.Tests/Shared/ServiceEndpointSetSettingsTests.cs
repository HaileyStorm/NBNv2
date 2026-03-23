using Nbn.Runtime.SettingsMonitor;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.Shared;

public sealed class ServiceEndpointSetSettingsTests
{
    [Fact]
    public void EncodeSetValue_RoundTrips_Candidates()
    {
        var encoded = ServiceEndpointSettings.EncodeSetValue(
            "io-gateway",
            [
                new ServiceEndpointCandidate("100.86.45.90:12050", "io-gateway", ServiceEndpointCandidateKind.Tailnet, Priority: 100, Label: "tailnet", IsDefault: true),
                new ServiceEndpointCandidate("192.168.0.14:12050", "io-gateway", ServiceEndpointCandidateKind.Lan, Priority: 50, Label: "lan")
            ]);

        Assert.True(ServiceEndpointSettings.TryParseSetValue(encoded, out var parsed));
        Assert.Equal("io-gateway", parsed.ActorName);
        Assert.Equal(2, parsed.Candidates.Count);
        Assert.Equal("100.86.45.90:12050", parsed.GetPreferredCandidate().HostPort);
        Assert.Contains(parsed.Candidates, candidate => candidate.Kind == ServiceEndpointCandidateKind.Tailnet);
        Assert.Contains(parsed.Candidates, candidate => candidate.Kind == ServiceEndpointCandidateKind.Lan);
    }

    [Fact]
    public void TryParseSetSetting_ParsesKnownEndpointSetKey()
    {
        var value = ServiceEndpointSettings.EncodeSetValue(
            "HiveMind",
            [
                new ServiceEndpointCandidate("100.86.45.90:12020", "HiveMind", ServiceEndpointCandidateKind.Tailnet, Priority: 100, IsDefault: true)
            ]);

        var parsed = ServiceEndpointSettings.TryParseSetSetting(
            ServiceEndpointSettings.ToEndpointSetKey(ServiceEndpointSettings.HiveMindKey),
            value,
            updatedMs: 42,
            out var registration);

        Assert.True(parsed);
        Assert.Equal(ServiceEndpointSettings.ToEndpointSetKey(ServiceEndpointSettings.HiveMindKey), registration.Key);
        Assert.Equal("HiveMind", registration.EndpointSet.ActorName);
        Assert.Equal("100.86.45.90:12020", registration.EndpointSet.GetPreferredCandidate().HostPort);
    }

    [Fact]
    public async Task PublishSetAsync_PublishesSetAndLegacyPreferredEndpoint()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);
        var endpointSet = new ServiceEndpointSet(
            "io-gateway",
            [
                new ServiceEndpointCandidate("100.86.45.90:12050", "io-gateway", ServiceEndpointCandidateKind.Tailnet, Priority: 100, IsDefault: true),
                new ServiceEndpointCandidate("192.168.0.14:12050", "io-gateway", ServiceEndpointCandidateKind.Lan, Priority: 50)
            ]);

        await client.PublishSetAsync(ServiceEndpointSettings.IoGatewayKey, endpointSet);

        var setValue = await harness.Root.RequestAsync<Nbn.Proto.Settings.SettingValue>(
            harness.SettingsPid,
            new ProtoSettings.SettingGet { Key = ServiceEndpointSettings.ToEndpointSetKey(ServiceEndpointSettings.IoGatewayKey) });
        var legacyValue = await harness.Root.RequestAsync<ProtoSettings.SettingValue>(
            harness.SettingsPid,
            new ProtoSettings.SettingGet { Key = ServiceEndpointSettings.IoGatewayKey });

        Assert.True(ServiceEndpointSettings.TryParseSetValue(setValue.Value, out var parsedSet));
        Assert.Equal(2, parsedSet.Candidates.Count);
        Assert.Equal("100.86.45.90:12050", parsedSet.GetPreferredCandidate().HostPort);
        Assert.True(ServiceEndpointSettings.TryParseValue(legacyValue.Value, out var parsedLegacy));
        Assert.Equal("100.86.45.90:12050", parsedLegacy.HostPort);
    }

    [Fact]
    public async Task ResolveSetAsync_FallsBackToLegacySingleEndpoint()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);
        var endpoint = new ServiceEndpoint("127.0.0.1:12020", "HiveMind");

        await client.PublishAsync(ServiceEndpointSettings.HiveMindKey, endpoint);

        var resolved = await client.ResolveSetAsync(ServiceEndpointSettings.HiveMindKey);

        Assert.NotNull(resolved);
        Assert.Equal(ServiceEndpointSettings.ToEndpointSetKey(ServiceEndpointSettings.HiveMindKey), resolved!.Value.Key);
        Assert.Equal("HiveMind", resolved.Value.EndpointSet.ActorName);
        Assert.Single(resolved.Value.EndpointSet.Candidates);
        Assert.Equal(endpoint.HostPort, resolved.Value.EndpointSet.GetPreferredCandidate().HostPort);
        Assert.True(resolved.Value.EndpointSet.GetPreferredCandidate().IsDefault);
    }

    private sealed class SettingsMonitorHarness : IAsyncDisposable
    {
        private readonly TempDatabaseScope _databaseScope;

        private SettingsMonitorHarness(TempDatabaseScope databaseScope, ActorSystem system, PID settingsPid)
        {
            _databaseScope = databaseScope;
            System = system;
            SettingsPid = settingsPid;
        }

        public ActorSystem System { get; }

        public PID SettingsPid { get; }

        public IRootContext Root => System.Root;

        public static async Task<SettingsMonitorHarness> CreateAsync()
        {
            var databaseScope = new TempDatabaseScope();
            var store = new SettingsMonitorStore(databaseScope.DatabasePath);
            await store.InitializeAsync();

            var system = new ActorSystem();
            var settingsPid = system.Root.Spawn(Props.FromProducer(() => new SettingsMonitorActor(store)));
            return new SettingsMonitorHarness(databaseScope, system, settingsPid);
        }

        public async ValueTask DisposeAsync()
        {
            await System.ShutdownAsync();
            _databaseScope.Dispose();
        }
    }

    private sealed class TempDatabaseScope : IDisposable
    {
        private readonly string _directoryPath;

        public TempDatabaseScope()
        {
            _directoryPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directoryPath);
            DatabasePath = Path.Combine(_directoryPath, "settings-monitor.db");
        }

        public string DatabasePath { get; }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(_directoryPath))
                {
                    Directory.Delete(_directoryPath, recursive: true);
                }
            }
            catch
            {
            }
        }
    }
}
