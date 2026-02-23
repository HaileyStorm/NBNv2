using Nbn.Runtime.SettingsMonitor;
using Nbn.Shared;
using Proto;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tests.SettingsMonitor;

public sealed class ServiceEndpointDiscoveryClientTests
{
    [Fact]
    public async Task PublishAsync_PublishesCanonicalHiveMindEndpointKey()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);

        var endpoint = new ServiceEndpoint("127.0.0.1:12020", "HiveMind");
        await client.PublishAsync(ServiceEndpointSettings.HiveMindKey, endpoint);

        var stored = await harness.Root.RequestAsync<ProtoSettings.SettingValue>(
            harness.SettingsPid,
            new ProtoSettings.SettingGet
            {
                Key = ServiceEndpointSettings.HiveMindKey
            });

        Assert.Equal(ServiceEndpointSettings.HiveMindKey, stored.Key);
        Assert.Equal(endpoint.ToSettingValue(), stored.Value);
        Assert.True(ServiceEndpointSettings.TryParseValue(stored.Value, out var parsed));
        Assert.Equal(endpoint, parsed);

        await client.DisposeAsync();
    }

    [Fact]
    public async Task ResolveMethods_ReturnPublishedEndpointsViaListAndGet()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);

        var hiveMindEndpoint = new ServiceEndpoint("127.0.0.1:12020", "HiveMind");
        var ioEndpoint = new ServiceEndpoint("127.0.0.1:12050", "io-gateway");

        await client.PublishAsync(ServiceEndpointSettings.HiveMindKey, hiveMindEndpoint);
        await client.PublishAsync(ServiceEndpointSettings.IoGatewayKey, ioEndpoint);

        var resolvedFromList = await client.ResolveFromListAsync();
        Assert.Equal(hiveMindEndpoint, resolvedFromList[ServiceEndpointSettings.HiveMindKey].Endpoint);
        Assert.Equal(ioEndpoint, resolvedFromList[ServiceEndpointSettings.IoGatewayKey].Endpoint);

        var resolvedFromGet = await client.ResolveAsync(ServiceEndpointSettings.HiveMindKey);
        Assert.NotNull(resolvedFromGet);
        Assert.Equal(hiveMindEndpoint, resolvedFromGet!.Value.Endpoint);

        var resolvedKnown = await client.ResolveKnownAsync();
        Assert.Equal(2, resolvedKnown.Count);

        await client.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_EmitsEndpointChangedWhenEndpointIsUpdated()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);

        var key = ServiceEndpointSettings.IoGatewayKey;
        await client.PublishAsync(key, new ServiceEndpoint("127.0.0.1:12050", "io-gateway"));

        var updatedEndpoint = new ServiceEndpoint("127.0.0.1:12051", "io-gateway");
        var changeTask = new TaskCompletionSource<ServiceEndpointRegistration>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.EndpointChanged += registration =>
        {
            if (registration.Key == key && registration.Endpoint == updatedEndpoint)
            {
                changeTask.TrySetResult(registration);
            }
        };

        await client.SubscribeAsync([key]);
        await Task.Delay(50);
        await client.PublishAsync(key, updatedEndpoint);

        var update = await changeTask.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(key, update.Key);
        Assert.Equal(updatedEndpoint, update.Endpoint);
        Assert.True(update.UpdatedMs > 0);

        await client.UnsubscribeAsync();
        await client.DisposeAsync();
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
