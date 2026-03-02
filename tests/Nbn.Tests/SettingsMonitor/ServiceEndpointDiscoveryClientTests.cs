using System.Net;
using System.Net.Sockets;
using Nbn.Proto;
using Nbn.Proto.Settings;
using Nbn.Runtime.SettingsMonitor;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
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
        var reproEndpoint = new ServiceEndpoint("127.0.0.1:12070", "ReproductionManager");
        var workerEndpoint = new ServiceEndpoint("127.0.0.1:12041", "worker-node");
        var obsEndpoint = new ServiceEndpoint("127.0.0.1:12060", "DebugHub");

        await client.PublishAsync(ServiceEndpointSettings.HiveMindKey, hiveMindEndpoint);
        await client.PublishAsync(ServiceEndpointSettings.IoGatewayKey, ioEndpoint);
        await client.PublishAsync(ServiceEndpointSettings.ReproductionManagerKey, reproEndpoint);
        await client.PublishAsync(ServiceEndpointSettings.WorkerNodeKey, workerEndpoint);
        await client.PublishAsync(ServiceEndpointSettings.ObservabilityKey, obsEndpoint);

        var resolvedFromList = await client.ResolveFromListAsync();
        Assert.Equal(hiveMindEndpoint, resolvedFromList[ServiceEndpointSettings.HiveMindKey].Endpoint);
        Assert.Equal(ioEndpoint, resolvedFromList[ServiceEndpointSettings.IoGatewayKey].Endpoint);
        Assert.Equal(reproEndpoint, resolvedFromList[ServiceEndpointSettings.ReproductionManagerKey].Endpoint);
        Assert.Equal(workerEndpoint, resolvedFromList[ServiceEndpointSettings.WorkerNodeKey].Endpoint);
        Assert.Equal(obsEndpoint, resolvedFromList[ServiceEndpointSettings.ObservabilityKey].Endpoint);

        var resolvedFromGet = await client.ResolveAsync(ServiceEndpointSettings.HiveMindKey);
        Assert.NotNull(resolvedFromGet);
        Assert.Equal(hiveMindEndpoint, resolvedFromGet!.Value.Endpoint);

        var resolvedKnown = await client.ResolveKnownAsync();
        Assert.Equal(ServiceEndpointSettings.AllKeys.Count, resolvedKnown.Count);
        Assert.Equal(reproEndpoint, resolvedKnown[ServiceEndpointSettings.ReproductionManagerKey].Endpoint);
        Assert.Equal(workerEndpoint, resolvedKnown[ServiceEndpointSettings.WorkerNodeKey].Endpoint);
        Assert.Equal(obsEndpoint, resolvedKnown[ServiceEndpointSettings.ObservabilityKey].Endpoint);

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

    [Fact]
    public async Task SubscribeAsync_EmitsEndpointObservedWhenWatchedKeyIsRemoved()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);

        var key = ServiceEndpointSettings.IoGatewayKey;
        await client.PublishAsync(key, new ServiceEndpoint("127.0.0.1:12050", "io-gateway"));

        var changedTriggered = false;
        var observedTask = new TaskCompletionSource<ServiceEndpointObservation>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.EndpointChanged += _ => changedTriggered = true;
        client.EndpointObserved += observation =>
        {
            if (observation.Key == key && observation.Kind == ServiceEndpointObservationKind.Removed)
            {
                observedTask.TrySetResult(observation);
            }
        };

        await client.SubscribeAsync([key]);
        await Task.Delay(50);
        await harness.Root.RequestAsync<ProtoSettings.SettingValue>(
            harness.SettingsPid,
            new ProtoSettings.SettingSet
            {
                Key = key,
                Value = string.Empty
            });

        var observation = await observedTask.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(key, observation.Key);
        Assert.Equal(ServiceEndpointObservationKind.Removed, observation.Kind);
        Assert.Null(observation.Registration);
        Assert.Equal("endpoint_removed", observation.FailureReason);
        Assert.True(observation.UpdatedMs > 0);
        Assert.False(changedTriggered);

        await client.UnsubscribeAsync();
        await client.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_EmitsEndpointObservedWhenWatchedKeyValueIsInvalid()
    {
        await using var harness = await SettingsMonitorHarness.CreateAsync();
        var client = new ServiceEndpointDiscoveryClient(harness.System, harness.SettingsPid);

        var key = ServiceEndpointSettings.IoGatewayKey;
        await client.PublishAsync(key, new ServiceEndpoint("127.0.0.1:12050", "io-gateway"));

        var changedTriggered = false;
        var observedTask = new TaskCompletionSource<ServiceEndpointObservation>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.EndpointChanged += _ => changedTriggered = true;
        client.EndpointObserved += observation =>
        {
            if (observation.Key == key && observation.Kind == ServiceEndpointObservationKind.Invalid)
            {
                observedTask.TrySetResult(observation);
            }
        };

        await client.SubscribeAsync([key]);
        await Task.Delay(50);
        await harness.Root.RequestAsync<ProtoSettings.SettingValue>(
            harness.SettingsPid,
            new ProtoSettings.SettingSet
            {
                Key = key,
                Value = "127.0.0.1:12050"
            });

        var observation = await observedTask.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(key, observation.Key);
        Assert.Equal(ServiceEndpointObservationKind.Invalid, observation.Kind);
        Assert.Null(observation.Registration);
        Assert.Equal("endpoint_parse_failed", observation.FailureReason);
        Assert.True(observation.UpdatedMs > 0);
        Assert.False(changedTriggered);

        await client.UnsubscribeAsync();
        await client.DisposeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_RemoteSettingsMonitor_DeliversEndpointChangedUpdates()
    {
        using var databaseScope = new TempDatabaseScope();
        var store = new SettingsMonitorStore(databaseScope.DatabasePath);
        await store.InitializeAsync();

        var settingsPort = GetFreePort();
        var subscriberPort = GetFreePort();
        var settingsSystem = new ActorSystem();
        var settingsConfig = RemoteConfig.BindToLocalhost(settingsPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnSettingsReflection.Descriptor);
        settingsSystem.WithRemote(settingsConfig);
        await settingsSystem.Remote().StartAsync();
        var settingsPid = settingsSystem.Root.SpawnNamed(
            Props.FromProducer(() => new SettingsMonitorActor(store)),
            "SettingsMonitor");

        var subscriberSystem = new ActorSystem();
        var subscriberConfig = RemoteConfig.BindToLocalhost(subscriberPort).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnSettingsReflection.Descriptor);
        subscriberSystem.WithRemote(subscriberConfig);
        await subscriberSystem.Remote().StartAsync();

        var client = new ServiceEndpointDiscoveryClient(
            subscriberSystem,
            new PID($"127.0.0.1:{settingsPort}", "SettingsMonitor"));

        try
        {
            var key = ServiceEndpointSettings.IoGatewayKey;
            var initialEndpoint = new ServiceEndpoint("127.0.0.1:12050", "io-gateway");
            var updatedEndpoint = new ServiceEndpoint("127.0.0.1:12051", "io-gateway");
            await client.PublishAsync(key, initialEndpoint);

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

            await settingsSystem.Root.RequestAsync<ProtoSettings.SettingValue>(
                settingsPid,
                new ProtoSettings.SettingSet
                {
                    Key = key,
                    Value = updatedEndpoint.ToSettingValue()
                });

            var changed = await changeTask.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(key, changed.Key);
            Assert.Equal(updatedEndpoint, changed.Endpoint);
        }
        finally
        {
            await client.DisposeAsync();
            await subscriberSystem.Remote().ShutdownAsync(true);
            await subscriberSystem.ShutdownAsync();
            await settingsSystem.Remote().ShutdownAsync(true);
            await settingsSystem.ShutdownAsync();
        }
    }

    [Fact]
    public async Task SubscribeAsync_UsesRoutableSubscriberActor_WhenSystemHasRemoteAddress()
    {
        var port = GetFreePort();
        var system = new ActorSystem();
        var remoteConfig = RemoteConfig.BindToLocalhost(port).WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnSettingsReflection.Descriptor);
        system.WithRemote(remoteConfig);
        await system.Remote().StartAsync();

        var subscribeLabel = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var settingsPid = system.Root.Spawn(Props.FromProducer(() => new CaptureSubscribeActor(subscribeLabel)));
        var client = new ServiceEndpointDiscoveryClient(system, settingsPid);

        try
        {
            await client.SubscribeAsync([ServiceEndpointSettings.HiveMindKey]);
            var label = await subscribeLabel.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Contains("/", label, StringComparison.Ordinal);
            Assert.False(string.IsNullOrWhiteSpace(system.Address));
            Assert.StartsWith($"{system.Address}/", label, StringComparison.Ordinal);
        }
        finally
        {
            await client.DisposeAsync();
            await system.Remote().ShutdownAsync(true);
            await system.ShutdownAsync();
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
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

    private sealed class CaptureSubscribeActor : IActor
    {
        private readonly TaskCompletionSource<string> _subscribeLabel;

        public CaptureSubscribeActor(TaskCompletionSource<string> subscribeLabel)
        {
            _subscribeLabel = subscribeLabel;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoSettings.SettingGet get:
                    context.Respond(new ProtoSettings.SettingValue
                    {
                        Key = get.Key ?? string.Empty,
                        Value = string.Empty,
                        UpdatedMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    });
                    break;
                case ProtoSettings.SettingSubscribe subscribe:
                    _subscribeLabel.TrySetResult(subscribe.SubscriberActor ?? string.Empty);
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
