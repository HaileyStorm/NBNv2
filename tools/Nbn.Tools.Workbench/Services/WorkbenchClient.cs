using System;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

namespace Nbn.Tools.Workbench.Services;

public sealed class WorkbenchClient : IAsyncDisposable
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);
    private readonly IWorkbenchEventSink _sink;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private ActorSystem? _system;
    private IRootContext? _root;
    private PID? _receiverPid;
    private PID? _ioGatewayPid;
    private PID? _debugHubPid;
    private PID? _vizHubPid;
    private PID? _settingsPid;
    private PID? _hiveMindPid;
    private string? _bindHost;
    private int _bindPort;

    public WorkbenchClient(IWorkbenchEventSink sink)
    {
        _sink = sink;
    }

    public bool IsRunning => _system is not null;

    public string ReceiverLabel => _receiverPid is null ? "offline" : PidLabel(_receiverPid);

    public async Task EnsureStartedAsync(string bindHost, int port, string? advertisedHost = null, int? advertisedPort = null)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_system is not null && string.Equals(_bindHost, bindHost, StringComparison.OrdinalIgnoreCase) && _bindPort == port)
            {
                return;
            }

            await StopAsync().ConfigureAwait(false);

            _bindHost = bindHost;
            _bindPort = port;

            var system = new ActorSystem();
            var remoteConfig = WorkbenchRemote.BuildConfig(bindHost, port, advertisedHost, advertisedPort);
            system.WithRemote(remoteConfig);
            await system.Remote().StartAsync().ConfigureAwait(false);

            var receiverPid = system.Root.SpawnNamed(
                Props.FromProducer(() => new WorkbenchReceiverActor(_sink)),
                "workbench-receiver");

            _system = system;
            _root = system.Root;
            _receiverPid = receiverPid;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<ConnectAck?> ConnectIoAsync(string host, int port, string gatewayName, string clientName)
    {
        if (_root is null)
        {
            return null;
        }

        var pid = new PID($"{host}:{port}", gatewayName);
        try
        {
            var ack = await _root.RequestAsync<ConnectAck>(pid, new Connect { ClientName = clientName }, DefaultTimeout)
                .ConfigureAwait(false);
            _ioGatewayPid = pid;
            if (_receiverPid is not null)
            {
                _root.Send(_receiverPid, new SetIoGatewayPid(pid));
            }

            _sink.OnIoStatus($"Connected to {host}:{port}", true);
            return ack;
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"IO connect failed: {ex.Message}", false);
            return null;
        }
    }

    public void DisconnectIo()
    {
        _ioGatewayPid = null;
        if (_receiverPid is not null)
        {
            _root?.Send(_receiverPid, new SetIoGatewayPid(null));
        }

        _sink.OnIoStatus("Disconnected", false);
    }

    public async Task<bool> ConnectSettingsAsync(string host, int port, string actorName, bool verify = false)
    {
        if (_root is null || _receiverPid is null)
        {
            _sink.OnSettingsStatus("Settings client not initialized.", false);
            return false;
        }

        _settingsPid = new PID($"{host}:{port}", actorName);
        var subscriber = PidLabel(_receiverPid);
        _root.Send(_settingsPid, new SettingSubscribe { SubscriberActor = subscriber });

        if (!verify)
        {
            _sink.OnSettingsStatus($"Subscribed to {host}:{port}", true);
            return true;
        }

        try
        {
            await _root.RequestAsync<NodeListResponse>(_settingsPid, new NodeListRequest(), DefaultTimeout)
                .ConfigureAwait(false);
            _sink.OnSettingsStatus($"Connected to {host}:{port}", true);
            return true;
        }
        catch (Exception ex)
        {
            _settingsPid = null;
            _sink.OnSettingsStatus($"Settings connect failed: {ex.Message}", false);
            return false;
        }
    }

    public async Task<Nbn.Proto.Control.HiveMindStatus?> ConnectHiveMindAsync(string host, int port, string actorName)
    {
        if (_root is null)
        {
            return null;
        }

        var pid = new PID($"{host}:{port}", actorName);
        try
        {
            var status = await _root.RequestAsync<Nbn.Proto.Control.HiveMindStatus>(
                    pid,
                    new Nbn.Proto.Control.GetHiveMindStatus(),
                    DefaultTimeout)
                .ConfigureAwait(false);
            _hiveMindPid = pid;
            _sink.OnHiveMindStatus($"Connected to {host}:{port}", true);
            return status;
        }
        catch (Exception ex)
        {
            _sink.OnHiveMindStatus($"HiveMind connect failed: {ex.Message}", false);
            return null;
        }
    }

    public void DisconnectHiveMind()
    {
        _hiveMindPid = null;
        _sink.OnHiveMindStatus("Disconnected", false);
    }

    public void DisconnectSettings()
    {
        if (_root is null || _receiverPid is null)
        {
            _settingsPid = null;
            return;
        }

        if (_settingsPid is not null)
        {
            _root.Send(_settingsPid, new SettingUnsubscribe { SubscriberActor = PidLabel(_receiverPid) });
        }

        _settingsPid = null;
        _sink.OnSettingsStatus("Disconnected", false);
    }

    public async Task<SettingValue?> GetSettingAsync(string key)
    {
        if (_root is null || _settingsPid is null || string.IsNullOrWhiteSpace(key))
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<SettingValue>(
                _settingsPid,
                new SettingGet { Key = key },
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnSettingsStatus($"Setting get failed: {ex.Message}", true);
            return null;
        }
    }

    public async Task<SettingListResponse?> ListSettingsAsync()
    {
        if (_root is null || _settingsPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<SettingListResponse>(
                _settingsPid,
                new SettingListRequest(),
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnSettingsStatus($"Setting list failed: {ex.Message}", true);
            return null;
        }
    }

    public async Task<SettingValue?> SetSettingAsync(string key, string value)
    {
        if (_root is null || _settingsPid is null || string.IsNullOrWhiteSpace(key))
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<SettingValue>(
                _settingsPid,
                new SettingSet { Key = key, Value = value },
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnSettingsStatus($"Setting set failed: {ex.Message}", true);
            return null;
        }
    }

    public async Task<NodeListResponse?> ListNodesAsync()
    {
        if (_root is null || _settingsPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<NodeListResponse>(
                _settingsPid,
                new NodeListRequest(),
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnSettingsStatus($"Node list failed: {ex.Message}", true);
            return null;
        }
    }

    public async Task<BrainListResponse?> ListBrainsAsync()
    {
        if (_root is null || _settingsPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<BrainListResponse>(
                _settingsPid,
                new BrainListRequest(),
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnSettingsStatus($"Brain list failed: {ex.Message}", true);
            return null;
        }
    }

    public Task ConnectObservabilityAsync(string host, int port, string debugHub, string vizHub, Nbn.Proto.Severity minSeverity, string contextRegex)
    {
        if (_root is null || _receiverPid is null)
        {
            return Task.CompletedTask;
        }

        _debugHubPid = new PID($"{host}:{port}", debugHub);
        _vizHubPid = new PID($"{host}:{port}", vizHub);

        var subscriber = PidLabel(_receiverPid);
        _root.Send(_debugHubPid, new DebugSubscribe
        {
            SubscriberActor = subscriber,
            MinSeverity = minSeverity,
            ContextRegex = contextRegex ?? string.Empty
        });

        _root.Send(_vizHubPid, new VizSubscribe
        {
            SubscriberActor = subscriber
        });

        _sink.OnObsStatus($"Subscribed to {host}:{port}", true);
        return Task.CompletedTask;
    }

    public void DisconnectObservability(string? contextRegex = null)
    {
        if (_root is null || _receiverPid is null)
        {
            return;
        }

        var subscriber = PidLabel(_receiverPid);
        if (_debugHubPid is not null)
        {
            _root.Send(_debugHubPid, new DebugUnsubscribe { SubscriberActor = subscriber });
        }

        if (_vizHubPid is not null)
        {
            _root.Send(_vizHubPid, new VizUnsubscribe { SubscriberActor = subscriber });
        }

        _sink.OnObsStatus("Disconnected", false);
    }

    public Task RefreshDebugFilterAsync(Nbn.Proto.Severity minSeverity, string contextRegex)
    {
        if (_root is null || _receiverPid is null || _debugHubPid is null)
        {
            return Task.CompletedTask;
        }

        _root.Send(_debugHubPid, new DebugSubscribe
        {
            SubscriberActor = PidLabel(_receiverPid),
            MinSeverity = minSeverity,
            ContextRegex = contextRegex ?? string.Empty
        });

        return Task.CompletedTask;
    }

    public Task RequestBrainInfoAsync(Guid brainId, Action<BrainInfo?> callback)
    {
        if (_root is null || _ioGatewayPid is null)
        {
            callback(null);
            return Task.CompletedTask;
        }

        return RequestBrainInfoInternalAsync(brainId, callback);
    }

    private async Task RequestBrainInfoInternalAsync(Guid brainId, Action<BrainInfo?> callback)
    {
        if (_root is null || _ioGatewayPid is null)
        {
            callback(null);
            return;
        }

        try
        {
            var info = await _root.RequestAsync<BrainInfo>(
                _ioGatewayPid,
                new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
                DefaultTimeout);
            callback(info);
        }
        catch (Exception)
        {
            callback(null);
        }
    }

    public void SubscribeOutputs(Guid brainId, bool vector)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, vector
            ? new SubscribeOutputsVectorCommand(brainId)
            : new SubscribeOutputsCommand(brainId));
    }

    public void UnsubscribeOutputs(Guid brainId, bool vector)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, vector
            ? new UnsubscribeOutputsVectorCommand(brainId)
            : new UnsubscribeOutputsCommand(brainId));
    }

    public void SendInput(Guid brainId, uint index, float value)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new InputWriteCommand(brainId, index, value));
    }

    public void SendInputVector(Guid brainId, IReadOnlyList<float> values)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new InputVectorCommand(brainId, values));
    }

    public void SendEnergyCredit(Guid brainId, long amount)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new EnergyCreditCommand(brainId, amount));
    }

    public void SendEnergyRate(Guid brainId, long unitsPerSecond)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new EnergyRateCommand(brainId, unitsPerSecond));
    }

    public void SetCostEnergy(Guid brainId, bool costEnabled, bool energyEnabled)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new SetCostEnergyCommand(brainId, costEnabled, energyEnabled));
    }

    public void SetPlasticity(Guid brainId, bool enabled, float rate, bool probabilistic)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new SetPlasticityCommand(brainId, enabled, rate, probabilistic));
    }

    public async Task<Nbn.Proto.Repro.ReproduceResult?> ReproduceByBrainIdsAsync(ReproduceByBrainIdsRequest request)
    {
        if (_root is null || _ioGatewayPid is null)
        {
            return null;
        }

        try
        {
            var result = await _root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
                    _ioGatewayPid,
                    new ReproduceByBrainIds { Request = request },
                    DefaultTimeout)
                .ConfigureAwait(false);
            return result?.Result;
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"Repro failed: {ex.Message}", false);
            return null;
        }
    }

    private async Task StopAsync()
    {
        if (_system is null)
        {
            return;
        }

        try
        {
            if (_system.Remote() is not null)
            {
                await _system.Remote().ShutdownAsync(true).ConfigureAwait(false);
            }

            await _system.ShutdownAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
        }
        finally
        {
            _system = null;
            _root = null;
            _receiverPid = null;
            _ioGatewayPid = null;
            _debugHubPid = null;
            _vizHubPid = null;
            _settingsPid = null;
            _hiveMindPid = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _gate.Dispose();
    }

    private string PidLabel(PID pid)
    {
        var address = pid.Address;
        if (string.IsNullOrWhiteSpace(address))
        {
            address = _system?.Address ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(address) && !string.IsNullOrWhiteSpace(_bindHost) && _bindPort > 0)
        {
            address = $"{_bindHost}:{_bindPort}";
        }

        return string.IsNullOrWhiteSpace(address) ? pid.Id : $"{address}/{pid.Id}";
    }
}
