using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Proto.Control;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

namespace Nbn.Tools.Workbench.Services;

public class WorkbenchClient : IAsyncDisposable
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan SpawnRequestTimeout = TimeSpan.FromSeconds(70);
    private static readonly TimeSpan ReproRequestTimeout = TimeSpan.FromSeconds(45);
    private static readonly bool LogVizDiagnostics = IsEnvTrue("NBN_VIZ_DIAGNOSTICS_ENABLED");
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
    private bool _debugSubscribed;
    private bool _vizSubscribed;
    private Guid? _vizBrainEnabled;
    private uint? _vizFocusRegionId;
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
            if (_vizBrainEnabled.HasValue)
            {
                _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: true, _vizFocusRegionId));
            }
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
        if (_root is not null && _hiveMindPid is not null && _vizBrainEnabled.HasValue)
        {
            _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: false));
        }

        _vizBrainEnabled = null;
        _vizFocusRegionId = null;
        _hiveMindPid = null;
        _sink.OnHiveMindStatus("Disconnected", false);
    }

    public virtual async Task<PlacementAck?> RequestPlacementAsync(RequestPlacement request)
    {
        if (_root is null || _hiveMindPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<PlacementAck>(_hiveMindPid, request, DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnHiveMindStatus($"Placement request failed: {ex.Message}", true);
            return null;
        }
    }

    public virtual async Task<PlacementLifecycleInfo?> GetPlacementLifecycleAsync(Guid brainId)
    {
        if (_root is null || _hiveMindPid is null || brainId == Guid.Empty)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<PlacementLifecycleInfo>(
                    _hiveMindPid,
                    new GetPlacementLifecycle { BrainId = brainId.ToProtoUuid() },
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch
        {
            return null;
        }
    }

    public virtual async Task<PlacementReconcileReport?> RequestPlacementReconcileAsync(
        string workerAddress,
        string workerRootActor,
        Guid brainId,
        ulong placementEpoch)
    {
        if (_root is null
            || brainId == Guid.Empty
            || placementEpoch == 0
            || string.IsNullOrWhiteSpace(workerAddress)
            || string.IsNullOrWhiteSpace(workerRootActor))
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<PlacementReconcileReport>(
                    new PID(workerAddress.Trim(), workerRootActor.Trim()),
                    new PlacementReconcileRequest
                    {
                        BrainId = brainId.ToProtoUuid(),
                        PlacementEpoch = placementEpoch
                    },
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch
        {
            return null;
        }
    }

    public virtual async Task<SpawnBrainAck?> SpawnBrainViaIoAsync(SpawnBrain request)
    {
        if (_root is null)
        {
            return BuildSpawnFailureAck(
                reasonCode: "spawn_unavailable",
                failureMessage: "Spawn failed: Workbench client is not initialized.");
        }

        if (_ioGatewayPid is null)
        {
            return BuildSpawnFailureAck(
                reasonCode: "spawn_unavailable",
                failureMessage: "Spawn failed: IO gateway is not connected.");
        }

        if (request is null)
        {
            return BuildSpawnFailureAck(
                reasonCode: "spawn_invalid_request",
                failureMessage: "Spawn request rejected: request payload was null.");
        }

        try
        {
            var response = await _root.RequestAsync<SpawnBrainViaIOAck>(
                    _ioGatewayPid,
                    new SpawnBrainViaIO { Request = request },
                    SpawnRequestTimeout)
                .ConfigureAwait(false);

            if (response?.Ack is not null)
            {
                return response.Ack;
            }

            if (!string.IsNullOrWhiteSpace(response?.FailureReasonCode)
                || !string.IsNullOrWhiteSpace(response?.FailureMessage))
            {
                return BuildSpawnFailureAck(
                    reasonCode: response?.FailureReasonCode,
                    failureMessage: response?.FailureMessage);
            }

            return BuildSpawnFailureAck(
                reasonCode: "spawn_empty_response",
                failureMessage: "Spawn failed: IO returned an empty spawn acknowledgment.");
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"Spawn request failed: {ex.Message}", true);
            return BuildSpawnFailureAck(
                reasonCode: "spawn_request_failed",
                failureMessage: $"Spawn failed: request to IO gateway failed ({ex.GetBaseException().Message}).");
        }
    }

    private static SpawnBrainAck BuildSpawnFailureAck(string? reasonCode, string? failureMessage)
    {
        var normalizedReasonCode = string.IsNullOrWhiteSpace(reasonCode)
            ? "spawn_failed"
            : reasonCode.Trim();
        var normalizedFailureMessage = string.IsNullOrWhiteSpace(failureMessage)
            ? "Spawn failed: IO did not return a valid spawn acknowledgment."
            : failureMessage.Trim();
        return new SpawnBrainAck
        {
            BrainId = Guid.Empty.ToProtoUuid(),
            FailureReasonCode = normalizedReasonCode,
            FailureMessage = normalizedFailureMessage
        };
    }

    public virtual Task<bool> KillBrainAsync(Guid brainId, string reason)
    {
        if (_root is null || _hiveMindPid is null || brainId == Guid.Empty)
        {
            return Task.FromResult(false);
        }

        try
        {
            _root.Send(_hiveMindPid, new KillBrain
            {
                BrainId = brainId.ToProtoUuid(),
                Reason = reason ?? string.Empty
            });
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            _sink.OnHiveMindStatus($"Kill request failed: {ex.Message}", true);
            return Task.FromResult(false);
        }
    }

    public virtual async Task<HiveMindStatus?> GetHiveMindStatusAsync()
    {
        if (_root is null || _hiveMindPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<HiveMindStatus>(
                    _hiveMindPid,
                    new GetHiveMindStatus(),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnHiveMindStatus($"HiveMind status request failed: {ex.Message}", true);
            return null;
        }
    }

    public async Task<SetTickRateOverrideAck?> SetTickRateOverrideAsync(float? targetTickHz)
    {
        if (_root is null || _hiveMindPid is null)
        {
            return null;
        }

        var request = targetTickHz.HasValue
            ? new SetTickRateOverride { TargetTickHz = targetTickHz.Value }
            : new SetTickRateOverride { ClearOverride = true };

        try
        {
            return await _root.RequestAsync<SetTickRateOverrideAck>(
                    _hiveMindPid,
                    request,
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnHiveMindStatus($"Tick override request failed: {ex.Message}", true);
            return null;
        }
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

    public virtual async Task<SettingListResponse?> ListSettingsAsync()
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

    public virtual async Task<SettingValue?> SetSettingAsync(string key, string value)
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

    public virtual async Task<NodeListResponse?> ListNodesAsync()
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

    public virtual async Task<WorkerInventorySnapshotResponse?> ListWorkerInventorySnapshotAsync()
    {
        if (_root is null || _settingsPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<WorkerInventorySnapshotResponse>(
                _settingsPid,
                new WorkerInventorySnapshotRequest(),
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _sink.OnSettingsStatus($"Worker inventory failed: {ex.Message}", true);
            return null;
        }
    }

    public virtual async Task<BrainListResponse?> ListBrainsAsync()
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

    public async Task<bool> ConnectObservabilityAsync(string host, int port, string debugHub, string vizHub, Nbn.Proto.Severity minSeverity, string contextRegex)
    {
        if (_root is null || _receiverPid is null)
        {
            _sink.OnObsStatus("Observability client not initialized.", false);
            return false;
        }

        _ = minSeverity;
        _ = contextRegex;

        _debugHubPid = new PID($"{host}:{port}", debugHub);
        _vizHubPid = new PID($"{host}:{port}", vizHub);

        _debugSubscribed = false;
        _vizSubscribed = false;
        var reachable = await IsEndpointReachableAsync(host, port, TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        _sink.OnObsStatus(
            reachable
                ? $"Connected to {host}:{port}"
                : $"Obs endpoint unreachable: {host}:{port}",
            reachable);
        return reachable;
    }

    public void SetDebugSubscription(bool enabled, DebugSubscriptionFilter filter)
    {
        if (_root is null || _receiverPid is null || _debugHubPid is null)
        {
            return;
        }

        filter ??= DebugSubscriptionFilter.Default;
        var subscriber = PidLabel(_receiverPid);
        if (enabled)
        {
            _root.Send(_debugHubPid, BuildDebugSubscribe(subscriber, filter));
            _debugSubscribed = true;
            if (WorkbenchLog.Enabled)
            {
                WorkbenchLog.Info(
                    $"DebugSub enabled=true subscriber={subscriber} hub={PidLabel(_debugHubPid)} min={filter.MinSeverity} regex={filter.ContextRegex}");
            }
            return;
        }

        if (!_debugSubscribed)
        {
            return;
        }

        _root.Send(_debugHubPid, new DebugUnsubscribe { SubscriberActor = subscriber });
        _debugSubscribed = false;
        if (WorkbenchLog.Enabled)
        {
            WorkbenchLog.Info($"DebugSub enabled=false subscriber={subscriber} hub={PidLabel(_debugHubPid)}");
        }
    }

    public void SetVizSubscription(bool enabled)
    {
        if (_root is null || _receiverPid is null || _vizHubPid is null)
        {
            return;
        }

        var subscriber = PidLabel(_receiverPid);
        if (enabled)
        {
            if (_vizSubscribed)
            {
                return;
            }

            _root.Send(_vizHubPid, new VizSubscribe { SubscriberActor = subscriber });
            _vizSubscribed = true;
            if (LogVizDiagnostics && WorkbenchLog.Enabled)
            {
                WorkbenchLog.Info($"VizSub enabled=true subscriber={subscriber} hub={PidLabel(_vizHubPid)}");
            }
            return;
        }

        if (!_vizSubscribed)
        {
            return;
        }

        _root.Send(_vizHubPid, new VizUnsubscribe { SubscriberActor = subscriber });
        _vizSubscribed = false;
        if (LogVizDiagnostics && WorkbenchLog.Enabled)
        {
            WorkbenchLog.Info($"VizSub enabled=false subscriber={subscriber} hub={PidLabel(_vizHubPid)}");
        }
    }

    public void SetActiveVisualizationBrain(Guid? brainId, uint? focusRegionId)
    {
        if (_vizBrainEnabled == brainId && _vizFocusRegionId == focusRegionId)
        {
            return;
        }

        if (_root is null || _hiveMindPid is null)
        {
            _vizBrainEnabled = brainId;
            _vizFocusRegionId = focusRegionId;
            return;
        }

        if (_vizBrainEnabled.HasValue && _vizBrainEnabled.Value != brainId)
        {
            _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: false));
        }

        _vizBrainEnabled = brainId;
        _vizFocusRegionId = focusRegionId;

        if (brainId.HasValue)
        {
            _root.Send(_hiveMindPid, BuildVisualizationRequest(brainId.Value, enabled: true, focusRegionId));
        }

        if (LogVizDiagnostics && WorkbenchLog.Enabled)
        {
            var brainLabel = brainId.HasValue ? brainId.Value.ToString("D") : "none";
            var focusLabel = focusRegionId.HasValue ? focusRegionId.Value.ToString() : "all";
            WorkbenchLog.Info($"VizScope brain={brainLabel} focus={focusLabel} subscribed={_vizSubscribed}");
        }
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

        _debugSubscribed = false;
        _vizSubscribed = false;
        _sink.OnObsStatus("Disconnected", false);
    }

    public Task RefreshDebugFilterAsync(DebugSubscriptionFilter filter)
    {
        if (_root is null || _receiverPid is null || _debugHubPid is null || !_debugSubscribed)
        {
            return Task.CompletedTask;
        }

        filter ??= DebugSubscriptionFilter.Default;
        _root.Send(_debugHubPid, BuildDebugSubscribe(PidLabel(_receiverPid), filter));

        return Task.CompletedTask;
    }

    public virtual async Task<BrainInfo?> RequestBrainInfoAsync(Guid brainId)
    {
        if (_root is null || _ioGatewayPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<BrainInfo>(
                _ioGatewayPid,
                new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
                DefaultTimeout).ConfigureAwait(false);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task RequestBrainInfoAsync(Guid brainId, Action<BrainInfo?> callback)
    {
        if (callback is null)
        {
            return;
        }

        var info = await RequestBrainInfoAsync(brainId).ConfigureAwait(false);
        callback(info);
    }

    public async Task<Nbn.Proto.ArtifactRef?> ExportBrainDefinitionAsync(Guid brainId, bool rebaseOverlays = false)
    {
        if (_root is null || _ioGatewayPid is null)
        {
            return null;
        }

        try
        {
            var ready = await _root.RequestAsync<BrainDefinitionReady>(
                _ioGatewayPid,
                new ExportBrainDefinition
                {
                    BrainId = brainId.ToProtoUuid(),
                    RebaseOverlays = rebaseOverlays
                },
                DefaultTimeout).ConfigureAwait(false);

            return ready?.BrainDef;
        }
        catch (Exception)
        {
            return null;
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

    public virtual void SendInputVector(Guid brainId, IReadOnlyList<float> values)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new InputVectorCommand(brainId, values));
    }

    public void SendRuntimeNeuronPulse(Guid brainId, uint targetRegionId, uint targetNeuronId, float value)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new RuntimeNeuronPulseCommand(brainId, targetRegionId, targetNeuronId, value));
    }

    public void SendRuntimeNeuronStateWrite(
        Guid brainId,
        uint targetRegionId,
        uint targetNeuronId,
        bool setBuffer,
        float bufferValue,
        bool setAccumulator,
        float accumulatorValue)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(
            _receiverPid,
            new RuntimeNeuronStateWriteCommand(
                brainId,
                targetRegionId,
                targetNeuronId,
                setBuffer,
                bufferValue,
                setAccumulator,
                accumulatorValue));
    }

    public void SendEnergyCredit(Guid brainId, long amount)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new EnergyCreditCommand(brainId, amount));
    }

    public virtual async Task<IoCommandResult> SendEnergyCreditAsync(Guid brainId, long amount)
    {
        if (_receiverPid is null || _root is null)
        {
            return new IoCommandResult(brainId, "energy_credit", false, "workbench_offline");
        }

        try
        {
            return await _root.RequestAsync<IoCommandResult>(
                    _receiverPid,
                    new EnergyCreditCommand(brainId, amount),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new IoCommandResult(brainId, "energy_credit", false, $"request_failed:{ex.Message}");
        }
    }

    public void SendEnergyRate(Guid brainId, long unitsPerSecond)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new EnergyRateCommand(brainId, unitsPerSecond));
    }

    public virtual async Task<IoCommandResult> SendEnergyRateAsync(Guid brainId, long unitsPerSecond)
    {
        if (_receiverPid is null || _root is null)
        {
            return new IoCommandResult(brainId, "energy_rate", false, "workbench_offline");
        }

        try
        {
            return await _root.RequestAsync<IoCommandResult>(
                    _receiverPid,
                    new EnergyRateCommand(brainId, unitsPerSecond),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new IoCommandResult(brainId, "energy_rate", false, $"request_failed:{ex.Message}");
        }
    }

    public void SetCostEnergy(Guid brainId, bool costEnabled, bool energyEnabled)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new SetCostEnergyCommand(brainId, costEnabled, energyEnabled));
    }

    public virtual async Task<IoCommandResult> SetCostEnergyAsync(Guid brainId, bool costEnabled, bool energyEnabled)
    {
        if (_receiverPid is null || _root is null)
        {
            return new IoCommandResult(brainId, "set_cost_energy", false, "workbench_offline");
        }

        try
        {
            return await _root.RequestAsync<IoCommandResult>(
                    _receiverPid,
                    new SetCostEnergyCommand(brainId, costEnabled, energyEnabled),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new IoCommandResult(brainId, "set_cost_energy", false, $"request_failed:{ex.Message}");
        }
    }

    public void SetPlasticity(Guid brainId, bool enabled, float rate, bool probabilistic)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, new SetPlasticityCommand(brainId, enabled, rate, probabilistic));
    }

    public virtual async Task<IoCommandResult> SetPlasticityAsync(Guid brainId, bool enabled, float rate, bool probabilistic)
    {
        if (_receiverPid is null || _root is null)
        {
            return new IoCommandResult(brainId, "set_plasticity", false, "workbench_offline");
        }

        try
        {
            return await _root.RequestAsync<IoCommandResult>(
                    _receiverPid,
                    new SetPlasticityCommand(brainId, enabled, rate, probabilistic),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new IoCommandResult(brainId, "set_plasticity", false, $"request_failed:{ex.Message}");
        }
    }

    public void SetHomeostasis(
        Guid brainId,
        bool enabled,
        HomeostasisTargetMode targetMode,
        HomeostasisUpdateMode updateMode,
        float baseProbability,
        uint minStepCodes,
        bool energyCouplingEnabled,
        float energyTargetScale,
        float energyProbabilityScale)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(
            _receiverPid,
            new SetHomeostasisCommand(
                brainId,
                enabled,
                targetMode,
                updateMode,
                baseProbability,
                minStepCodes,
                energyCouplingEnabled,
                energyTargetScale,
                energyProbabilityScale));
    }

    public virtual async Task<IoCommandResult> SetHomeostasisAsync(
        Guid brainId,
        bool enabled,
        HomeostasisTargetMode targetMode,
        HomeostasisUpdateMode updateMode,
        float baseProbability,
        uint minStepCodes,
        bool energyCouplingEnabled,
        float energyTargetScale,
        float energyProbabilityScale)
    {
        if (_receiverPid is null || _root is null)
        {
            return new IoCommandResult(brainId, "set_homeostasis", false, "workbench_offline");
        }

        try
        {
            return await _root.RequestAsync<IoCommandResult>(
                    _receiverPid,
                    new SetHomeostasisCommand(
                        brainId,
                        enabled,
                        targetMode,
                        updateMode,
                        baseProbability,
                        minStepCodes,
                        energyCouplingEnabled,
                        energyTargetScale,
                        energyProbabilityScale),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            return new IoCommandResult(brainId, "set_homeostasis", false, $"request_failed:{ex.Message}");
        }
    }

    public virtual async Task<Nbn.Proto.Repro.ReproduceResult?> ReproduceByBrainIdsAsync(ReproduceByBrainIdsRequest request)
    {
        if (_root is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        if (_ioGatewayPid is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        try
        {
            var result = await _root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
                    _ioGatewayPid,
                    new ReproduceByBrainIds { Request = request },
                    ReproRequestTimeout)
                .ConfigureAwait(false);
            return result?.Result ?? BuildReproFailureResult("repro_empty_response");
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"Repro failed: {ex.Message}", false);
            return BuildReproFailureResult("repro_request_failed");
        }
    }

    public virtual async Task<Nbn.Proto.Repro.ReproduceResult?> ReproduceByArtifactsAsync(ReproduceByArtifactsRequest request)
    {
        if (_root is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        if (_ioGatewayPid is null)
        {
            return BuildReproFailureResult("repro_unavailable");
        }

        try
        {
            var result = await _root.RequestAsync<Nbn.Proto.Io.ReproduceResult>(
                    _ioGatewayPid,
                    new ReproduceByArtifacts { Request = request },
                    ReproRequestTimeout)
                .ConfigureAwait(false);
            return result?.Result ?? BuildReproFailureResult("repro_empty_response");
        }
        catch (Exception ex)
        {
            _sink.OnIoStatus($"Repro failed: {ex.Message}", false);
            return BuildReproFailureResult("repro_request_failed");
        }
    }

    private static Nbn.Proto.Repro.ReproduceResult BuildReproFailureResult(string reasonCode)
    {
        return new Nbn.Proto.Repro.ReproduceResult
        {
            Report = new SimilarityReport
            {
                Compatible = false,
                AbortReason = string.IsNullOrWhiteSpace(reasonCode) ? "repro_request_failed" : reasonCode.Trim(),
                SimilarityScore = 0f,
                RegionSpanScore = 0f,
                FunctionScore = 0f,
                ConnectivityScore = 0f
            },
            Summary = new MutationSummary(),
            Spawned = false
        };
    }

    private async Task StopAsync()
    {
        if (_system is null)
        {
            return;
        }

        try
        {
            if (_root is not null && _hiveMindPid is not null && _vizBrainEnabled.HasValue)
            {
                _root.Send(_hiveMindPid, BuildVisualizationRequest(_vizBrainEnabled.Value, enabled: false));
            }

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
            _debugSubscribed = false;
            _vizSubscribed = false;
            _vizBrainEnabled = null;
            _vizFocusRegionId = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _gate.Dispose();
    }

    private static DebugSubscribe BuildDebugSubscribe(string subscriber, DebugSubscriptionFilter filter)
    {
        var request = new DebugSubscribe
        {
            SubscriberActor = subscriber,
            MinSeverity = filter.MinSeverity,
            ContextRegex = filter.ContextRegex ?? string.Empty
        };

        request.IncludeContextPrefixes.Add(filter.IncludeContextPrefixes ?? Array.Empty<string>());
        request.ExcludeContextPrefixes.Add(filter.ExcludeContextPrefixes ?? Array.Empty<string>());
        request.IncludeSummaryPrefixes.Add(filter.IncludeSummaryPrefixes ?? Array.Empty<string>());
        request.ExcludeSummaryPrefixes.Add(filter.ExcludeSummaryPrefixes ?? Array.Empty<string>());
        return request;
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

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";
    }

    private static async Task<bool> IsEndpointReachableAsync(string host, int port, TimeSpan timeout)
    {
        if (string.IsNullOrWhiteSpace(host) || port <= 0 || port >= 65536)
        {
            return false;
        }

        try
        {
            using var client = new TcpClient();
            using var cts = new CancellationTokenSource(timeout);
            await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
            return client.Connected;
        }
        catch
        {
            return false;
        }
    }

    private SetBrainVisualization BuildVisualizationRequest(Guid brainId, bool enabled, uint? focusRegionId = null)
    {
        var message = new SetBrainVisualization
        {
            BrainId = brainId.ToProtoUuid(),
            Enabled = enabled,
            HasFocusRegion = focusRegionId.HasValue,
            FocusRegionId = focusRegionId ?? 0
        };

        var subscriberActor = GetVisualizationSubscriberActor();
        if (!string.IsNullOrWhiteSpace(subscriberActor))
        {
            message.SubscriberActor = subscriberActor;
        }

        return message;
    }

    private string? GetVisualizationSubscriberActor()
        => _receiverPid is null ? null : PidLabel(_receiverPid);
}
