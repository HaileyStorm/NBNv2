using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Speciation;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Proto.Control;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;

namespace Nbn.Tools.Workbench.Services;

public partial class WorkbenchClient
{
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

    public virtual async Task<PlacementWorkerInventory?> GetPlacementWorkerInventoryAsync()
    {
        if (_root is null || _hiveMindPid is null)
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<PlacementWorkerInventory>(
                    _hiveMindPid,
                    new PlacementWorkerInventoryRequest(),
                    DefaultTimeout)
                .ConfigureAwait(false);
        }
        catch
        {
            return null;
        }
    }

    public virtual async Task<PlacementWorkerInventory?> WaitForPlacementWorkerAvailabilityAsync(
        int minEligibleWorkers = 1,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null)
    {
        var requiredWorkers = Math.Max(1, minEligibleWorkers);
        var waitTimeout = timeout ?? PlacementWorkerReadyTimeout;
        var waitInterval = pollInterval ?? PlacementWorkerReadyPollInterval;
        var deadline = DateTime.UtcNow + waitTimeout;
        PlacementWorkerInventory? lastInventory = null;

        while (true)
        {
            lastInventory = await GetPlacementWorkerInventoryAsync().ConfigureAwait(false);
            if (lastInventory is not null && lastInventory.Workers.Count >= requiredWorkers)
            {
                return lastInventory;
            }

            if (DateTime.UtcNow >= deadline)
            {
                return lastInventory;
            }

            await Task.Delay(waitInterval).ConfigureAwait(false);
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
            _root.Send(_settingsPid, new SettingUnsubscribe
            {
                SubscriberActor = GetSubscriberActorReference() ?? PidLabel(_receiverPid)
            });
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

    public virtual async Task<SettingListResponse?> ListSettingsAsync(string host, int port, string actorName)
    {
        if (_root is null || string.IsNullOrWhiteSpace(host) || port <= 0 || port >= 65536 || string.IsNullOrWhiteSpace(actorName))
        {
            return null;
        }

        try
        {
            return await _root.RequestAsync<SettingListResponse>(
                new PID($"{host.Trim()}:{port}", actorName.Trim()),
                new SettingListRequest(),
                DefaultTimeout).ConfigureAwait(false);
        }
        catch
        {
            return null;
        }
    }

    public virtual async Task<TcpEndpointProbeResult> ProbeTcpEndpointAsync(string host, int port, TimeSpan? timeout = null)
    {
        if (string.IsNullOrWhiteSpace(host) || port <= 0 || port >= 65536)
        {
            return new TcpEndpointProbeResult(false, $"TCP connect to {host}:{port} is invalid.");
        }

        using var client = new TcpClient();
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(3));
        try
        {
            await client.ConnectAsync(host.Trim(), port, cts.Token).ConfigureAwait(false);
            return new TcpEndpointProbeResult(true, $"TCP connect to {host}:{port} succeeded.");
        }
        catch (OperationCanceledException)
        {
            return new TcpEndpointProbeResult(false, $"TCP connect to {host}:{port} timed out.");
        }
        catch (SocketException ex)
        {
            return new TcpEndpointProbeResult(false, $"TCP connect to {host}:{port} failed: {ex.SocketErrorCode}.");
        }
        catch (Exception ex)
        {
            return new TcpEndpointProbeResult(false, $"TCP connect to {host}:{port} failed: {ex.GetBaseException().Message}");
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
            ? new SubscribeOutputsVectorCommand(brainId, GetSubscriberActorReference())
            : new SubscribeOutputsCommand(brainId, GetSubscriberActorReference()));
    }

    public void UnsubscribeOutputs(Guid brainId, bool vector)
    {
        if (_receiverPid is null || _root is null)
        {
            return;
        }

        _root.Send(_receiverPid, vector
            ? new UnsubscribeOutputsVectorCommand(brainId, GetSubscriberActorReference())
            : new UnsubscribeOutputsCommand(brainId, GetSubscriberActorReference()));
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
}
