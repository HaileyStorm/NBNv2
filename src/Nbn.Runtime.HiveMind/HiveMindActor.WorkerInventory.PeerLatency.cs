using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private void MaybeRefreshPeerLatency(IContext context, bool force)
    {
        var task = EnsurePeerLatencyRefreshTask(context, force);
        if (task is null)
        {
            return;
        }

        context.ReenterAfter(
            task,
            completed => ApplyPeerLatencyRefreshResult(task, completed));
    }

    private Task<IReadOnlyList<WorkerPeerLatencyMeasurement>>? EnsurePeerLatencyRefreshTask(IContext context, bool force)
    {
        if (_peerLatencyRefreshTask is not null && !_peerLatencyRefreshTask.IsCompleted)
        {
            return _peerLatencyRefreshTask;
        }

        var nowMs = NowMs();
        if (!force
            && _lastPeerLatencyRefreshMs > 0
            && nowMs - _lastPeerLatencyRefreshMs < (long)PlacementPeerLatencyRefreshInterval.TotalMilliseconds)
        {
            return null;
        }

        var probeTargets = BuildPeerLatencyProbeTargets(nowMs);
        if (probeTargets.Count < 2)
        {
            ClearPeerLatencyMeasurements(nowMs);
            _lastPeerLatencyRefreshMs = nowMs;
            return null;
        }

        _lastPeerLatencyRefreshMs = nowMs;
        _peerLatencyRefreshTask = CollectPeerLatencyMeasurementsAsync(context.System, probeTargets);
        return _peerLatencyRefreshTask;
    }

    private List<PeerLatencyProbeTarget> BuildPeerLatencyProbeTargets(long nowMs)
    {
        RefreshWorkerCatalogFreshness(nowMs);
        return _workerCatalog.Values
            .Where(entry =>
                entry.IsAlive
                && entry.IsReady
                && entry.IsFresh
                && IsPlacementWorkerCandidate(entry.LogicalName, entry.WorkerRootActorName)
                && !string.IsNullOrWhiteSpace(entry.WorkerRootActorName))
            .OrderBy(static entry => entry.WorkerAddress, StringComparer.Ordinal)
            .ThenBy(static entry => entry.NodeId)
            .Select(static entry => new PeerLatencyProbeTarget(
                entry.NodeId,
                entry.WorkerAddress,
                entry.WorkerRootActorName))
            .ToList();
    }

    private void ApplyPeerLatencyRefreshResult(
        Task<IReadOnlyList<WorkerPeerLatencyMeasurement>> refreshTask,
        Task<IReadOnlyList<WorkerPeerLatencyMeasurement>> completed)
    {
        if (ReferenceEquals(_peerLatencyRefreshTask, refreshTask))
        {
            _peerLatencyRefreshTask = null;
        }

        if (!completed.IsCompletedSuccessfully)
        {
            return;
        }

        ApplyPeerLatencyMeasurements(completed.Result, NowMs());
    }

    private void ApplyPeerLatencyMeasurements(IReadOnlyList<WorkerPeerLatencyMeasurement> measurements, long snapshotMs)
    {
        var byWorker = measurements.ToDictionary(static measurement => measurement.WorkerNodeId);
        foreach (var entry in _workerCatalog.Values)
        {
            if (byWorker.TryGetValue(entry.NodeId, out var measurement))
            {
                entry.AveragePeerLatencyMs = measurement.AveragePeerLatencyMs;
                entry.PeerLatencySampleCount = measurement.SampleCount;
                entry.PeerLatencySnapshotMs = snapshotMs;
                continue;
            }

            entry.AveragePeerLatencyMs = 0f;
            entry.PeerLatencySampleCount = 0;
            entry.PeerLatencySnapshotMs = snapshotMs;
        }
    }

    private void ClearPeerLatencyMeasurements(long snapshotMs)
    {
        foreach (var entry in _workerCatalog.Values)
        {
            entry.AveragePeerLatencyMs = 0f;
            entry.PeerLatencySampleCount = 0;
            entry.PeerLatencySnapshotMs = snapshotMs;
        }
    }

    private static async Task<IReadOnlyList<WorkerPeerLatencyMeasurement>> CollectPeerLatencyMeasurementsAsync(
        ActorSystem system,
        IReadOnlyList<PeerLatencyProbeTarget> probeTargets)
    {
        if (probeTargets.Count < 2)
        {
            return Array.Empty<WorkerPeerLatencyMeasurement>();
        }

        var measurements = new List<WorkerPeerLatencyMeasurement>(probeTargets.Count);
        foreach (var worker in probeTargets)
        {
            var peerTargets = probeTargets
                .Where(peer => peer.NodeId != worker.NodeId)
                .OrderBy(static peer => peer.WorkerAddress, StringComparer.Ordinal)
                .ThenBy(static peer => peer.NodeId)
                .ToArray();
            if (peerTargets.Length == 0)
            {
                measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
                continue;
            }

            var request = new ProtoControl.PlacementPeerLatencyRequest
            {
                TimeoutMs = (uint)Math.Max(50, PlacementPeerLatencyProbeTimeout.TotalMilliseconds)
            };
            foreach (var peer in peerTargets)
            {
                request.Peers.Add(new ProtoControl.PlacementPeerTarget
                {
                    WorkerNodeId = peer.NodeId.ToProtoUuid(),
                    WorkerAddress = peer.WorkerAddress,
                    WorkerRootActorName = peer.WorkerRootActorName
                });
            }

            var target = new PID(worker.WorkerAddress, worker.WorkerRootActorName);
            var timeoutMs = Math.Max(
                250,
                peerTargets.Length * (int)PlacementPeerLatencyProbeTimeout.TotalMilliseconds + 250);
            try
            {
                var response = await system.Root.RequestAsync<ProtoControl.PlacementPeerLatencyResponse>(
                        target,
                        request,
                        TimeSpan.FromMilliseconds(timeoutMs))
                    .ConfigureAwait(false);
                if (response is null || !TryGetGuid(response.WorkerNodeId, out var workerNodeId))
                {
                    measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
                    continue;
                }

                measurements.Add(new WorkerPeerLatencyMeasurement(
                    workerNodeId,
                    response.AveragePeerLatencyMs,
                    (int)response.SampleCount));
            }
            catch
            {
                measurements.Add(new WorkerPeerLatencyMeasurement(worker.NodeId, 0f, 0));
            }
        }

        return measurements;
    }
}
