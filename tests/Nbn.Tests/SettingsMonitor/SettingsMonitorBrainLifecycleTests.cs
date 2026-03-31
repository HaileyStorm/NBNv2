using Nbn.Runtime.SettingsMonitor;
using Nbn.Tests.TestSupport;

namespace Nbn.Tests.SettingsMonitor;

public sealed class SettingsMonitorBrainLifecycleTests
{
    [Fact]
    public async Task DeadBrain_IgnoresLateControllerHeartbeat_StateChange_AndTick()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var brainId = Guid.NewGuid();
        var nodeId = Guid.NewGuid();

        await store.UpsertBrainAsync(brainId, "Active", spawnedMs: 100, lastTickId: 12);
        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 150);

        await store.MarkBrainControllerOfflineAsync(brainId, timeMs: 300);
        await store.UpdateBrainStateAsync(brainId, "Dead");

        await store.RecordBrainControllerHeartbeatAsync(new BrainControllerHeartbeat(brainId, 200));
        await store.UpdateBrainStateAsync(brainId, "Recovering");
        await store.UpdateBrainTickAsync(brainId, 99);

        var brain = await store.GetBrainAsync(brainId);
        var controller = await store.GetBrainControllerAsync(brainId);

        Assert.NotNull(brain);
        Assert.Equal("Dead", brain!.State);
        Assert.Equal(12, brain.LastTickId);

        Assert.NotNull(controller);
        Assert.False(controller!.IsAlive);
        Assert.Equal(300, controller.LastSeenMs);
    }

    [Fact]
    public async Task ExplicitReRegistration_RevivesDeadBrainAndController()
    {
        using var db = new TempDatabaseScope("settings-monitor.db");
        var store = new SettingsMonitorStore(db.DatabasePath);
        await store.InitializeAsync();

        var brainId = Guid.NewGuid();
        var nodeId = Guid.NewGuid();

        await store.UpsertBrainAsync(brainId, "Active", spawnedMs: 100, lastTickId: 12);
        await store.UpsertNodeAsync(new NodeRegistration(nodeId, "worker-a", "127.0.0.1:12041", "worker-node"), timeMs: 100);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 150);
        await store.MarkBrainControllerOfflineAsync(brainId, timeMs: 300);
        await store.UpdateBrainStateAsync(brainId, "Dead");

        await store.UpsertBrainAsync(brainId, "Recovering", spawnedMs: 500, lastTickId: 0);
        await store.UpsertBrainControllerAsync(
            new BrainControllerRegistration(brainId, nodeId, "worker-node/brain-root"),
            timeMs: 500);
        await store.UpdateBrainStateAsync(brainId, "Active");
        await store.RecordBrainControllerHeartbeatAsync(new BrainControllerHeartbeat(brainId, 550));
        await store.UpdateBrainTickAsync(brainId, 77);

        var brain = await store.GetBrainAsync(brainId);
        var controller = await store.GetBrainControllerAsync(brainId);

        Assert.NotNull(brain);
        Assert.Equal("Active", brain!.State);
        Assert.Equal(77, brain.LastTickId);

        Assert.NotNull(controller);
        Assert.True(controller!.IsAlive);
        Assert.Equal(550, controller.LastSeenMs);
    }
}
