using Nbn.Runtime.HiveMind;

namespace Nbn.Tests.HiveMind;

public sealed class BackpressureControllerTests
{
    [Fact]
    public void Evaluate_SustainedLateArrivals_DecaysTargetTickHz_Then_Recovers_OnHealthyTicks()
    {
        var controller = new BackpressureController(CreateOptions(
            targetTickHz: 20f,
            minTickHz: 5f,
            backpressureDecay: 0.5f,
            backpressureRecovery: 2.0f,
            lateBackpressureThreshold: 2,
            timeoutRescheduleThreshold: 10,
            timeoutPauseThreshold: 20));

        var firstLate = controller.Evaluate(CreateOutcome(
            tickId: 1,
            lateComputeCount: 1));
        var secondLate = controller.Evaluate(CreateOutcome(
            tickId: 2,
            lateDeliverCount: 1));
        var thirdLate = controller.Evaluate(CreateOutcome(
            tickId: 3,
            lateComputeCount: 1));
        var firstHealthy = controller.Evaluate(CreateOutcome(tickId: 4));
        var secondHealthy = controller.Evaluate(CreateOutcome(tickId: 5));

        Assert.Equal(20f, firstLate.TargetTickHz, 3);
        Assert.False(firstLate.RequestReschedule);
        Assert.False(firstLate.RequestPause);

        Assert.Equal(10f, secondLate.TargetTickHz, 3);
        Assert.False(secondLate.RequestReschedule);
        Assert.False(secondLate.RequestPause);
        Assert.Contains("backpressure after 2 late ticks", secondLate.Reason, StringComparison.Ordinal);

        Assert.Equal(5f, thirdLate.TargetTickHz, 3);
        Assert.False(thirdLate.RequestReschedule);
        Assert.False(thirdLate.RequestPause);
        Assert.Contains("backpressure after 3 late ticks", thirdLate.Reason, StringComparison.Ordinal);

        Assert.Equal(10f, firstHealthy.TargetTickHz, 3);
        Assert.Equal(20f, secondHealthy.TargetTickHz, 3);
        Assert.False(firstHealthy.RequestReschedule);
        Assert.False(firstHealthy.RequestPause);
        Assert.False(secondHealthy.RequestReschedule);
        Assert.False(secondHealthy.RequestPause);
        Assert.Equal(0, controller.TimeoutStreak);
    }

    [Fact]
    public void Evaluate_TimeoutStreak_RequestsReschedule_ThenPause_And_HealthyTickResetsState()
    {
        var controller = new BackpressureController(CreateOptions(
            targetTickHz: 20f,
            minTickHz: 5f,
            backpressureDecay: 0.5f,
            backpressureRecovery: 2.0f,
            timeoutRescheduleThreshold: 2,
            timeoutPauseThreshold: 3));

        var firstTimeout = controller.Evaluate(CreateOutcome(
            tickId: 1,
            computeTimedOut: true,
            expectedComputeCount: 2,
            completedComputeCount: 1));
        var secondTimeout = controller.Evaluate(CreateOutcome(
            tickId: 2,
            deliverTimedOut: true,
            expectedDeliverCount: 1,
            completedDeliverCount: 0));
        var thirdTimeout = controller.Evaluate(CreateOutcome(
            tickId: 3,
            computeTimedOut: true));
        var healthy = controller.Evaluate(CreateOutcome(tickId: 4));
        var recovered = controller.Evaluate(CreateOutcome(tickId: 5));

        Assert.Equal(10f, firstTimeout.TargetTickHz, 3);
        Assert.False(firstTimeout.RequestReschedule);
        Assert.False(firstTimeout.RequestPause);
        Assert.Equal(1, firstTimeout.TimeoutStreak);

        Assert.Equal(5f, secondTimeout.TargetTickHz, 3);
        Assert.True(secondTimeout.RequestReschedule);
        Assert.False(secondTimeout.RequestPause);
        Assert.Equal(2, secondTimeout.TimeoutStreak);
        Assert.Contains("reschedule after 2 timeouts", secondTimeout.Reason, StringComparison.Ordinal);

        Assert.Equal(5f, thirdTimeout.TargetTickHz, 3);
        Assert.True(thirdTimeout.RequestReschedule);
        Assert.True(thirdTimeout.RequestPause);
        Assert.Equal(3, thirdTimeout.TimeoutStreak);
        Assert.Contains("pause after 3 timeouts", thirdTimeout.Reason, StringComparison.Ordinal);

        Assert.Equal(10f, healthy.TargetTickHz, 3);
        Assert.Equal(0, healthy.TimeoutStreak);
        Assert.False(healthy.RequestReschedule);
        Assert.False(healthy.RequestPause);

        Assert.Equal(20f, recovered.TargetTickHz, 3);
        Assert.Equal(0, controller.TimeoutStreak);
        Assert.False(recovered.RequestReschedule);
        Assert.False(recovered.RequestPause);
    }

    private static TickOutcome CreateOutcome(
        ulong tickId,
        bool computeTimedOut = false,
        bool deliverTimedOut = false,
        int lateComputeCount = 0,
        int lateDeliverCount = 0,
        int expectedComputeCount = 1,
        int completedComputeCount = 1,
        int expectedDeliverCount = 1,
        int completedDeliverCount = 1)
        => new(
            TickId: tickId,
            ComputeDuration: TimeSpan.FromMilliseconds(10),
            DeliverDuration: TimeSpan.FromMilliseconds(10),
            ComputeTimedOut: computeTimedOut,
            DeliverTimedOut: deliverTimedOut,
            LateComputeCount: lateComputeCount,
            LateDeliverCount: lateDeliverCount,
            ExpectedComputeCount: expectedComputeCount,
            CompletedComputeCount: completedComputeCount,
            ExpectedDeliverCount: expectedDeliverCount,
            CompletedDeliverCount: completedDeliverCount);

    private static HiveMindOptions CreateOptions(
        float targetTickHz = 50f,
        float minTickHz = 10f,
        float backpressureDecay = 0.9f,
        float backpressureRecovery = 1.1f,
        int lateBackpressureThreshold = 2,
        int timeoutRescheduleThreshold = 3,
        int timeoutPauseThreshold = 6)
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: targetTickHz,
            MinTickHz: minTickHz,
            ComputeTimeoutMs: 500,
            DeliverTimeoutMs: 500,
            BackpressureDecay: backpressureDecay,
            BackpressureRecovery: backpressureRecovery,
            LateBackpressureThreshold: lateBackpressureThreshold,
            TimeoutRescheduleThreshold: timeoutRescheduleThreshold,
            TimeoutPauseThreshold: timeoutPauseThreshold,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false,
            EnableOpenTelemetry: false,
            EnableOtelMetrics: false,
            EnableOtelTraces: false,
            EnableOtelConsoleExporter: false,
            OtlpEndpoint: null,
            ServiceName: "nbn.hivemind.tests",
            SettingsDbPath: null,
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            IoAddress: null,
            IoName: null);
}
