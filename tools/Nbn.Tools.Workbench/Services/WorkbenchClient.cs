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

/// <summary>
/// Owns the Workbench actor-system client used to connect to runtime services and forward runtime events to the UI.
/// </summary>
public partial class WorkbenchClient : IAsyncDisposable
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan SpawnRequestTimeout = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan PlacementWorkerReadyTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan PlacementWorkerReadyPollInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan ReproRequestTimeout = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan SpeciationRequestTimeout = TimeSpan.FromSeconds(45);
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
    private string? _advertisedHost;
    private int? _advertisedPort;

    /// <summary>
    /// Creates a Workbench client that reports status and runtime events to the supplied sink.
    /// </summary>
    public WorkbenchClient(IWorkbenchEventSink sink)
    {
        _sink = sink;
    }

    /// <summary>
    /// Gets whether the local Workbench actor system is running.
    /// </summary>
    public bool IsRunning => _system is not null;

    /// <summary>
    /// Gets the current receiver actor label used for runtime callbacks.
    /// </summary>
    public string ReceiverLabel => _receiverPid is null ? "offline" : PidLabel(_receiverPid);
}

/// <summary>
/// Reports the outcome of a direct TCP reachability probe.
/// </summary>
public sealed record TcpEndpointProbeResult(bool Reachable, string Detail);
