namespace Nbn.Tools.EvolutionSim;

/// <summary>
/// Owns the single-session start/stop lifecycle exposed to CLI and Workbench callers.
/// </summary>
public sealed class EvolutionSimulationController
{
    private readonly EvolutionSimulationSession _session;
    // _gate owns the active run handles so polling and cancellation observe one coherent session state.
    private readonly object _gate = new();
    private CancellationTokenSource? _runCancellation;
    private Task<EvolutionSimulationStatus>? _runTask;

    /// <summary>
    /// Initializes a controller for one simulation session instance.
    /// </summary>
    public EvolutionSimulationController(EvolutionSimulationSession session)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    /// <summary>
    /// Gets the currently active session task, or <see langword="null"/> when idle.
    /// </summary>
    public Task<EvolutionSimulationStatus>? CurrentSessionTask
    {
        get
        {
            lock (_gate)
            {
                return _runTask;
            }
        }
    }

    /// <summary>
    /// Starts the session if no run is currently active.
    /// </summary>
    public bool Start()
    {
        lock (_gate)
        {
            if (HasActiveRunLocked())
            {
                return false;
            }

            StartSessionLocked();
            return true;
        }
    }

    /// <summary>
    /// Returns the latest observable session status snapshot.
    /// </summary>
    public EvolutionSimulationStatus GetStatus() => _session.GetStatus();

    /// <summary>
    /// Requests cancellation of the active run and waits until the session stops.
    /// </summary>
    public async Task<bool> StopAsync()
    {
        Task<EvolutionSimulationStatus>? runTask;
        CancellationTokenSource? runCancellation;

        lock (_gate)
        {
            if (!HasActiveRunLocked())
            {
                return false;
            }

            runTask = _runTask!;
            runCancellation = _runCancellation;
            runCancellation?.Cancel();
        }

        try
        {
            await runTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // normal stop path
        }
        finally
        {
            lock (_gate)
            {
                ClearRunStateLocked(runTask);
            }
        }

        return true;
    }

    // Caller must hold _gate.
    private bool HasActiveRunLocked()
        => _runTask is not null && !_runTask.IsCompleted;

    // Caller must hold _gate.
    private void StartSessionLocked()
    {
        _runCancellation?.Dispose();
        var runCancellation = new CancellationTokenSource();
        _runCancellation = runCancellation;
        _runTask = Task.Run(() => _session.RunAsync(runCancellation.Token));
    }

    // Caller must hold _gate.
    private void ClearRunStateLocked(Task<EvolutionSimulationStatus>? completedTask)
    {
        if (completedTask is not null && !ReferenceEquals(completedTask, _runTask))
        {
            return;
        }

        _runTask = null;
        _runCancellation?.Dispose();
        _runCancellation = null;
    }
}
