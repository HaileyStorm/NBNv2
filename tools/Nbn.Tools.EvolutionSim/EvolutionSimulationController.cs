namespace Nbn.Tools.EvolutionSim;

public sealed class EvolutionSimulationController
{
    private readonly EvolutionSimulationSession _session;
    private readonly object _gate = new();
    private CancellationTokenSource? _runCancellation;
    private Task<EvolutionSimulationStatus>? _runTask;

    public EvolutionSimulationController(EvolutionSimulationSession session)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

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

    public bool Start()
    {
        lock (_gate)
        {
            if (_runTask is not null && !_runTask.IsCompleted)
            {
                return false;
            }

            _runCancellation?.Dispose();
            _runCancellation = new CancellationTokenSource();
            _runTask = Task.Run(() => _session.RunAsync(_runCancellation.Token));
            return true;
        }
    }

    public EvolutionSimulationStatus GetStatus() => _session.GetStatus();

    public async Task<bool> StopAsync()
    {
        Task<EvolutionSimulationStatus>? runTask;
        CancellationTokenSource? runCancellation;

        lock (_gate)
        {
            if (_runTask is null || _runTask.IsCompleted)
            {
                return false;
            }

            runTask = _runTask;
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
                if (ReferenceEquals(runTask, _runTask))
                {
                    _runTask = null;
                    _runCancellation?.Dispose();
                    _runCancellation = null;
                }
            }
        }

        return true;
    }
}
