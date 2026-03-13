using System.Diagnostics;
using System.Text;

namespace Nbn.Tests.TestSupport;

public sealed class CrossProcessArtifactStoreHarness : IDisposable
{
    private readonly List<WorkerProcessHandle> _workers = new();

    public WorkerProcessHandle StartWorker(
        string mode,
        string rootPath,
        string payloadPath,
        string mediaType,
        string? readySignalPath = null,
        string? releaseSignalPath = null)
    {
        var workerDllPath = ResolveWorkerDllPath();
        var startInfo = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        startInfo.ArgumentList.Add(workerDllPath);
        startInfo.ArgumentList.Add(mode);
        startInfo.ArgumentList.Add(rootPath);
        startInfo.ArgumentList.Add(payloadPath);
        startInfo.ArgumentList.Add(mediaType);
        startInfo.ArgumentList.Add(readySignalPath ?? "-");
        startInfo.ArgumentList.Add(releaseSignalPath ?? "-");

        var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Failed to start cross-process artifact worker.");
        var handle = new WorkerProcessHandle(process);
        _workers.Add(handle);
        return handle;
    }

    public static async Task WaitForFileAsync(string path, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!File.Exists(path))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException($"Timed out waiting for file '{path}'.");
            }

            await Task.Delay(25, cancellationToken);
        }
    }

    public static string GetWorkerPayloadPath(string directoryPath, string name)
        => Path.Combine(directoryPath, $"{name}.bin");

    public void Dispose()
    {
        foreach (var worker in _workers)
        {
            worker.Dispose();
        }
    }

    private static string ResolveWorkerDllPath()
    {
        var searchRoot = new DirectoryInfo(AppContext.BaseDirectory);
        while (searchRoot is not null)
        {
            var directCandidate = Path.Combine(
                searchRoot.FullName,
                "Nbn.Tests.CrossProcessArtifactWorker",
                "release",
                "Nbn.Tests.CrossProcessArtifactWorker.dll");
            if (File.Exists(directCandidate))
            {
                return directCandidate;
            }

            var tfmCandidate = Path.Combine(
                searchRoot.FullName,
                "Nbn.Tests.CrossProcessArtifactWorker",
                "release",
                "net8.0",
                "Nbn.Tests.CrossProcessArtifactWorker.dll");
            if (File.Exists(tfmCandidate))
            {
                return tfmCandidate;
            }

            var binDirectCandidate = Path.Combine(
                searchRoot.FullName,
                "bin",
                "Nbn.Tests.CrossProcessArtifactWorker",
                "release",
                "Nbn.Tests.CrossProcessArtifactWorker.dll");
            if (File.Exists(binDirectCandidate))
            {
                return binDirectCandidate;
            }

            var binTfmCandidate = Path.Combine(
                searchRoot.FullName,
                "bin",
                "Nbn.Tests.CrossProcessArtifactWorker",
                "release",
                "net8.0",
                "Nbn.Tests.CrossProcessArtifactWorker.dll");
            if (File.Exists(binTfmCandidate))
            {
                return binTfmCandidate;
            }

            searchRoot = searchRoot.Parent;
        }

        var repoRoot = FindRepoRoot();
        var repoBinCandidate = Path.Combine(
            repoRoot,
            "tests",
            "Nbn.Tests.CrossProcessArtifactWorker",
            "bin",
            "Release",
            "net8.0",
            "Nbn.Tests.CrossProcessArtifactWorker.dll");
        if (File.Exists(repoBinCandidate))
        {
            return repoBinCandidate;
        }

        throw new FileNotFoundException("Cross-process artifact worker output was not found near the current test artifacts path.");
    }

    private static string FindRepoRoot()
    {
        var searchRoot = new DirectoryInfo(AppContext.BaseDirectory);
        while (searchRoot is not null)
        {
            var projectPath = Path.Combine(searchRoot.FullName, "tests", "Nbn.Tests", "Nbn.Tests.csproj");
            if (File.Exists(projectPath))
            {
                return searchRoot.FullName;
            }

            searchRoot = searchRoot.Parent;
        }

        throw new DirectoryNotFoundException("Repository root could not be located from the current test output path.");
    }
}

public sealed class WorkerProcessHandle : IDisposable
{
    private readonly Process _process;
    private readonly Task<string> _stdoutTask;
    private readonly Task<string> _stderrTask;
    private bool _disposed;

    public WorkerProcessHandle(Process process)
    {
        _process = process;
        _stdoutTask = process.StandardOutput.ReadToEndAsync();
        _stderrTask = process.StandardError.ReadToEndAsync();
    }

    public async Task WaitForSuccessAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);
        await _process.WaitForExitAsync(timeoutCts.Token);

        var stdout = await _stdoutTask;
        var stderr = await _stderrTask;
        if (_process.ExitCode != 0)
        {
            var message = new StringBuilder()
                .AppendLine($"Worker exited with code {_process.ExitCode}.")
                .AppendLine("STDOUT:")
                .AppendLine(stdout)
                .AppendLine("STDERR:")
                .AppendLine(stderr)
                .ToString();
            throw new Xunit.Sdk.XunitException(message);
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        try
        {
            if (!_process.HasExited)
            {
                _process.Kill(entireProcessTree: true);
            }
        }
        catch
        {
        }

        _process.Dispose();
    }
}
