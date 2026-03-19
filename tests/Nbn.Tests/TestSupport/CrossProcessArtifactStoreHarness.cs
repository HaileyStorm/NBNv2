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
        var workerPath = ResolveWorkerPath();
        var startInfo = CreateWorkerStartInfo(workerPath);

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

    private static ProcessStartInfo CreateWorkerStartInfo(string workerPath)
    {
        var isManagedAssembly = workerPath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase);
        var startInfo = isManagedAssembly
            ? new ProcessStartInfo("dotnet")
            : new ProcessStartInfo(workerPath);

        startInfo.UseShellExecute = false;
        startInfo.RedirectStandardOutput = true;
        startInfo.RedirectStandardError = true;

        if (isManagedAssembly)
        {
            startInfo.ArgumentList.Add(workerPath);
        }

        return startInfo;
    }

    private static string ResolveWorkerPath()
    {
        var searchRoot = new DirectoryInfo(AppContext.BaseDirectory);
        while (searchRoot is not null)
        {
            var candidate = TryResolveWorkerPath(searchRoot.FullName, includeRepoOutputLayouts: false);
            if (candidate is not null)
            {
                return candidate;
            }

            searchRoot = searchRoot.Parent;
        }

        var repoRoot = FindRepoRoot();
        var repoCandidate = TryResolveWorkerPath(repoRoot, includeRepoOutputLayouts: true);
        if (repoCandidate is not null)
        {
            return repoCandidate;
        }

        throw new FileNotFoundException("Cross-process artifact worker output was not found near the current test artifacts path.");
    }

    private static string? TryResolveWorkerPath(string searchRoot, bool includeRepoOutputLayouts)
    {
        var candidateDirectories = new List<string>
        {
            searchRoot,
            Path.Combine(searchRoot, "Nbn.Tests.CrossProcessArtifactWorker", "release"),
            Path.Combine(searchRoot, "Nbn.Tests.CrossProcessArtifactWorker", "release", "net8.0"),
            Path.Combine(searchRoot, "bin", "Nbn.Tests.CrossProcessArtifactWorker", "release"),
            Path.Combine(searchRoot, "bin", "Nbn.Tests.CrossProcessArtifactWorker", "release", "net8.0")
        };

        if (includeRepoOutputLayouts)
        {
            candidateDirectories.Add(Path.Combine(searchRoot, "tests", "Nbn.Tests.CrossProcessArtifactWorker", "bin", "Release", "net8.0"));
            candidateDirectories.Add(Path.Combine(searchRoot, "tests", "Nbn.Tests.CrossProcessArtifactWorker", "bin", "Debug", "net8.0"));
        }

        foreach (var directory in candidateDirectories)
        {
            foreach (var fileName in EnumerateWorkerFileNames())
            {
                var candidate = Path.Combine(directory, fileName);
                if (IsRunnableWorkerCandidate(candidate))
                {
                    return candidate;
                }
            }
        }

        return null;
    }

    private static IEnumerable<string> EnumerateWorkerFileNames()
    {
        yield return "Nbn.Tests.CrossProcessArtifactWorker.dll";
        yield return "Nbn.Tests.CrossProcessArtifactWorker.exe";
        yield return "Nbn.Tests.CrossProcessArtifactWorker";
    }

    private static bool IsRunnableWorkerCandidate(string candidatePath)
    {
        if (!File.Exists(candidatePath))
        {
            return false;
        }

        if (candidatePath.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Framework-dependent apphosts need their companion assembly beside them.
        var companionAssemblyPath = candidatePath.EndsWith(".exe", StringComparison.OrdinalIgnoreCase)
            ? Path.ChangeExtension(candidatePath, ".dll")
            : candidatePath + ".dll";
        return File.Exists(companionAssemblyPath);
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
