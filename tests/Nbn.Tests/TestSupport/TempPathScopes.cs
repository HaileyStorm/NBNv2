using Microsoft.Data.Sqlite;

namespace Nbn.Tests.TestSupport;

internal sealed class TempDirectoryScope : IDisposable
{
    private readonly bool _clearSqlitePools;
    private readonly bool _suppressCleanupFailures;

    private TempDirectoryScope(string rootPath, bool clearSqlitePools, bool suppressCleanupFailures)
    {
        RootPath = rootPath;
        _clearSqlitePools = clearSqlitePools;
        _suppressCleanupFailures = suppressCleanupFailures;
        Directory.CreateDirectory(rootPath);
    }

    public string RootPath { get; }

    public static TempDirectoryScope Create(
        string prefix,
        bool clearSqlitePools = false,
        bool suppressCleanupFailures = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        return new TempDirectoryScope(
            Path.Combine(Path.GetTempPath(), $"{prefix}-{Guid.NewGuid():N}"),
            clearSqlitePools,
            suppressCleanupFailures);
    }

    public static TempDirectoryScope CreateNested(
        string directoryName,
        bool clearSqlitePools = false,
        bool suppressCleanupFailures = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryName);
        return new TempDirectoryScope(
            Path.Combine(Path.GetTempPath(), directoryName, Guid.NewGuid().ToString("N")),
            clearSqlitePools,
            suppressCleanupFailures);
    }

    public string GetPath(string fileName)
        => Path.Combine(RootPath, fileName);

    public void Dispose()
    {
        if (_suppressCleanupFailures)
        {
            try
            {
                DisposeCore();
            }
            catch
            {
            }
        }
        else
        {
            DisposeCore();
        }
    }

    private void DisposeCore()
    {
        if (_clearSqlitePools)
        {
            SqliteConnection.ClearAllPools();
        }

        if (Directory.Exists(RootPath))
        {
            Directory.Delete(RootPath, recursive: true);
        }
    }

    public override string ToString()
        => RootPath;

    public static implicit operator string(TempDirectoryScope scope)
        => scope.RootPath;
}

internal sealed class TempFileScope : IDisposable
{
    private readonly bool _clearSqlitePools;
    private readonly bool _suppressCleanupFailures;

    private TempFileScope(string filePath, bool clearSqlitePools, bool suppressCleanupFailures)
    {
        FilePath = filePath;
        _clearSqlitePools = clearSqlitePools;
        _suppressCleanupFailures = suppressCleanupFailures;
    }

    public string FilePath { get; }

    public static TempFileScope Create(
        string extension = ".tmp",
        string? prefix = null,
        bool clearSqlitePools = false,
        bool suppressCleanupFailures = false)
    {
        var normalizedExtension = NormalizeExtension(extension);
        var fileName = $"{(string.IsNullOrWhiteSpace(prefix) ? Guid.NewGuid().ToString("N") : prefix)}{normalizedExtension}";
        return new TempFileScope(Path.Combine(Path.GetTempPath(), fileName), clearSqlitePools, suppressCleanupFailures);
    }

    public static async Task<TempFileScope> WriteAllTextAsync(
        string contents,
        string extension = ".tmp",
        string? prefix = null,
        bool clearSqlitePools = false,
        bool suppressCleanupFailures = false,
        CancellationToken cancellationToken = default)
    {
        var scope = Create(extension, prefix, clearSqlitePools, suppressCleanupFailures);
        await File.WriteAllTextAsync(scope.FilePath, contents, cancellationToken).ConfigureAwait(false);
        return scope;
    }

    public void Dispose()
    {
        if (_suppressCleanupFailures)
        {
            try
            {
                DisposeCore();
            }
            catch
            {
            }
        }
        else
        {
            DisposeCore();
        }
    }

    private void DisposeCore()
    {
        if (_clearSqlitePools)
        {
            SqliteConnection.ClearAllPools();
        }

        if (File.Exists(FilePath))
        {
            File.Delete(FilePath);
        }
    }

    public override string ToString()
        => FilePath;

    public static implicit operator string(TempFileScope scope)
        => scope.FilePath;

    private static string NormalizeExtension(string extension)
    {
        if (string.IsNullOrWhiteSpace(extension))
        {
            return ".tmp";
        }

        return extension.StartsWith(".", StringComparison.Ordinal) ? extension : $".{extension}";
    }
}

internal sealed class TempDatabaseScope : IDisposable
{
    private readonly TempDirectoryScope _directoryScope;

    public TempDatabaseScope(string databaseFileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseFileName);
        _directoryScope = TempDirectoryScope.CreateNested("nbn-tests", suppressCleanupFailures: true);
        DatabasePath = _directoryScope.GetPath(databaseFileName);
    }

    public string DatabasePath { get; }

    public void Dispose()
        => _directoryScope.Dispose();
}
