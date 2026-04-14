using System.Data;
using System.Globalization;
using Dapper;
using Microsoft.Data.Sqlite;

namespace Nbn.Runtime.SettingsMonitor;

/// <summary>
/// Persists SettingsMonitor node, brain, and operator-setting state in SQLite.
/// </summary>
public sealed partial class SettingsMonitorStore
{
    static SettingsMonitorStore()
    {
        RegisterTypeHandlers();
    }

    private readonly string _databasePath;
    private readonly string _connectionString;
    private readonly TimeProvider _timeProvider;
    private const long StorageMaintenanceMinimumBytes = 512L * 1024L * 1024L;
    private const double StorageMaintenanceFreePageRatio = 0.35d;
    private static bool _handlersRegistered;
    private static readonly object _handlerLock = new();

    /// <summary>
    /// Creates a SQLite-backed store rooted at the provided database path.
    /// </summary>
    /// <param name="databasePath">Absolute or relative path to the SettingsMonitor database file.</param>
    /// <param name="timeProvider">Optional clock used for persisted timestamps.</param>
    public SettingsMonitorStore(string databasePath, TimeProvider? timeProvider = null)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
        {
            throw new ArgumentException("Database path is required.", nameof(databasePath));
        }

        _databasePath = databasePath;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();
    }

    private static void RegisterTypeHandlers()
    {
        lock (_handlerLock)
        {
            if (_handlersRegistered)
            {
                return;
            }

            SqlMapper.AddTypeHandler(new GuidTextHandler());
            SqlMapper.AddTypeHandler(new NullableGuidTextHandler());
            _handlersRegistered = true;
        }
    }

    private sealed class GuidTextHandler : SqlMapper.TypeHandler<Guid>
    {
        public override void SetValue(IDbDataParameter parameter, Guid value)
        {
            parameter.Value = value.ToString("D");
        }

        public override Guid Parse(object value)
        {
            if (value is Guid guid)
            {
                return guid;
            }

            if (value is byte[] bytes && bytes.Length == 16)
            {
                return new Guid(bytes);
            }

            if (value is string text && Guid.TryParse(text, out var parsed))
            {
                return parsed;
            }

            throw new DataException($"Unable to parse Guid from '{value}'.");
        }
    }

    private sealed class NullableGuidTextHandler : SqlMapper.TypeHandler<Guid?>
    {
        public override void SetValue(IDbDataParameter parameter, Guid? value)
        {
            parameter.Value = value?.ToString("D");
        }

        public override Guid? Parse(object value)
        {
            if (value is null || value is DBNull)
            {
                return null;
            }

            if (value is Guid guid)
            {
                return guid;
            }

            if (value is byte[] bytes && bytes.Length == 16)
            {
                return new Guid(bytes);
            }

            if (value is string text && Guid.TryParse(text, out var parsed))
            {
                return parsed;
            }

            throw new DataException($"Unable to parse Guid? from '{value}'.");
        }
    }

    private sealed class TableInfoRow
    {
        public string Name { get; set; } = string.Empty;
    }

    private long NowMs() => _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private static string ToDatabaseId(Guid value) => value.ToString("D");

    private static void ThrowIfEmptyGuid(Guid value, string parameterName, string displayName)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException($"{displayName} is required.", parameterName);
        }
    }

    private static void ThrowIfNullOrWhiteSpace(string? value, string parameterName, string displayName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{displayName} is required.", parameterName);
        }
    }

    private static int ParseInt(string? value, int fallback)
    {
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static bool ParseBool(string? value, bool fallback)
    {
        return bool.TryParse(value, out var parsed) ? parsed : fallback;
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(PragmaSql, cancellationToken: cancellationToken));
        return connection;
    }

    public async Task RunStorageMaintenanceAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await RunStorageMaintenanceAsync(connection, compactFreePages: true, cancellationToken).ConfigureAwait(false);
    }

    private async Task RunStorageMaintenanceAsync(
        SqliteConnection connection,
        bool compactFreePages,
        CancellationToken cancellationToken)
    {
        await TryCheckpointWalAsync(connection, cancellationToken).ConfigureAwait(false);
        if (!compactFreePages || !File.Exists(_databasePath) || new FileInfo(_databasePath).Length < StorageMaintenanceMinimumBytes)
        {
            return;
        }

        try
        {
            var pageCount = await connection.ExecuteScalarAsync<long>(
                new CommandDefinition("PRAGMA page_count;", cancellationToken: cancellationToken));
            var freeListCount = await connection.ExecuteScalarAsync<long>(
                new CommandDefinition("PRAGMA freelist_count;", cancellationToken: cancellationToken));
            if (pageCount <= 0 || freeListCount <= 0 || freeListCount / (double)pageCount < StorageMaintenanceFreePageRatio)
            {
                return;
            }

            await connection.ExecuteAsync(new CommandDefinition("VACUUM;", cancellationToken: cancellationToken));
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // Storage compaction is opportunistic; maintenance should not fail because a DB is busy.
        }

        await TryCheckpointWalAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    private static async Task TryCheckpointWalAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        try
        {
            var rows = await connection.QueryAsync(
                new CommandDefinition("PRAGMA wal_checkpoint(TRUNCATE);", cancellationToken: cancellationToken));
            _ = rows.ToArray();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // Checkpointing is best-effort; SQLite may refuse while another connection is active.
        }
    }
}
