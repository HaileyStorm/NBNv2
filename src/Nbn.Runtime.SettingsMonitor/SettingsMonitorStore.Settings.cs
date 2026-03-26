using Dapper;

namespace Nbn.Runtime.SettingsMonitor;

public sealed partial class SettingsMonitorStore
{
    private const string UpsertSettingSql = """
INSERT INTO settings (key, value, updated_ms)
VALUES (@key, @value, @updated_ms)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_ms = excluded.updated_ms
WHERE excluded.updated_ms >= settings.updated_ms;
""";

    private const string InsertSettingIfMissingSql = """
INSERT INTO settings (key, value, updated_ms)
VALUES (@key, @value, @updated_ms)
ON CONFLICT(key) DO NOTHING;
""";

    private const string GetSettingSql = """
SELECT key AS Key, value AS Value, updated_ms AS UpdatedMs
FROM settings
WHERE key = @key;
""";

    private const string ListSettingsSql = """
SELECT key AS Key, value AS Value, updated_ms AS UpdatedMs
FROM settings
ORDER BY key;
""";

    /// <summary>
    /// Upserts a single operator setting when the write is not older than the persisted value.
    /// </summary>
    /// <param name="key">Setting key.</param>
    /// <param name="value">Setting value.</param>
    /// <param name="updatedMs">Optional update timestamp in milliseconds.</param>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task SetSettingAsync(
        string key,
        string value,
        long? updatedMs = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfNullOrWhiteSpace(key, nameof(key), "Setting key");

        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }

        var timeMs = updatedMs ?? NowMs();
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertSettingSql,
                new
                {
                    key,
                    value,
                    updated_ms = timeMs
                },
                cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Inserts the canonical default settings when they are missing from the store.
    /// </summary>
    /// <param name="cancellationToken">Cancels the database write.</param>
    public async Task EnsureDefaultSettingsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var nowMs = NowMs();

        foreach (var setting in SettingsMonitorDefaults.DefaultSettings)
        {
            await connection.ExecuteAsync(
                new CommandDefinition(
                    InsertSettingIfMissingSql,
                    new
                    {
                        key = setting.Key,
                        value = setting.Value,
                        updated_ms = nowMs
                    },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// Reads artifact compression settings, applying SettingsMonitor defaults for missing or invalid values.
    /// </summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<ArtifactCompressionSettings> GetArtifactCompressionSettingsAsync(
        CancellationToken cancellationToken = default)
    {
        var defaults = SettingsMonitorDefaults.ArtifactCompressionDefaults;

        var kindEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionKindKey, cancellationToken);
        var levelEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionLevelKey, cancellationToken);
        var minBytesEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionMinBytesKey, cancellationToken);
        var onlyIfSmallerEntry = await GetSettingAsync(SettingsMonitorDefaults.ArtifactChunkCompressionOnlyIfSmallerKey, cancellationToken);

        var kind = string.IsNullOrWhiteSpace(kindEntry?.Value)
            ? defaults.Kind
            : kindEntry.Value.Trim();

        var level = ParseInt(levelEntry?.Value, defaults.Level);
        var minBytes = ParseInt(minBytesEntry?.Value, defaults.MinBytes);
        var onlyIfSmaller = ParseBool(onlyIfSmallerEntry?.Value, defaults.OnlyIfSmaller);

        return new ArtifactCompressionSettings(kind, level, minBytes, onlyIfSmaller);
    }

    /// <summary>
    /// Looks up a single persisted setting row.
    /// </summary>
    /// <param name="key">Setting key to fetch.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<SettingEntry?> GetSettingAsync(
        string key,
        CancellationToken cancellationToken = default)
    {
        ThrowIfNullOrWhiteSpace(key, nameof(key), "Setting key");

        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<SettingEntry>(
            new CommandDefinition(GetSettingSql, new { key }, cancellationToken: cancellationToken));
    }

    /// <summary>
    /// Lists all persisted setting rows.
    /// </summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    public async Task<IReadOnlyList<SettingEntry>> ListSettingsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var rows = await connection.QueryAsync<SettingEntry>(
            new CommandDefinition(ListSettingsSql, cancellationToken: cancellationToken));
        return rows.AsList();
    }
}
