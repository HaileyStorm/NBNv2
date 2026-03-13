using System.Collections.Concurrent;
using System.Text.Json;

namespace Nbn.Runtime.Artifacts;

public static class ArtifactStoreRegistry
{
    private sealed record Registration(IArtifactStore Store, bool EnableNodeLocalCache);

    private static readonly ConcurrentDictionary<string, Registration> Registrations = new(StringComparer.Ordinal);
    private static readonly object EnvironmentMapGate = new();
    private static string? _lastEnvironmentMapRaw;
    private static IReadOnlyDictionary<string, string> _lastEnvironmentMap = new Dictionary<string, string>(StringComparer.Ordinal);

    public static IDisposable Register(string storeUri, IArtifactStore store, bool enableNodeLocalCache = true)
    {
        if (store is null)
        {
            throw new ArgumentNullException(nameof(store));
        }

        var key = NormalizeStoreUri(storeUri);
        var registration = new Registration(store, enableNodeLocalCache);
        Registrations[key] = registration;
        return new RegistrationLease(key, registration);
    }

    public static bool Unregister(string storeUri)
    {
        var key = NormalizeStoreUri(storeUri);
        return Registrations.TryRemove(key, out _);
    }

    public static bool TryResolve(string storeUri, out IArtifactStore store, out bool enableNodeLocalCache)
    {
        var key = NormalizeStoreUri(storeUri);
        if (Registrations.TryGetValue(key, out var registration))
        {
            store = registration.Store;
            enableNodeLocalCache = registration.EnableNodeLocalCache;
            return true;
        }

        var environmentMap = GetEnvironmentMap();
        if (environmentMap.TryGetValue(key, out var mappedRootPath))
        {
            store = new LocalArtifactStore(new ArtifactStoreOptions(mappedRootPath));
            enableNodeLocalCache = true;
            return true;
        }

        store = default!;
        enableNodeLocalCache = false;
        return false;
    }

    public static void Clear()
    {
        Registrations.Clear();
    }

    internal static string NormalizeStoreUri(string storeUri)
    {
        if (string.IsNullOrWhiteSpace(storeUri))
        {
            throw new ArgumentException("Artifact store URI is required.", nameof(storeUri));
        }

        var trimmed = storeUri.Trim();
        return Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            ? uri.AbsoluteUri
            : trimmed;
    }

    private static IReadOnlyDictionary<string, string> GetEnvironmentMap()
    {
        var raw = Environment.GetEnvironmentVariable("NBN_ARTIFACT_STORE_URI_MAP");
        lock (EnvironmentMapGate)
        {
            if (string.Equals(raw, _lastEnvironmentMapRaw, StringComparison.Ordinal))
            {
                return _lastEnvironmentMap;
            }

            _lastEnvironmentMapRaw = raw;
            _lastEnvironmentMap = ParseEnvironmentMap(raw);
            return _lastEnvironmentMap;
        }
    }

    private static IReadOnlyDictionary<string, string> ParseEnvironmentMap(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        try
        {
            var parsed = JsonSerializer.Deserialize<Dictionary<string, string>>(raw);
            if (parsed is null || parsed.Count == 0)
            {
                return new Dictionary<string, string>(StringComparer.Ordinal);
            }

            var normalized = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var pair in parsed)
            {
                if (string.IsNullOrWhiteSpace(pair.Key) || string.IsNullOrWhiteSpace(pair.Value))
                {
                    continue;
                }

                normalized[NormalizeStoreUri(pair.Key)] = pair.Value.Trim();
            }

            return normalized;
        }
        catch (JsonException)
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }
    }

    private sealed class RegistrationLease : IDisposable
    {
        private readonly string _key;
        private readonly Registration _registration;
        private bool _disposed;

        public RegistrationLease(string key, Registration registration)
        {
            _key = key;
            _registration = registration;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (Registrations.TryGetValue(_key, out var existing) && ReferenceEquals(existing, _registration))
            {
                Registrations.TryRemove(_key, out _);
            }
        }
    }
}
