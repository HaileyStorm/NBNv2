using Nbn.Runtime.Artifacts;

namespace Nbn.Tests.TestSupport;

public sealed class RegisteredArtifactStoreScope : IDisposable
{
    private readonly IDisposable _registration;
    private bool _disposed;

    public RegisteredArtifactStoreScope(string? storeUri = null, bool enableNodeLocalCache = true)
    {
        StoreUri = storeUri ?? $"memory+test://{Guid.NewGuid():N}/artifacts";
        Store = new CountingArtifactStore();
        _registration = ArtifactStoreRegistry.Register(StoreUri, Store, enableNodeLocalCache);
    }

    public string StoreUri { get; }
    public CountingArtifactStore Store { get; }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _registration.Dispose();
        Store.Dispose();
    }
}

public sealed class CountingArtifactStore : IArtifactStore, IDisposable
{
    private readonly TempDirectoryScope _rootPath;
    private readonly LocalArtifactStore _store;
    private int _containsCalls;
    private int _manifestCalls;
    private int _openCalls;
    private int _rangeOpenCalls;
    private int _storeCalls;
    private bool _disposed;

    public CountingArtifactStore()
    {
        _rootPath = TempDirectoryScope.Create("nbn-remote-artifacts", clearSqlitePools: true);
        _store = new LocalArtifactStore(new ArtifactStoreOptions(_rootPath));
    }

    public string RootPath => _rootPath;
    public int StoreCalls => Volatile.Read(ref _storeCalls);
    public int ManifestCalls => Volatile.Read(ref _manifestCalls);
    public int ContainsCalls => Volatile.Read(ref _containsCalls);
    public int OpenCalls => Volatile.Read(ref _openCalls);
    public int RangeOpenCalls => Volatile.Read(ref _rangeOpenCalls);

    public async Task<ArtifactManifest> StoreAsync(
        Stream content,
        string mediaType,
        ArtifactStoreWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _storeCalls);
        return await _store.StoreAsync(content, mediaType, options, cancellationToken).ConfigureAwait(false);
    }

    public async Task<ArtifactManifest?> TryGetManifestAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _manifestCalls);
        return await _store.TryGetManifestAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> ContainsAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _containsCalls);
        return await _store.ContainsAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream?> TryOpenArtifactAsync(Sha256Hash artifactId, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _openCalls);
        return await _store.TryOpenArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream?> TryOpenArtifactRangeAsync(Sha256Hash artifactId, long offset, long length, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _rangeOpenCalls);
        return await _store.TryOpenArtifactRangeAsync(artifactId, offset, length, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _rootPath.Dispose();
    }
}
