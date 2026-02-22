using Google.Protobuf;
using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.RegionHost;
using Nbn.Tests.Format;
using SharedAddress32 = Nbn.Shared.Addressing.Address32;

namespace Nbn.Tests.RegionHost;

public class RegionShardArtifactLoaderStateTests
{
    [Fact]
    public async Task LoadAsync_WithSnapshotOverlay_PopulatesStrengthMetadataAndDeterministicTuple()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-regionhost-loader-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var richNbs = NbnTestVectors.CreateRichNbsVector(richNbn);

            var nbnManifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");
            var nbsManifest = await store.StoreAsync(new MemoryStream(richNbs.Bytes), "application/x-nbs");

            var load = await RegionShardArtifactLoader.LoadAsync(
                store,
                BuildArtifactRef(nbnManifest),
                BuildArtifactRef(nbsManifest),
                regionId: 0,
                neuronStart: 0,
                neuronCount: 3,
                expectedBrainId: NbnTestVectors.SampleBrainId);

            var axons = load.State.Axons;
            Assert.Equal(NbnTestVectors.SampleBrainSeed, load.State.BrainSeed);
            Assert.Equal(new byte[] { 10, 12, 5 }, axons.BaseStrengthCodes);
            Assert.Equal(new byte[] { 19, 12, 5 }, axons.RuntimeStrengthCodes);
            Assert.Equal(new[] { true, false, false }, axons.HasRuntimeOverlay);

            Assert.Equal(
                new[] { SharedAddress32.From(0, 0).Value, SharedAddress32.From(0, 0).Value, SharedAddress32.From(0, 2).Value },
                axons.FromAddress32);
            Assert.Equal(
                new[] { SharedAddress32.From(1, 1).Value, SharedAddress32.From(31, 0).Value, SharedAddress32.From(1, 3).Value },
                axons.ToAddress32);

            for (var i = 0; i < axons.Count; i++)
            {
                var expected = load.Header.Quantization.Strength.Decode(axons.RuntimeStrengthCodes[i], 5);
                Assert.Equal(expected, axons.Strengths[i], precision: 6);
            }

            var deterministic = load.State.GetDeterministicRngInput(tickId: 444, axonIndex: 0);
            Assert.Equal(NbnTestVectors.SampleBrainSeed, deterministic.BrainSeed);
            Assert.Equal((ulong)444, deterministic.TickId);
            Assert.Equal(SharedAddress32.From(0, 0).Value, deterministic.FromAddress32);
            Assert.Equal(SharedAddress32.From(1, 1).Value, deterministic.ToAddress32);
            Assert.Equal(
                RegionShardDeterministicRngInput.MixToU64(
                    deterministic.BrainSeed,
                    deterministic.TickId,
                    deterministic.FromAddress32,
                    deterministic.ToAddress32),
                deterministic.ToSeed());
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task LoadAsync_WithoutSnapshot_UsesBaseStrengthCodesWithoutOverlayFlags()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-regionhost-loader-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var richNbn = NbnTestVectors.CreateRichNbnVector();
            var nbnManifest = await store.StoreAsync(new MemoryStream(richNbn.Bytes), "application/x-nbn");

            var load = await RegionShardArtifactLoader.LoadAsync(
                store,
                BuildArtifactRef(nbnManifest),
                nbsRef: null,
                regionId: 1,
                neuronStart: 0,
                neuronCount: 4);

            var axons = load.State.Axons;
            Assert.Equal(axons.BaseStrengthCodes, axons.RuntimeStrengthCodes);
            Assert.All(axons.HasRuntimeOverlay, hasOverlay => Assert.False(hasOverlay));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    private static ArtifactRef BuildArtifactRef(ArtifactManifest manifest)
    {
        return new ArtifactRef
        {
            Sha256 = new Sha256 { Value = ByteString.CopyFrom(manifest.ArtifactId.Bytes.ToArray()) },
            MediaType = manifest.MediaType,
            SizeBytes = (ulong)manifest.ByteLength
        };
    }
}
