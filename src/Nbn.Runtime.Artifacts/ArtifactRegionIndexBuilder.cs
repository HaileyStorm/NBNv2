using Nbn.Shared;
using Nbn.Shared.Format;

namespace Nbn.Runtime.Artifacts;

public static class ArtifactRegionIndexBuilder
{
    private const string NbnMediaType = "application/x-nbn";
    private const string CanonicalMagic = "NBN2";
    private const ushort CanonicalVersion = 2;
    private const byte CanonicalEndianness = 1;
    private const byte CanonicalHeaderBytesPow2 = 10;

    public static IReadOnlyList<ArtifactRegionIndexEntry> BuildFromNbnBytes(ReadOnlySpan<byte> data)
    {
        var header = NbnBinary.ReadNbnHeader(data);
        return BuildFromNbnHeader(header);
    }

    public static IReadOnlyList<ArtifactRegionIndexEntry> BuildFromNbnHeader(NbnHeaderV2 header)
    {
        var index = new List<ArtifactRegionIndexEntry>();
        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var entry = header.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                continue;
            }

            var length = NbnBinary.GetNbnRegionSectionSize(entry.NeuronSpan, entry.TotalAxons, header.AxonStride);
            index.Add(new ArtifactRegionIndexEntry(regionId, checked((long)entry.Offset), length));
        }

        return index;
    }

    internal static ArtifactStoreWriteOptions? PopulateIfMissing(Stream content, string mediaType, ArtifactStoreWriteOptions? options)
    {
        if (options?.RegionIndex is not null
            || !string.Equals(mediaType, NbnMediaType, StringComparison.OrdinalIgnoreCase)
            || !content.CanSeek)
        {
            return options;
        }

        try
        {
            var originalPosition = content.Position;
            try
            {
                if (content.Length - originalPosition < NbnBinary.NbnHeaderBytes)
                {
                    return options;
                }

                var headerBytes = new byte[NbnBinary.NbnHeaderBytes];
                content.Seek(originalPosition, SeekOrigin.Begin);
                ReadExactly(content, headerBytes);

                if (!TryBuildFromHeaderBytes(headerBytes, content.Length - originalPosition, out var regionIndex))
                {
                    return options;
                }

                return regionIndex.Count == 0
                    ? options
                    : new ArtifactStoreWriteOptions
                    {
                        RegionIndex = regionIndex
                    };
            }
            finally
            {
                content.Seek(originalPosition, SeekOrigin.Begin);
            }
        }
        catch (NotSupportedException)
        {
            return options;
        }
    }

    private static bool TryBuildFromHeaderBytes(
        ReadOnlySpan<byte> headerBytes,
        long streamLength,
        out IReadOnlyList<ArtifactRegionIndexEntry> regionIndex)
    {
        regionIndex = Array.Empty<ArtifactRegionIndexEntry>();

        try
        {
            var header = NbnBinary.ReadNbnHeader(headerBytes);
            if (!IsCanonicalHeader(header))
            {
                return false;
            }

            var built = BuildFromNbnHeader(header);
            if (!IsWithinStreamBounds(built, streamLength))
            {
                return false;
            }

            regionIndex = built;
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    private static bool IsCanonicalHeader(NbnHeaderV2 header)
        => string.Equals(header.Magic, CanonicalMagic, StringComparison.Ordinal)
           && header.Version == CanonicalVersion
           && header.Endianness == CanonicalEndianness
           && header.HeaderBytesPow2 == CanonicalHeaderBytesPow2
           && header.AxonStride > 0
           && header.Regions.Length == NbnConstants.RegionCount;

    private static bool IsWithinStreamBounds(IReadOnlyList<ArtifactRegionIndexEntry> regionIndex, long streamLength)
    {
        foreach (var entry in regionIndex)
        {
            if (entry.Offset < NbnBinary.NbnHeaderBytes
                || entry.Length < 0
                || entry.Offset > streamLength
                || entry.Length > streamLength - entry.Offset)
            {
                return false;
            }
        }

        return true;
    }

    private static void ReadExactly(Stream stream, byte[] buffer)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = stream.Read(buffer, offset, buffer.Length - offset);
            if (read == 0)
            {
                throw new EndOfStreamException($"Unable to read the {buffer.Length}-byte NBN header.");
            }

            offset += read;
        }
    }
}
