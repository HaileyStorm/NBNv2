using Nbn.Shared.Format;

namespace Nbn.Runtime.Artifacts;

public static class ArtifactRegionIndexBuilder
{
    private const string NbnMediaType = "application/x-nbn";

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

                var regionIndex = BuildFromNbnBytes(headerBytes);
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
