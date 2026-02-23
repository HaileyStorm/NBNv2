using System.Diagnostics.Metrics;

namespace Nbn.Tests.TestSupport;

internal sealed class MeterCollector : IDisposable
{
    private readonly MeterListener _listener;
    private readonly object _gate = new();
    private readonly List<LongSample> _longSamples = new();
    private readonly List<DoubleSample> _doubleSamples = new();

    public MeterCollector(params string[] meterNames)
    {
        var allowedMeters = new HashSet<string>(meterNames ?? [], StringComparer.Ordinal);
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (allowedMeters.Contains(instrument.Meter.Name))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };

        _listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, tags, _) => AddLong(instrument.Name, measurement, tags));
        _listener.SetMeasurementEventCallback<int>(
            (instrument, measurement, tags, _) => AddLong(instrument.Name, measurement, tags));
        _listener.SetMeasurementEventCallback<double>(
            (instrument, measurement, tags, _) => AddDouble(instrument.Name, measurement, tags));
        _listener.Start();
    }

    public long SumLong(string instrumentName, string? tagKey = null, string? tagValue = null)
    {
        lock (_gate)
        {
            return _longSamples
                .Where(sample => string.Equals(sample.Name, instrumentName, StringComparison.Ordinal))
                .Where(sample => MatchesTag(sample.Tags, tagKey, tagValue))
                .Sum(sample => sample.Value);
        }
    }

    public int CountDouble(string instrumentName, string? tagKey = null, string? tagValue = null)
    {
        lock (_gate)
        {
            return _doubleSamples.Count(sample =>
                string.Equals(sample.Name, instrumentName, StringComparison.Ordinal)
                && MatchesTag(sample.Tags, tagKey, tagValue));
        }
    }

    public void Dispose()
    {
        _listener.Dispose();
    }

    private void AddLong(string instrumentName, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        lock (_gate)
        {
            _longSamples.Add(new LongSample(instrumentName, measurement, CaptureTags(tags)));
        }
    }

    private void AddDouble(string instrumentName, double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        lock (_gate)
        {
            _doubleSamples.Add(new DoubleSample(instrumentName, measurement, CaptureTags(tags)));
        }
    }

    private static Dictionary<string, string> CaptureTags(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < tags.Length; i++)
        {
            var tag = tags[i];
            if (string.IsNullOrWhiteSpace(tag.Key))
            {
                continue;
            }

            map[tag.Key] = tag.Value?.ToString() ?? string.Empty;
        }

        return map;
    }

    private static bool MatchesTag(
        IReadOnlyDictionary<string, string> tags,
        string? tagKey,
        string? tagValue)
    {
        if (string.IsNullOrWhiteSpace(tagKey))
        {
            return true;
        }

        if (!tags.TryGetValue(tagKey, out var value))
        {
            return false;
        }

        if (tagValue is null)
        {
            return true;
        }

        return string.Equals(value, tagValue, StringComparison.Ordinal);
    }

    private sealed record LongSample(string Name, long Value, IReadOnlyDictionary<string, string> Tags);
    private sealed record DoubleSample(string Name, double Value, IReadOnlyDictionary<string, string> Tags);
}
