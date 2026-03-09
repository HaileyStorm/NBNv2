using System.Collections.Concurrent;
using System.Diagnostics;

namespace Nbn.Tests.TestSupport;

internal sealed class ActivityCollector : IDisposable
{
    private readonly ActivityListener _listener;
    private readonly ConcurrentBag<Activity> _completedActivities = new();

    public ActivityCollector(params string[] sourceNames)
    {
        var allowedSources = new HashSet<string>(sourceNames ?? Array.Empty<string>(), StringComparer.Ordinal);
        _listener = new ActivityListener
        {
            ShouldListenTo = source => allowedSources.Contains(source.Name),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => _completedActivities.Add(activity)
        };

        ActivitySource.AddActivityListener(_listener);
    }

    public IReadOnlyList<Activity> CompletedActivities => _completedActivities.ToArray();

    public void Dispose()
    {
        _listener.Dispose();
    }
}
