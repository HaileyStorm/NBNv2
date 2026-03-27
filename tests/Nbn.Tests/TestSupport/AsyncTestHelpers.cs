using System.Diagnostics;
using Xunit.Sdk;

namespace Nbn.Tests.TestSupport;

internal static class AsyncTestHelpers
{
    public static Task WaitForAsync(
        Func<bool> predicate,
        int timeoutMs = 3000,
        int pollIntervalMs = 20,
        string? failureMessage = null)
        => WaitForAsync(
            () => Task.FromResult(predicate()),
            TimeSpan.FromMilliseconds(timeoutMs),
            pollIntervalMs,
            failureMessage);

    public static Task WaitForAsync(
        Func<Task<bool>> predicate,
        int timeoutMs,
        int pollIntervalMs = 20,
        string? failureMessage = null)
        => WaitForAsync(
            predicate,
            TimeSpan.FromMilliseconds(timeoutMs),
            pollIntervalMs,
            failureMessage);

    public static async Task WaitForAsync(
        Func<Task<bool>> predicate,
        TimeSpan timeout,
        int pollIntervalMs = 20,
        string? failureMessage = null)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(pollIntervalMs).ConfigureAwait(false);
        }

        throw new XunitException(failureMessage ?? $"Condition was not satisfied within {timeout}.");
    }
}
