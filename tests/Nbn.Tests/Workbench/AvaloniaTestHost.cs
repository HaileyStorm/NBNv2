using Avalonia;
using Avalonia.Controls;
using Avalonia.Headless;
using System.Collections.Concurrent;

namespace Nbn.Tests.Workbench;

internal static class AvaloniaTestHost
{
    private static readonly object InitGate = new();
    private static readonly ManualResetEventSlim UiReady = new(false);
    private static readonly BlockingCollection<UiWorkItem> UiQueue = new();
    private static Thread? s_uiThread;
    private static Exception? s_initFailure;

    private sealed record UiWorkItem(Func<object?> Action, TaskCompletionSource<object?> Completion);

    public static void EnsureInitialized()
    {
        lock (InitGate)
        {
            if (s_uiThread is not null)
            {
                ThrowIfInitFailed();
                return;
            }

            s_uiThread = new Thread(() =>
            {
                try
                {
                    AppBuilder.Configure<Application>()
                        .UseHeadless(new AvaloniaHeadlessPlatformOptions())
                        .SetupWithoutStarting();
                }
                catch (Exception ex)
                {
                    s_initFailure = ex;
                }
                finally
                {
                    UiReady.Set();
                }

                if (s_initFailure is not null)
                {
                    return;
                }

                foreach (var work in UiQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        work.Completion.SetResult(work.Action());
                    }
                    catch (Exception ex)
                    {
                        work.Completion.SetException(ex);
                    }
                }
            })
            {
                IsBackground = true,
                Name = "Nbn.Tests.Avalonia.UI",
            };

            s_uiThread.Start();
        }

        UiReady.Wait();
        ThrowIfInitFailed();
    }

    private static void ThrowIfInitFailed()
    {
        if (s_initFailure is not null)
        {
            throw new InvalidOperationException("Failed to initialize Avalonia test host.", s_initFailure);
        }
    }

    public static void RunOnUiThread(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        RunOnUiThread(() =>
        {
            action();
            return true;
        });
    }

    public static T RunOnUiThread<T>(Func<T> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        EnsureInitialized();

        if (Thread.CurrentThread == s_uiThread)
        {
            return action();
        }

        var completion = new TaskCompletionSource<object?>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        UiQueue.Add(new UiWorkItem(() => action(), completion));
        return (T)completion.Task.GetAwaiter().GetResult()!;
    }
}
