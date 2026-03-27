using System;
using Avalonia;
using Avalonia.Threading;

namespace Nbn.Tools.Workbench.Services;

/// <summary>
/// Dispatches UI work while preserving deterministic inline execution for headless Workbench usage.
/// </summary>
public sealed class UiDispatcher
{
    /// <summary>
    /// Executes an action on the Avalonia UI thread, or inline when the UI loop is unavailable.
    /// </summary>
    public void Post(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);

        if (Dispatcher.UIThread.CheckAccess())
        {
            action();
            return;
        }

        // In headless/tests (or during startup/shutdown), Avalonia can be configured
        // without an active application lifetime. Posting in that state may enqueue work
        // that never runs, so execute inline to keep state transitions deterministic.
        if (Application.Current?.ApplicationLifetime is null)
        {
            action();
            return;
        }

        Dispatcher.UIThread.Post(action);
    }
}
