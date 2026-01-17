using System;
using Avalonia.Threading;

namespace Nbn.Tools.Workbench.Services;

public sealed class UiDispatcher
{
    public void Post(Action action)
    {
        if (Dispatcher.UIThread.CheckAccess())
        {
            action();
            return;
        }

        Dispatcher.UIThread.Post(action);
    }
}
