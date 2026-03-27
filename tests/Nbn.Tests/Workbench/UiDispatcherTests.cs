using Avalonia;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tests.Workbench;

public sealed class UiDispatcherTests
{
    [Fact]
    public void Post_WithoutApplicationLifetime_ExecutesInlineOnCallingThread()
    {
        AvaloniaTestHost.EnsureInitialized();
        Assert.Null(Application.Current?.ApplicationLifetime);

        var dispatcher = new UiDispatcher();
        var callingThreadId = Environment.CurrentManagedThreadId;
        var executedThreadId = -1;

        dispatcher.Post(() => executedThreadId = Environment.CurrentManagedThreadId);

        Assert.Equal(callingThreadId, executedThreadId);
    }
}
