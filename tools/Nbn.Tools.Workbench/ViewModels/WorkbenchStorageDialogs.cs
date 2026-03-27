using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Platform.Storage;

namespace Nbn.Tools.Workbench.ViewModels;

/// <summary>
/// Provides headless-safe access to Workbench file-picker and export helpers.
/// </summary>
internal static class WorkbenchStorageDialogs
{
    public static async Task<IStorageFile?> PickOpenFileAsync(string title, string? filterName = null, string? extension = null)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var options = new FilePickerOpenOptions
        {
            Title = title,
            AllowMultiple = false
        };
        if (!string.IsNullOrWhiteSpace(filterName) && !string.IsNullOrWhiteSpace(extension))
        {
            options.FileTypeFilter =
            [
                new FilePickerFileType(filterName)
                {
                    Patterns = [$"*.{extension.TrimStart('.')}"]
                }
            ];
        }

        var results = await provider.OpenFilePickerAsync(options).ConfigureAwait(false);
        return results.FirstOrDefault();
    }

    public static async Task<IStorageFile?> PickSaveFileAsync(string title, string filterName, string extension, string? suggestedName)
    {
        var provider = GetStorageProvider();
        if (provider is null)
        {
            return null;
        }

        var normalizedExtension = extension.TrimStart('.');
        var options = new FilePickerSaveOptions
        {
            Title = title,
            DefaultExtension = normalizedExtension,
            SuggestedFileName = suggestedName,
            FileTypeChoices =
            [
                new FilePickerFileType(filterName)
                {
                    Patterns = [$"*.{normalizedExtension}"]
                }
            ]
        };

        return await provider.SaveFilePickerAsync(options).ConfigureAwait(false);
    }

    public static async Task WriteAllTextAsync(IStorageFile file, string content)
    {
        await using var stream = await file.OpenWriteAsync().ConfigureAwait(false);
        stream.SetLength(0);
        await using var writer = new StreamWriter(stream, new UTF8Encoding(false));
        await writer.WriteAsync(content).ConfigureAwait(false);
        await writer.FlushAsync().ConfigureAwait(false);
        await stream.FlushAsync().ConfigureAwait(false);
    }

    public static string FormatPath(IStorageItem item)
        => item.Path?.LocalPath ?? item.Path?.ToString() ?? item.Name;

    private static IStorageProvider? GetStorageProvider()
    {
        var window = GetMainWindow();
        return window?.StorageProvider;
    }

    private static Window? GetMainWindow()
        => Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop
            ? desktop.MainWindow
            : null;
}
