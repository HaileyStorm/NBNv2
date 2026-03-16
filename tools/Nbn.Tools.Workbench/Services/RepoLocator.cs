using System;
using System.IO;

namespace Nbn.Tools.Workbench.Services;

public static class RepoLocator
{
    public static DirectoryInfo? FindRepoRoot(string? startDirectory = null)
    {
        var current = CreateStartDirectory(startDirectory);
        for (var i = 0; i < 8 && current is not null; i++)
        {
            if (LooksLikeRepoRoot(current))
            {
                return current;
            }

            current = current.Parent;
        }

        return null;
    }

    public static bool IsLocalSourceCheckout(string? startDirectory = null)
        => FindRepoRoot(startDirectory) is not null;

    public static FileInfo? FindRuntimeManifest(string? startDirectory = null)
    {
        var current = CreateStartDirectory(startDirectory);
        for (var i = 0; i < 8 && current is not null; i++)
        {
            var candidate = Path.Combine(current.FullName, "runtime-manifest.json");
            if (File.Exists(candidate))
            {
                return new FileInfo(candidate);
            }

            current = current.Parent;
        }

        return null;
    }

    public static string? ResolvePathFromRepo(params string[] segments)
    {
        var root = FindRepoRoot();
        if (root is null)
        {
            return null;
        }

        var path = root.FullName;
        foreach (var segment in segments)
        {
            path = Path.Combine(path, segment);
        }

        return path;
    }

    private static DirectoryInfo CreateStartDirectory(string? startDirectory)
    {
        var resolved = string.IsNullOrWhiteSpace(startDirectory)
            ? AppContext.BaseDirectory
            : startDirectory;
        return new DirectoryInfo(resolved);
    }

    private static bool LooksLikeRepoRoot(DirectoryInfo directory)
    {
        var root = directory.FullName;
        return File.Exists(Path.Combine(root, "NBNv2.sln"))
               && File.Exists(Path.Combine(root, "Directory.Build.props"))
               && Directory.Exists(Path.Combine(root, "tools", "Nbn.Tools.Workbench"))
               && Directory.Exists(Path.Combine(root, "src", "Nbn.Runtime.SettingsMonitor"));
    }
}
