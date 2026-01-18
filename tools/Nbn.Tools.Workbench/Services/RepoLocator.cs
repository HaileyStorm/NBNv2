using System;
using System.IO;

namespace Nbn.Tools.Workbench.Services;

public static class RepoLocator
{
    public static DirectoryInfo? FindRepoRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 8 && current is not null; i++)
        {
            if (Directory.Exists(Path.Combine(current.FullName, ".git")))
            {
                return current;
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
}
