using System.Xml.Linq;

namespace Nbn.Tests.Dist;

public class ReleaseBrandingTests
{
    [Fact]
    public void ExecutableProjects_Inherit_PrimaryWindowsApplicationIcon()
    {
        var repoRoot = FindRepoRoot();
        var propsPath = Path.Combine(repoRoot, "Directory.Build.props");
        var document = XDocument.Load(propsPath);

        var applicationIcon = document
            .Descendants()
            .FirstOrDefault(element => element.Name.LocalName == "ApplicationIcon");

        Assert.NotNull(applicationIcon);
        Assert.Equal("$(NbnPrimaryIconIco)", applicationIcon!.Value.Trim());
        Assert.True(File.Exists(Path.Combine(repoRoot, "docs", "branding", "ico", "nbn-soft-gold-right-n-icon.ico")));
    }

    [Theory]
    [InlineData("tools/dist/packaging/windows/nbn-suite.iss", @"SetupIconFile={#BrandingIconFile}")]
    [InlineData("tools/dist/packaging/windows/nbn-worker.iss", @"SetupIconFile={#BrandingIconFile}")]
    [InlineData("tools/dist/packaging/windows/nbn-suite.iss", @"UninstallDisplayIcon={app}\apps\workbench\Nbn.Tools.Workbench.exe")]
    [InlineData("tools/dist/packaging/windows/nbn-worker.iss", @"UninstallDisplayIcon={app}\services\worker\Nbn.Runtime.WorkerNode.exe")]
    public void WindowsInstallerTemplates_Use_Branding_Icons(string relativePath, string expectedText)
    {
        var repoRoot = FindRepoRoot();
        var templatePath = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        var text = File.ReadAllText(templatePath);

        Assert.Contains(expectedText, text, StringComparison.Ordinal);
    }

    [Fact]
    public void LinuxInstallerDesktopEntry_PackagingAssets_Are_Configured()
    {
        var repoRoot = FindRepoRoot();
        var desktopTemplatePath = Path.Combine(repoRoot, "tools", "dist", "packaging", "linux", "nbn-workbench.desktop.template");
        var desktopTemplate = File.ReadAllText(desktopTemplatePath);

        Assert.Contains("Exec=__INSTALL_ROOT__/bin/nbn-workbench", desktopTemplate, StringComparison.Ordinal);
        Assert.Contains("Icon=nbn-workbench", desktopTemplate, StringComparison.Ordinal);
        Assert.Contains("Terminal=false", desktopTemplate, StringComparison.Ordinal);
        Assert.True(File.Exists(Path.Combine(repoRoot, "docs", "branding", "png", "nbn-soft-gold-right-n-icon.png")));

        var installerTemplate = File.ReadAllText(Path.Combine(repoRoot, "tools", "dist", "packaging", "linux", "install-template.sh"));
        Assert.Contains("--desktop-dir", installerTemplate, StringComparison.Ordinal);
        Assert.Contains("--icon-dir", installerTemplate, StringComparison.Ordinal);
        Assert.Contains("nbn-workbench.desktop.template", installerTemplate, StringComparison.Ordinal);
        Assert.Contains("nbn-workbench.png", installerTemplate, StringComparison.Ordinal);
    }

    private static string FindRepoRoot()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "NBNv2.sln"))
                && File.Exists(Path.Combine(current.FullName, "Directory.Build.props")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new InvalidOperationException("Unable to locate repo root.");
    }
}
