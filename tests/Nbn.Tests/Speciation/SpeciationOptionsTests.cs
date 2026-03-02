using Nbn.Runtime.Speciation;

namespace Nbn.Tests.Speciation;

public sealed class SpeciationOptionsTests
{
    [Fact]
    public void FromArgs_UsesLocalAppDataDefaultDatabasePath_WhenDbNotSpecified()
    {
        var options = SpeciationOptions.FromArgs(Array.Empty<string>());

        var expected = SpeciationOptions.GetDefaultDatabasePath();
        Assert.Equal(expected, options.DatabasePath);
    }

    [Fact]
    public void FromArgs_UsesExplicitDatabasePath_WhenDbArgumentProvided()
    {
        var explicitPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"), "speciation.db");
        var options = SpeciationOptions.FromArgs(new[] { "--db", explicitPath });

        Assert.Equal(explicitPath, options.DatabasePath);
    }
}
