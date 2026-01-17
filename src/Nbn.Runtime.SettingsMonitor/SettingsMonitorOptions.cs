namespace Nbn.Runtime.SettingsMonitor;

public sealed record SettingsMonitorOptions(string DatabasePath)
{
    public static SettingsMonitorOptions FromArgs(string[] args)
    {
        var dbPath = "settingsmonitor.db";

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg.Equals("--db", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                dbPath = args[++i];
                continue;
            }

            if (arg.StartsWith("--db=", StringComparison.OrdinalIgnoreCase))
            {
                dbPath = arg.Substring("--db=".Length);
            }
        }

        return new SettingsMonitorOptions(dbPath);
    }
}
