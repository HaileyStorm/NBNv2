using System.Diagnostics;
using System.Text.Json;
using Nbn.Tools.PerfProbe;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

if (args.Length == 0 || IsHelpToken(args[0]))
{
    PrintHelp();
    return;
}

var command = args[0].Trim().ToLowerInvariant();
var remaining = args.Skip(1).ToArray();
var outputDirectory = Path.GetFullPath(
    GetArg(remaining, "--output-dir")
    ?? Path.Combine(Environment.CurrentDirectory, $"perf-probe-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}"));
var jsonOnly = HasFlag(remaining, "--json");
var openReport = !HasFlag(remaining, "--no-open-report");

try
{
    PerfReport report = command switch
    {
        "all" or "worker-profile" or "localhost-stress" => await PerfProbeRunner.RunAsync(
            command,
            PerfProbeDefaults.Create(outputDirectory)),
        "current-system" => await PerfProbeRunner.RunCurrentSystemProfileAsync(
            new CurrentSystemProfileConfig(
                SettingsHost: GetArg(remaining, "--settings-host") ?? throw new InvalidOperationException("--settings-host is required."),
                SettingsPort: GetIntArg(remaining, "--settings-port") ?? throw new InvalidOperationException("--settings-port is required."),
                SettingsName: GetArg(remaining, "--settings-name") ?? "SettingsMonitor",
                BindHost: GetArg(remaining, "--bind-host") ?? "127.0.0.1",
                BindPort: GetIntArg(remaining, "--bind-port") ?? 12110)),
        _ => throw new InvalidOperationException($"Unknown command '{command}'.")
    };

    await PerfReportWriter.WriteAsync(report, outputDirectory);
    var htmlReportPath = Path.Combine(outputDirectory, "perf-report.html");
    if (openReport)
    {
        TryOpenReport(htmlReportPath);
    }

    var payload = new
    {
        command,
        output_directory = outputDirectory,
        scenario_count = report.Scenarios.Count,
        statuses = report.Scenarios
            .GroupBy(static scenario => scenario.Status)
            .ToDictionary(group => group.Key.ToString(), group => group.Count())
    };

    var json = JsonSerializer.Serialize(payload);
    Console.WriteLine(json);
    if (!jsonOnly)
    {
        Console.WriteLine($"Perf probe complete. Reports: {outputDirectory}");
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine($"perf-probe failed: {ex.GetBaseException().Message}");
    Environment.ExitCode = 1;
}

static bool IsHelpToken(string token)
    => token.Equals("--help", StringComparison.OrdinalIgnoreCase)
       || token.Equals("-h", StringComparison.OrdinalIgnoreCase)
       || token.Equals("help", StringComparison.OrdinalIgnoreCase);

static string? GetArg(string[] args, string key)
{
    for (var i = 0; i < args.Length; i++)
    {
        if (args[i].Equals(key, StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        {
            return args[i + 1];
        }

        if (args[i].StartsWith(key + "=", StringComparison.OrdinalIgnoreCase))
        {
            return args[i][(key.Length + 1)..];
        }
    }

    return null;
}

static int? GetIntArg(string[] args, string key)
{
    var raw = GetArg(args, key);
    return int.TryParse(raw, out var value) ? value : null;
}

static bool HasFlag(string[] args, string key)
    => args.Any(arg => arg.Equals(key, StringComparison.OrdinalIgnoreCase));

static void TryOpenReport(string htmlReportPath)
{
    if (!File.Exists(htmlReportPath))
    {
        return;
    }

    try
    {
        Process.Start(new ProcessStartInfo
        {
            FileName = htmlReportPath,
            UseShellExecute = true
        });
    }
    catch
    {
    }
}

static void PrintHelp()
{
    Console.WriteLine("NBN performance probe");
    Console.WriteLine();
    Console.WriteLine("Commands:");
    Console.WriteLine("  all");
    Console.WriteLine("  worker-profile");
    Console.WriteLine("  localhost-stress");
    Console.WriteLine("  current-system --settings-host <host> --settings-port <port> [--settings-name <actor>]");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine("  --output-dir <path>   Write JSON/CSV/Markdown/HTML reports to this directory.");
    Console.WriteLine("  --bind-host <host>    Current-system mode: local bind host for the probe actor client (default 127.0.0.1).");
    Console.WriteLine("  --bind-port <port>    Current-system mode: local bind port for the probe actor client (default 12110).");
    Console.WriteLine("  --json                Emit only the JSON completion payload.");
    Console.WriteLine("  --no-open-report      Do not open the generated HTML report when the run completes.");
}
