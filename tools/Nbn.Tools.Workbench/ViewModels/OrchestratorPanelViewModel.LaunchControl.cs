using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Nbn.Shared;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class OrchestratorPanelViewModel
{
    /// <summary>
    /// Stops locally launched services during Workbench shutdown without re-subscribing UI state.
    /// </summary>
    public async Task StopAllAsyncForShutdown()
    {
        _connections.PropertyChanged -= OnConnectionsPropertyChanged;
        _refreshCts.Cancel();
        _disconnectAll?.Invoke();
        await StopRunnerAsync(_workerRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, _ => { }).ConfigureAwait(false);
        await StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_obsRunner, _ => { }).ConfigureAwait(false);
    }

    private async Task StartSettingsMonitorAsync()
    {
        var configuredDbPath = Connections.SettingsDbPath?.Trim();
        var defaultDbPath = BuildDefaultSettingsDbPath();
        var resolvedDbPath = string.IsNullOrWhiteSpace(configuredDbPath) ? defaultDbPath : configuredDbPath;
        var includeDbArg = !string.IsNullOrWhiteSpace(configuredDbPath)
            && !PathsEqual(configuredDbPath, defaultDbPath);

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.SettingsMonitor");
        if (!TryParsePort(Connections.SettingsPortText, out var port))
        {
            SettingsLaunchStatus = "Invalid Settings port.";
            return;
        }

        var networkArgs = BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.SettingsHost, port);
        var args = includeDbArg
            ? $"--db \"{resolvedDbPath}\" {networkArgs}"
            : networkArgs;
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.SettingsMonitor", args, "SettingsMonitor").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            SettingsLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        var result = await _settingsRunner.StartAsync(startInfo, waitForExit: false, label: "SettingsMonitor");
        SettingsLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("SettingsMonitor", port, result.Message).ConfigureAwait(false)
            : result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartHiveMindAsync()
    {
        if (!TryParsePort(Connections.HiveMindPortText, out var port))
        {
            HiveMindLaunchStatus = "Invalid HiveMind port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.HiveMind");
        var settingsDbPath = ResolveSettingsDbPath();
        var args = $"{BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.HiveMindHost, port)} --settings-db \"{settingsDbPath}\""
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + $" --tick-hz {LocalDefaultTickHz:0.###} --min-tick-hz {LocalDefaultMinTickHz:0.###}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.HiveMind", args, "HiveMind").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            HiveMindLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _hiveMindRunner.StartAsync(startInfo, waitForExit: false, label: "HiveMind");
        HiveMindLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("HiveMind", port, result.Message).ConfigureAwait(false)
            : result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartIoAsync()
    {
        if (!TryParsePort(Connections.IoPortText, out var port))
        {
            IoLaunchStatus = "Invalid IO port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.IO");
        var args = BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.IoHost, port)
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.IO", args, "IoGateway").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            IoLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _ioRunner.StartAsync(startInfo, waitForExit: false, label: "IoGateway");
        IoLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("IoGateway", port, result.Message).ConfigureAwait(false)
            : result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartReproAsync()
    {
        if (!TryParsePort(Connections.ReproPortText, out var reproPort))
        {
            ReproLaunchStatus = "Invalid Reproduction port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Reproduction");
        var args = BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.ReproHost, reproPort)
                 + $" --manager-name {Connections.ReproManager}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.Reproduction", args, "Reproduction").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            ReproLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _reproRunner.StartAsync(startInfo, waitForExit: false, label: "Reproduction");
        ReproLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("Reproduction", reproPort, result.Message).ConfigureAwait(false)
            : result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartSpeciationAsync()
    {
        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Speciation");
        if (!TryParsePort(Connections.SpeciationPortText, out var speciationPort))
        {
            SpeciationLaunchStatus = "Invalid Speciation port.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            SpeciationLaunchStatus = "Invalid Settings port.";
            return;
        }

        var args = BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.SpeciationHost, speciationPort)
                 + $" --manager-name {Connections.SpeciationManager}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.Speciation", args, "Speciation").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            SpeciationLaunchStatus = launch.Message;
            StatusMessage = $"Speciation launch: {launch.Message}";
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _speciationRunner.StartAsync(startInfo, waitForExit: false, label: "Speciation");
        SpeciationLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("Speciation", speciationPort, result.Message).ConfigureAwait(false)
            : result.Message;
        StatusMessage = $"Speciation launch: {SpeciationLaunchStatus}";
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartWorkerAsync()
    {
        if (!TryParsePort(Connections.WorkerPortText, out var workerPort))
        {
            WorkerLaunchStatus = "Invalid worker port.";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            WorkerLaunchStatus = "Invalid Settings port.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerCpuLimitPercentText, out var workerCpuLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker CPU limit.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerRamLimitPercentText, out var workerRamLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker RAM limit.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerGpuLimitPercentText, out var workerGpuLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker GPU limit.";
            return;
        }

        if (!TryParsePercent(Connections.WorkerVramLimitPercentText, out var workerVramLimitPercent))
        {
            WorkerLaunchStatus = "Invalid worker VRAM limit.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.WorkerNode");
        var args = BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.WorkerHost, workerPort)
                 + $" --logical-name {Connections.WorkerLogicalName}"
                 + $" --root-name {Connections.WorkerRootName}"
                 + $" --cpu-pct {workerCpuLimitPercent}"
                 + $" --ram-pct {workerRamLimitPercent}"
                 + $" --storage-pct {LocalDefaultWorkerStorageLimitPercent}"
                 + $" --gpu-compute-pct {workerGpuLimitPercent}"
                 + $" --gpu-vram-pct {workerVramLimitPercent}"
                 + $" --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.WorkerNode", args, "WorkerNode").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            WorkerLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        ApplyObservabilityEnvironment(startInfo);
        var result = await _workerRunner.StartAsync(startInfo, waitForExit: false, label: "WorkerNode");
        WorkerLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("WorkerNode", workerPort, result.Message).ConfigureAwait(false)
            : result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private async Task StartObsAsync()
    {
        if (!TryParsePort(Connections.ObsPortText, out var port))
        {
            ObsLaunchStatus = "Invalid Obs port.";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("src", "Nbn.Runtime.Observability");
        var args = BuildLocalServiceNetworkArgs(Connections.ResolveExplicitLocalAdvertiseHost(), Connections.ObsHost, port)
                 + $" --settings-host {Connections.SettingsHost} --settings-port {Connections.SettingsPortText} --settings-name {Connections.SettingsName}"
                 + " --enable-debug --enable-viz";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Runtime.Observability", args, "Observability").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            ObsLaunchStatus = launch.Message;
            return;
        }

        var startInfo = launch.StartInfo;
        ApplyRuntimeDiagnosticsEnvironment(startInfo);
        var result = await _obsRunner.StartAsync(startInfo, waitForExit: false, label: "Observability");
        ObsLaunchStatus = result.Success
            ? await AppendFirewallAttentionAsync("Observability", port, result.Message).ConfigureAwait(false)
            : result.Message;
        await TriggerReconnectAsync().ConfigureAwait(false);
    }

    private static string BuildLocalServiceNetworkArgs(string? explicitAdvertiseHost, string? configuredHost, int port)
    {
        _ = configuredHost;
        var args = $"--bind-host {NetworkAddressDefaults.DefaultBindHost} --port {port}";
        var advertisedHost = ResolveExplicitAdvertiseHost(explicitAdvertiseHost);
        if (!string.IsNullOrWhiteSpace(advertisedHost))
        {
            args += $" --advertise-host {advertisedHost}";
        }

        return args;
    }

    private static string? ResolveExplicitAdvertiseHost(string? explicitAdvertiseHost)
    {
        if (string.IsNullOrWhiteSpace(explicitAdvertiseHost))
        {
            return null;
        }

        var trimmed = explicitAdvertiseHost.Trim();
        if (NetworkAddressDefaults.IsLoopbackHost(trimmed)
            || NetworkAddressDefaults.IsAllInterfaces(trimmed))
        {
            return null;
        }

        return trimmed;
    }
    private async Task<string> AppendFirewallAttentionAsync(string serviceLabel, int port, string launchMessage)
    {
        var firewall = await _firewallManager
            .EnsureInboundTcpAccessAsync(serviceLabel, NetworkAddressDefaults.DefaultBindHost, port)
            .ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(firewall.Message))
        {
            var logMessage = $"{serviceLabel} firewall: {firewall.Message}";
            if (firewall.RequiresAttention)
            {
                WorkbenchLog.Warn(logMessage);
            }
            else
            {
                WorkbenchLog.Info(logMessage);
            }
        }

        return firewall.RequiresAttention && !string.IsNullOrWhiteSpace(firewall.Message)
            ? $"{launchMessage} {firewall.Message}"
            : launchMessage;
    }

    private static async Task StopRunnerAsync(LocalServiceRunner runner, Action<string> setStatus)
    {
        setStatus(await runner.StopAsync().ConfigureAwait(false));
    }

    /// <summary>
    /// Starts the local Speciation runtime service using the current Orchestrator connection settings.
    /// </summary>
    public Task StartSpeciationServiceAsync()
        => StartSpeciationAsync();

    /// <summary>
    /// Stops the locally launched Speciation runtime service and updates the visible launch status.
    /// </summary>
    public Task StopSpeciationServiceAsync()
        => StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value);

    private async Task StartAllAsync()
    {
        await StartSettingsMonitorAsync().ConfigureAwait(false);
        await StartHiveMindAsync().ConfigureAwait(false);
        await StartWorkerAsync().ConfigureAwait(false);
        await StartReproAsync().ConfigureAwait(false);
        await StartSpeciationAsync().ConfigureAwait(false);
        await StartIoAsync().ConfigureAwait(false);
        await StartObsAsync().ConfigureAwait(false);
    }

    private async Task StopAllAsync()
    {
        _disconnectAll?.Invoke();
        await StopRunnerAsync(_obsRunner, value => ObsLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_ioRunner, value => IoLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_reproRunner, value => ReproLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_speciationRunner, value => SpeciationLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_workerRunner, value => WorkerLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_hiveMindRunner, value => HiveMindLaunchStatus = value).ConfigureAwait(false);
        await StopRunnerAsync(_settingsRunner, value => SettingsLaunchStatus = value).ConfigureAwait(false);
    }

    private async Task ProfileCurrentSystemAsync()
    {
        if (!Connections.HasSpawnServiceReadiness())
        {
            ProfileCurrentSystemStatus = Connections.BuildSpawnReadinessGuidance();
            StatusMessage = $"Profile current system: {ProfileCurrentSystemStatus}";
            return;
        }

        if (!TryParsePort(Connections.SettingsPortText, out var settingsPort))
        {
            ProfileCurrentSystemStatus = "Invalid Settings port.";
            StatusMessage = $"Profile current system: {ProfileCurrentSystemStatus}";
            return;
        }

        var projectPath = RepoLocator.ResolvePathFromRepo("tools", "Nbn.Tools.PerfProbe");
        var outputDirectory = ResolveProfileCurrentSystemOutputDirectory();
        Directory.CreateDirectory(outputDirectory);
        var bindPort = ResolveProfileClientPort();
        var args = $"current-system --settings-host {Connections.SettingsHost} --settings-port {settingsPort} --settings-name {Connections.SettingsName}"
                 + $" --bind-host {Connections.LocalBindHost} --bind-port {bindPort}"
                 + $" --output-dir \"{outputDirectory}\"";
        var launch = await _launchPreparer.PrepareAsync(projectPath, "Nbn.Tools.PerfProbe", args, "PerfProbe").ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            ProfileCurrentSystemStatus = launch.Message;
            StatusMessage = $"Profile current system: {launch.Message}";
            return;
        }

        ProfileCurrentSystemStatus = "Profiling current runtime...";
        StatusMessage = "Profile current system: running.";
        var result = await _profileCurrentSystemRunner.StartAsync(launch.StartInfo, waitForExit: true, label: "PerfProbe").ConfigureAwait(false);
        if (!result.Success)
        {
            ProfileCurrentSystemStatus = result.Message;
            StatusMessage = $"Profile current system: {result.Message}";
            return;
        }

        var reportPath = Path.Combine(outputDirectory, "perf-report.html");
        if (!File.Exists(reportPath))
        {
            ProfileCurrentSystemStatus = "Perf probe did not produce report artifacts.";
            StatusMessage = $"Profile current system: {ProfileCurrentSystemStatus}";
            return;
        }

        ProfileCurrentSystemStatus = $"Completed. Report: {reportPath}";
        StatusMessage = "Profile current system: completed.";
    }

    private string ResolveProfileCurrentSystemOutputDirectory()
    {
        var root = RepoLocator.ResolvePathFromRepo(".artifacts-temp", "perf-probe");
        if (string.IsNullOrWhiteSpace(root))
        {
            root = Path.Combine(Environment.CurrentDirectory, ".artifacts-temp", "perf-probe");
        }

        return Path.Combine(root, DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss"));
    }

    private int ResolveProfileClientPort()
    {
        return TryParsePort(Connections.LocalPortText, out var localPort)
            ? Math.Max(1024, localPort + 20)
            : 12110;
    }

    private async Task TriggerReconnectAsync()
    {
        if (_connectAll is null)
        {
            return;
        }

        await Task.Delay(500).ConfigureAwait(false);
        await _connectAll().ConfigureAwait(false);
    }

    private static string BuildDefaultSettingsDbPath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return "settingsmonitor.db";
        }

        var baseDir = Path.Combine(localAppData, "Nbn.Workbench");
        Directory.CreateDirectory(baseDir);
        return Path.Combine(baseDir, "settingsmonitor.db");
    }

    private string ResolveSettingsDbPath()
    {
        var configured = Connections.SettingsDbPath?.Trim();
        return string.IsNullOrWhiteSpace(configured) ? BuildDefaultSettingsDbPath() : configured;
    }

    private static bool PathsEqual(string left, string right)
    {
        try
        {
            var leftFull = Path.GetFullPath(left);
            var rightFull = Path.GetFullPath(right);
            var comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
            return string.Equals(leftFull, rightFull, comparison);
        }
        catch
        {
            return false;
        }
    }

    private void ApplyObservabilityEnvironment(ProcessStartInfo startInfo)
    {
        if (startInfo.UseShellExecute)
        {
            return;
        }

        var host = Connections.ObsHost?.Trim();
        if (!string.IsNullOrWhiteSpace(host)
            && TryParsePort(Connections.ObsPortText, out var obsPort))
        {
            startInfo.EnvironmentVariables["NBN_OBS_HOST"] = host;
            startInfo.EnvironmentVariables["NBN_OBS_PORT"] = obsPort.ToString();
            startInfo.EnvironmentVariables["NBN_OBS_ADDRESS"] = $"{host}:{obsPort}";
        }

        var debugHub = Connections.DebugHub?.Trim();
        if (!string.IsNullOrWhiteSpace(debugHub))
        {
            startInfo.EnvironmentVariables["NBN_OBS_DEBUG_HUB"] = debugHub;
        }

        var vizHub = Connections.VizHub?.Trim();
        if (!string.IsNullOrWhiteSpace(vizHub))
        {
            startInfo.EnvironmentVariables["NBN_OBS_VIZ_HUB"] = vizHub;
        }
    }

    private static void ApplyRuntimeDiagnosticsEnvironment(ProcessStartInfo startInfo)
    {
        if (!EnableRuntimeDiagnostics || startInfo.UseShellExecute)
        {
            return;
        }

        SetEnvIfMissing(startInfo, "NBN_RUNTIME_TICK_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_RUNTIME_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_HIVEMIND_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_HIVEMIND_LOG_TICK_BARRIER", "1");
        SetEnvIfMissing(startInfo, "NBN_IO_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_INPUT_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_INPUT_TRACE_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_BRAIN_LOG_DELIVERY", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONHOST_LOG_DELIVERY", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_INIT_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_REGIONSHARD_ACTIVITY_DIAGNOSTICS_PERIOD", ActivityDiagnosticsPeriod);
        SetEnvIfMissing(startInfo, "NBN_VIZ_DIAGNOSTICS_ENABLED", "1");
    }

    private static void SetEnvIfMissing(ProcessStartInfo startInfo, string key, string value)
    {
        if (!startInfo.EnvironmentVariables.ContainsKey(key))
        {
            startInfo.EnvironmentVariables[key] = value;
        }
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private static string ResolveEnvOrDefault(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }
}
