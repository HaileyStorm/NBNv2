using System;
using System.Collections.Generic;
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
        await Task.WhenAll(
                StopRunnerAsync(_workerRunner, _ => { }),
                StopRunnerAsync(_settingsRunner, _ => { }),
                StopRunnerAsync(_hiveMindRunner, _ => { }),
                StopRunnerAsync(_ioRunner, _ => { }),
                StopRunnerAsync(_reproRunner, _ => { }),
                StopRunnerAsync(_speciationRunner, _ => { }),
                StopRunnerAsync(_obsRunner, _ => { }))
            .ConfigureAwait(false);
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
        SettingsLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.SettingsMonitor",
            args,
            "SettingsMonitor",
            port,
            _settingsRunner).ConfigureAwait(false);
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
        HiveMindLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.HiveMind",
            args,
            "HiveMind",
            port,
            _hiveMindRunner,
            includeRuntimeDiagnostics: true).ConfigureAwait(false);
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
        IoLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.IO",
            args,
            "IoGateway",
            port,
            _ioRunner,
            includeRuntimeDiagnostics: true).ConfigureAwait(false);
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
        ReproLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.Reproduction",
            args,
            "Reproduction",
            reproPort,
            _reproRunner,
            includeRuntimeDiagnostics: true).ConfigureAwait(false);
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
        SpeciationLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.Speciation",
            args,
            "Speciation",
            speciationPort,
            _speciationRunner,
            includeRuntimeDiagnostics: true).ConfigureAwait(false);
        StatusMessage = $"Speciation launch: {SpeciationLaunchStatus}";
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
        WorkerLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.WorkerNode",
            args,
            "WorkerNode",
            workerPort,
            _workerRunner,
            includeRuntimeDiagnostics: true,
            includeObservabilityEnvironment: true).ConfigureAwait(false);
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
        ObsLaunchStatus = await StartLocalServiceAsync(
            projectPath,
            "Nbn.Runtime.Observability",
            args,
            "Observability",
            port,
            _obsRunner,
            includeRuntimeDiagnostics: true).ConfigureAwait(false);
    }

    private async Task<string> StartLocalServiceAsync(
        string? projectPath,
        string executableName,
        string runtimeArgs,
        string launchLabel,
        int port,
        LocalServiceRunner runner,
        bool includeRuntimeDiagnostics = false,
        bool includeObservabilityEnvironment = false)
    {
        var launch = await _launchPreparer.PrepareAsync(projectPath, executableName, runtimeArgs, launchLabel).ConfigureAwait(false);
        if (!launch.Success || launch.StartInfo is null)
        {
            return launch.Message;
        }

        var startInfo = launch.StartInfo;
        if (includeRuntimeDiagnostics)
        {
            ApplyRuntimeDiagnosticsEnvironment(startInfo);
        }

        if (includeObservabilityEnvironment)
        {
            ApplyObservabilityEnvironment(startInfo);
        }

        var result = await runner.StartAsync(startInfo, waitForExit: false, label: launchLabel).ConfigureAwait(false);
        if (!result.Success)
        {
            await TriggerReconnectAsync().ConfigureAwait(false);
            return result.Message;
        }

        var launchStatus = await AppendFirewallAttentionAsync(launchLabel, port, result.Message).ConfigureAwait(false);
        await TriggerReconnectAsync().ConfigureAwait(false);
        return launchStatus;
    }

    private static string BuildLocalServiceNetworkArgs(string? explicitAdvertiseHost, string? configuredHost, int port)
    {
        var args = $"--bind-host {NetworkAddressDefaults.DefaultBindHost} --port {port}";
        var advertisedHost = ResolveExplicitAdvertiseHost(explicitAdvertiseHost)
                             ?? ResolveLocalServiceAdvertiseHost(configuredHost);
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

    private static string? ResolveLocalServiceAdvertiseHost(string? configuredHost)
    {
        if (string.IsNullOrWhiteSpace(configuredHost))
        {
            return null;
        }

        var trimmed = configuredHost.Trim();
        if (NetworkAddressDefaults.IsLoopbackHost(trimmed)
            || NetworkAddressDefaults.IsAllInterfaces(trimmed))
        {
            return null;
        }

        return NetworkAddressDefaults.IsLocalHost(trimmed) ? trimmed : null;
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
        var rollbackActions = new List<Func<Task>>();

        if (!await StartAllStepAsync(
                StartSettingsMonitorAsync,
                _settingsRunner,
                () => SettingsLaunchStatus,
                value => SettingsLaunchStatus = value,
                "SettingsMonitor",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        if (!await StartAllStepAsync(
                StartHiveMindAsync,
                _hiveMindRunner,
                () => HiveMindLaunchStatus,
                value => HiveMindLaunchStatus = value,
                "HiveMind",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        if (!await StartAllStepAsync(
                StartWorkerAsync,
                _workerRunner,
                () => WorkerLaunchStatus,
                value => WorkerLaunchStatus = value,
                "WorkerNode",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        if (!await StartAllStepAsync(
                StartReproAsync,
                _reproRunner,
                () => ReproLaunchStatus,
                value => ReproLaunchStatus = value,
                "Reproduction",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        if (!await StartAllStepAsync(
                StartSpeciationAsync,
                _speciationRunner,
                () => SpeciationLaunchStatus,
                value => SpeciationLaunchStatus = value,
                "Speciation",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        if (!await StartAllStepAsync(
                StartIoAsync,
                _ioRunner,
                () => IoLaunchStatus,
                value => IoLaunchStatus = value,
                "IoGateway",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        if (!await StartAllStepAsync(
                StartObsAsync,
                _obsRunner,
                () => ObsLaunchStatus,
                value => ObsLaunchStatus = value,
                "Observability",
                rollbackActions).ConfigureAwait(false))
        {
            return;
        }

        StatusMessage = "Start All completed.";
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

        SetEnvIfMissing(startInfo, "NBN_RUNTIME_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_HIVEMIND_METADATA_DIAGNOSTICS_ENABLED", "1");
        SetEnvIfMissing(startInfo, "NBN_IO_METADATA_DIAGNOSTICS_ENABLED", "1");
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

    private async Task<bool> StartAllStepAsync(
        Func<Task> startAsync,
        LocalServiceRunner runner,
        Func<string> statusAccessor,
        Action<string> statusSetter,
        string serviceLabel,
        List<Func<Task>> rollbackActions)
    {
        var wasRunning = runner.IsRunning;
        await startAsync().ConfigureAwait(false);

        var status = statusAccessor();
        if (IsSuccessfulLaunchStatus(status))
        {
            if (!wasRunning)
            {
                rollbackActions.Add(async () =>
                {
                    var stopStatus = await runner.StopAsync().ConfigureAwait(false);
                    statusSetter($"{stopStatus} Rolled back after Start All failure.");
                });
            }

            return true;
        }

        if (rollbackActions.Count == 0)
        {
            return true;
        }

        await RollBackStartAllAsync(rollbackActions).ConfigureAwait(false);
        StatusMessage = $"Start All failed while starting {serviceLabel}: {status}";
        return false;
    }

    private async Task RollBackStartAllAsync(IReadOnlyList<Func<Task>> rollbackActions)
    {
        _disconnectAll?.Invoke();
        for (var i = rollbackActions.Count - 1; i >= 0; i--)
        {
            await rollbackActions[i]().ConfigureAwait(false);
        }
    }

    private static bool IsSuccessfulLaunchStatus(string? status)
    {
        return !string.IsNullOrWhiteSpace(status)
               && (status.StartsWith("Running", StringComparison.Ordinal)
                   || string.Equals(status, "Already running.", StringComparison.Ordinal));
    }
}
