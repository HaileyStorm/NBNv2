using System;
using System.Linq;
using System.Threading.Tasks;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class OrchestratorPanelViewModel
{
    private async Task ApplySettingsAsync()
    {
        if (!Connections.SettingsConnected)
        {
            StatusMessage = "SettingsMonitor not connected.";
            return;
        }

        var dirty = Settings.Where(entry => entry.IsDirty).ToList();
        if (dirty.Count == 0)
        {
            StatusMessage = "No settings changes.";
            return;
        }

        StatusMessage = $"Applying {dirty.Count} setting(s)...";
        foreach (var entry in dirty)
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            _dispatcher.Post(() =>
            {
                entry.MarkApplied(result.Value ?? entry.Value, FormatUpdated(result.UpdatedMs));
            });
        }

        StatusMessage = "Settings updated.";
    }

    private async Task PullSettingsAsync()
    {
        if (!Connections.SettingsConnected)
        {
            SettingsPullStatus = "Connect the current SettingsMonitor first.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var sourceHost = PullSettingsHost?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sourceHost))
        {
            SettingsPullStatus = "Pull source host is required.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        if (!TryParsePort(PullSettingsPortText, out var sourcePort))
        {
            SettingsPullStatus = "Invalid pull source port.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var sourceName = PullSettingsName?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(sourceName))
        {
            SettingsPullStatus = "Pull source actor name is required.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var sourceDisplay = FormatEndpointDisplay($"{sourceHost}:{sourcePort}", sourceName);
        SettingsPullStatus = $"Pulling settings from {sourceDisplay}...";
        StatusMessage = SettingsPullStatus;

        var response = await _client.ListSettingsAsync(sourceHost, sourcePort, sourceName).ConfigureAwait(false);
        if (response is null)
        {
            SettingsPullStatus = await BuildUnavailablePullSettingsStatusAsync(
                sourceHost,
                sourcePort,
                sourceName,
                sourceDisplay).ConfigureAwait(false);
            StatusMessage = SettingsPullStatus;
            return;
        }

        var imported = new System.Collections.Generic.Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var skippedEndpointCount = 0;
        foreach (var entry in response.Settings)
        {
            var key = entry.Key?.Trim() ?? string.Empty;
            if (key.Length == 0)
            {
                continue;
            }

            if (!IsPullImportableSetting(key))
            {
                skippedEndpointCount++;
                continue;
            }

            imported[key] = entry.Value ?? string.Empty;
        }

        if (imported.Count == 0)
        {
            SettingsPullStatus = skippedEndpointCount > 0
                ? $"No importable settings found at {sourceDisplay}; skipped {skippedEndpointCount} endpoint setting(s)."
                : $"No settings found at {sourceDisplay}.";
            StatusMessage = SettingsPullStatus;
            return;
        }

        var appliedCount = 0;
        foreach (var entry in imported.OrderBy(row => row.Key, StringComparer.OrdinalIgnoreCase))
        {
            var result = await _client.SetSettingAsync(entry.Key, entry.Value).ConfigureAwait(false);
            if (result is null)
            {
                continue;
            }

            appliedCount++;
            ApplyAuthoritativeSettingToView(
                result.Key ?? entry.Key,
                result.Value ?? entry.Value,
                result.UpdatedMs);
        }

        var failedCount = imported.Count - appliedCount;
        SettingsPullStatus = BuildPullSettingsStatus(sourceDisplay, appliedCount, imported.Count, failedCount, skippedEndpointCount);
        StatusMessage = SettingsPullStatus;
    }

    private async Task<string> BuildUnavailablePullSettingsStatusAsync(
        string sourceHost,
        int sourcePort,
        string sourceName,
        string sourceDisplay)
    {
        var probe = await _client.ProbeTcpEndpointAsync(sourceHost, sourcePort).ConfigureAwait(false);
        if (!probe.Reachable)
        {
            return $"Pull failed: source {sourceDisplay} is unavailable. {probe.Detail} Using the same port number on another machine is fine. The remote SettingsMonitor is most likely still bound to 127.0.0.1 or blocked by a firewall. On the remote machine, launch SettingsMonitor with the default bind host (0.0.0.0) or pass --bind-host 0.0.0.0, then allow inbound TCP {sourcePort}.";
        }

        return $"Pull failed: source {sourceDisplay} is unavailable. {probe.Detail} The TCP endpoint is reachable, but actor {sourceName} did not answer as a SettingsMonitor. Verify the actor name and that the remote instance is running a compatible NBN remoting build. If Workbench was started before the networking-defaults update, reconnect after restarting Workbench so the local Workbench client is not still bound to 127.0.0.1.";
    }

    private void SeedPullSettingsSourceFromConnections(bool force)
    {
        var currentHost = Connections.SettingsHost?.Trim() ?? string.Empty;
        var currentPort = Connections.SettingsPortText?.Trim() ?? string.Empty;
        var currentName = Connections.SettingsName?.Trim() ?? string.Empty;

        if (force || string.Equals(_pullSettingsHost, _lastSeededPullSettingsHost, StringComparison.Ordinal))
        {
            PullSettingsHost = currentHost;
        }

        if (force || string.Equals(_pullSettingsPortText, _lastSeededPullSettingsPortText, StringComparison.Ordinal))
        {
            PullSettingsPortText = currentPort;
        }

        if (force || string.Equals(_pullSettingsName, _lastSeededPullSettingsName, StringComparison.Ordinal))
        {
            PullSettingsName = currentName;
        }

        _lastSeededPullSettingsHost = currentHost;
        _lastSeededPullSettingsPortText = currentPort;
        _lastSeededPullSettingsName = currentName;
    }

    private static bool IsPullImportableSetting(string key)
        => !string.IsNullOrWhiteSpace(key)
           && !key.Trim().StartsWith(ServiceEndpointSettings.EndpointPrefix, StringComparison.OrdinalIgnoreCase);

    private void ApplyAuthoritativeSettingToView(string key, string value, ulong updatedMs)
        => ApplyAuthoritativeSettingToView(key, value, FormatUpdated(updatedMs));

    private void ApplyAuthoritativeSettingToView(string key, string value, string updatedDisplay)
    {
        _dispatcher.Post(() =>
        {
            var existing = Settings.FirstOrDefault(entry => string.Equals(entry.Key, key, StringComparison.OrdinalIgnoreCase));
            if (existing is null)
            {
                Settings.Add(new SettingEntryViewModel(key, value, updatedDisplay));
            }
            else
            {
                existing.MarkApplied(value, updatedDisplay);
            }

            var item = new SettingItem(key, value, updatedDisplay);
            TryApplyWorkerPolicySetting(item);
            TryApplyServiceEndpointSetting(item);
            Trim(Settings);
        });
    }

    private static string BuildPullSettingsStatus(
        string sourceDisplay,
        int appliedCount,
        int totalCount,
        int failedCount,
        int skippedEndpointCount)
    {
        var status = failedCount == 0
            ? $"Pulled {appliedCount} setting(s) from {sourceDisplay}."
            : $"Pulled {appliedCount} of {totalCount} setting(s) from {sourceDisplay}; {failedCount} failed.";

        if (skippedEndpointCount > 0)
        {
            status += $" Skipped {skippedEndpointCount} endpoint setting(s).";
        }

        return status;
    }
}
