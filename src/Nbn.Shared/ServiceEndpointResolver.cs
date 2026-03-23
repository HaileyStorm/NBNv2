namespace Nbn.Shared;

public static class ServiceEndpointResolver
{
    public static async Task<ServiceEndpoint> ResolveAsync(
        ServiceEndpointSet endpointSet,
        Func<ServiceEndpointCandidate, CancellationToken, Task<bool>>? probeAsync = null,
        CancellationToken cancellationToken = default)
    {
        if (endpointSet.Candidates.Count == 0)
        {
            throw new InvalidOperationException("Endpoint set does not contain any candidates.");
        }

        var orderedCandidates = endpointSet.Candidates
            .OrderByDescending(candidate => candidate.IsDefault)
            .ThenByDescending(candidate => candidate.Priority)
            .ThenBy(candidate => candidate.HostPort, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (probeAsync is null || orderedCandidates.Length == 1)
        {
            return orderedCandidates[0].ToEndpoint();
        }

        var probeCandidates = ReorderForLocalPreference(orderedCandidates);
        foreach (var candidate in probeCandidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await probeAsync(candidate, cancellationToken).ConfigureAwait(false))
            {
                return candidate.ToEndpoint();
            }
        }

        return orderedCandidates[0].ToEndpoint();
    }

    private static IReadOnlyList<ServiceEndpointCandidate> ReorderForLocalPreference(
        IReadOnlyList<ServiceEndpointCandidate> orderedCandidates)
    {
        var localMatches = orderedCandidates
            .Select((candidate, index) => new
            {
                Candidate = candidate,
                Index = index,
                IsLocal = TryGetHost(candidate.HostPort, out var host) && NetworkAddressDefaults.IsLocalHost(host)
            })
            .Where(static entry => entry.IsLocal)
            .ToArray();

        if (localMatches.Length == 0)
        {
            return orderedCandidates;
        }

        return orderedCandidates
            .Select((candidate, index) => new { Candidate = candidate, Index = index })
            .OrderByDescending(entry => localMatches.Any(local => local.Index == entry.Index))
            .ThenBy(entry => entry.Candidate.Kind switch
            {
                ServiceEndpointCandidateKind.Loopback => 0,
                ServiceEndpointCandidateKind.Lan => 1,
                ServiceEndpointCandidateKind.Tailnet => 2,
                ServiceEndpointCandidateKind.Public => 3,
                _ => 4
            })
            .ThenBy(entry => entry.Index)
            .Select(static entry => entry.Candidate)
            .ToArray();
    }

    private static bool TryGetHost(string? hostPort, out string host)
    {
        host = string.Empty;
        if (string.IsNullOrWhiteSpace(hostPort))
        {
            return false;
        }

        var trimmed = hostPort.Trim();
        var colonIndex = trimmed.LastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= trimmed.Length - 1)
        {
            return false;
        }

        host = trimmed[..colonIndex];
        if (host.StartsWith("[", StringComparison.Ordinal) && host.EndsWith("]", StringComparison.Ordinal))
        {
            host = host[1..^1];
        }

        return !string.IsNullOrWhiteSpace(host);
    }
}
