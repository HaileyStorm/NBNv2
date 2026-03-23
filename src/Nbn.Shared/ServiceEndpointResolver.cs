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

        foreach (var candidate in orderedCandidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await probeAsync(candidate, cancellationToken).ConfigureAwait(false))
            {
                return candidate.ToEndpoint();
            }
        }

        return orderedCandidates[0].ToEndpoint();
    }
}
