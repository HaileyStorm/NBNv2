namespace Nbn.Runtime.Artifacts;

/// <summary>
/// Defines the HTTP header names used by the built-in artifact-service protocol.
/// </summary>
public static class HttpArtifactStoreHeaderNames
{
    /// <summary>
    /// Carries a base64-encoded JSON region index for artifact uploads.
    /// </summary>
    public const string RegionIndex = "X-Nbn-Artifact-Region-Index";
}
