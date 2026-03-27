using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Proto.Remote;

namespace Nbn.Runtime.HiveMind;

/// <summary>
/// Builds the Proto.Remote configuration used by the HiveMind runtime host.
/// </summary>
public static class HiveMindRemote
{
    /// <summary>
    /// Creates a remote configuration that matches the resolved HiveMind bind and advertise settings.
    /// </summary>
    /// <param name="options">The resolved HiveMind runtime options.</param>
    /// <returns>The remote configuration for the HiveMind process.</returns>
    public static RemoteConfig BuildConfig(HiveMindOptions options)
    {
        var bindHost = options.BindHost;
        RemoteConfig config;

        if (IsAllInterfaces(bindHost))
        {
            var advertisedHost = NetworkAddressDefaults.ResolveAdvertisedHost(bindHost, options.AdvertisedHost);
            config = RemoteConfig.BindToAllInterfaces(advertisedHost, options.Port);
        }
        else if (IsLocalhost(bindHost))
        {
            config = RemoteConfig.BindToLocalhost(options.Port);
        }
        else
        {
            config = RemoteConfig.BindTo(bindHost, options.Port);
        }

        if (!string.IsNullOrWhiteSpace(options.AdvertisedHost))
        {
            config = config.WithAdvertisedHost(options.AdvertisedHost);
        }

        if (options.AdvertisedPort.HasValue)
        {
            config = config.WithAdvertisedPort(options.AdvertisedPort);
        }

        config = config.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnControlReflection.Descriptor,
            NbnDebugReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnSettingsReflection.Descriptor,
            NbnSignalsReflection.Descriptor,
            NbnVizReflection.Descriptor);

        return config;
    }

    private static bool IsLocalhost(string host)
        => NetworkAddressDefaults.IsLoopbackHost(host);

    private static bool IsAllInterfaces(string host)
        => NetworkAddressDefaults.IsAllInterfaces(host);
}
