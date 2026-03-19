using Nbn.Proto;
using Nbn.Proto.Debug;
using Nbn.Proto.Settings;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Proto.Remote;

namespace Nbn.Runtime.Observability;

public static class ObservabilityRemote
{
    public static RemoteConfig BuildConfig(ObservabilityOptions options)
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
            NbnDebugReflection.Descriptor,
            NbnSettingsReflection.Descriptor,
            NbnVizReflection.Descriptor);

        return config;
    }

    private static bool IsLocalhost(string host)
        => NetworkAddressDefaults.IsLoopbackHost(host);

    private static bool IsAllInterfaces(string host)
        => NetworkAddressDefaults.IsAllInterfaces(host);
}
