using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Proto.Remote;

namespace Nbn.Runtime.HiveMind;

public static class HiveMindRemote
{
    public static RemoteConfig BuildConfig(HiveMindOptions options)
    {
        var bindHost = options.BindHost;
        RemoteConfig config;

        if (IsAllInterfaces(bindHost))
        {
            var advertisedHost = options.AdvertisedHost ?? bindHost;
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
            NbnSettingsReflection.Descriptor,
            NbnSignalsReflection.Descriptor);

        return config;
    }

    private static bool IsLocalhost(string host)
        => host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
           || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
           || host.Equals("::1", StringComparison.OrdinalIgnoreCase);

    private static bool IsAllInterfaces(string host)
        => host.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase)
           || host.Equals("::", StringComparison.OrdinalIgnoreCase)
           || host.Equals("*", StringComparison.OrdinalIgnoreCase);
}
