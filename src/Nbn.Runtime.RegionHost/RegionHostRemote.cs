using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Proto.Remote;

namespace Nbn.Runtime.RegionHost;

public static class RegionHostRemote
{
    public static RemoteConfig BuildConfig(RegionHostOptions options)
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
            NbnDebugReflection.Descriptor,
            NbnSettingsReflection.Descriptor,
            NbnSignalsReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnVizReflection.Descriptor);

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
