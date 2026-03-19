using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Speciation;
using Nbn.Proto.Settings;
using Nbn.Shared;
using Proto.Remote;

namespace Nbn.Runtime.Speciation;

public static class SpeciationRemote
{
    public static RemoteConfig BuildConfig(SpeciationOptions options)
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
            config = config.WithAdvertisedPort(options.AdvertisedPort.Value);
        }

        config = config.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnReproReflection.Descriptor,
            NbnSpeciationReflection.Descriptor,
            NbnSettingsReflection.Descriptor);

        return config;
    }

    private static bool IsLocalhost(string host)
        => NetworkAddressDefaults.IsLoopbackHost(host);

    private static bool IsAllInterfaces(string host)
        => NetworkAddressDefaults.IsAllInterfaces(host);
}
