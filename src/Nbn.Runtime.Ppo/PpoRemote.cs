using Nbn.Proto;
using Nbn.Proto.Io;
using Nbn.Proto.Ppo;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Speciation;
using Nbn.Shared;
using Proto.Remote;

namespace Nbn.Runtime.Ppo;

public static class PpoRemote
{
    public static RemoteConfig BuildConfig(PpoOptions options)
    {
        var bindHost = options.BindHost;
        RemoteConfig config;

        if (NetworkAddressDefaults.IsAllInterfaces(bindHost))
        {
            var advertisedHost = NetworkAddressDefaults.ResolveAdvertisedHost(bindHost, options.AdvertisedHost);
            config = RemoteConfig.BindToAllInterfaces(advertisedHost, options.Port);
        }
        else if (NetworkAddressDefaults.IsLoopbackHost(bindHost))
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

        return config.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnPpoReflection.Descriptor,
            NbnReproReflection.Descriptor,
            NbnSettingsReflection.Descriptor,
            NbnSpeciationReflection.Descriptor);
    }
}
