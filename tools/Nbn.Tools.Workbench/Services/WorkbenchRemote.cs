using System;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Shared;
using Proto.Remote;

namespace Nbn.Tools.Workbench.Services;

public static class WorkbenchRemote
{
    public static RemoteConfig BuildConfig(string bindHost, int port, string? advertisedHost = null, int? advertisedPort = null)
    {
        RemoteConfig config;

        if (IsAllInterfaces(bindHost))
        {
            var advertised = NetworkAddressDefaults.ResolveAdvertisedHost(bindHost, advertisedHost);
            config = RemoteConfig.BindToAllInterfaces(advertised, port);
        }
        else if (IsLocalhost(bindHost))
        {
            config = RemoteConfig.BindToLocalhost(port);
        }
        else
        {
            config = RemoteConfig.BindTo(bindHost, port);
        }

        if (!string.IsNullOrWhiteSpace(advertisedHost))
        {
            config = config.WithAdvertisedHost(advertisedHost);
        }

        if (advertisedPort.HasValue)
        {
            config = config.WithAdvertisedPort(advertisedPort.Value);
        }

        return config.WithProtoMessages(
            NbnCommonReflection.Descriptor,
            NbnControlReflection.Descriptor,
            NbnIoReflection.Descriptor,
            NbnReproReflection.Descriptor,
            NbnSignalsReflection.Descriptor,
            NbnDebugReflection.Descriptor,
            NbnVizReflection.Descriptor,
            NbnSettingsReflection.Descriptor);
    }

    private static bool IsLocalhost(string host)
        => NetworkAddressDefaults.IsLoopbackHost(host);

    private static bool IsAllInterfaces(string host)
        => NetworkAddressDefaults.IsAllInterfaces(host);
}
