using Proto;

namespace Nbn.Runtime.Observability;

public sealed record VizSubscribe(PID Subscriber);

public sealed record VizUnsubscribe(PID Subscriber);
