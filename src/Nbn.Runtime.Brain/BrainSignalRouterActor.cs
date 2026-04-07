using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Proto.Signal;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;

namespace Nbn.Runtime.Brain;

/// <summary>
/// Routes shard output batches and IO-driven input contributions across a brain's tick phases.
/// </summary>
public sealed partial class BrainSignalRouterActor : IActor
{
    private static readonly bool LogDelivery = IsEnvTrue("NBN_BRAIN_LOG_DELIVERY");
    private static readonly bool LogInputDiagnostics = IsEnvTrue("NBN_INPUT_DIAGNOSTICS_ENABLED");
    private static readonly bool LogInputTraceDiagnostics = IsEnvTrue("NBN_INPUT_TRACE_DIAGNOSTICS_ENABLED");
    private static readonly TimeSpan RuntimeStateResetTimeout = TimeSpan.FromSeconds(5);

    private readonly Guid _brainId;
    private readonly Nbn.Proto.Uuid _brainIdProto;
    private readonly RoutingTable _routingTable = new();
    private readonly Dictionary<ulong, TickOutbox> _pendingOutboxes = new();
    private readonly Dictionary<ulong, PendingDeliver> _pendingDeliveries = new();
    private readonly Dictionary<ulong, PendingInputDrain> _pendingInputDrains = new();
    private ulong _minimumAcceptedTickId = 1;

    private PID? _ioGatewayPid;
    private bool _inputCoordinatorModeKnown;
    private InputCoordinatorMode _inputCoordinatorMode = InputCoordinatorMode.DirtyOnChange;
    private bool _inputDrainPending;
    private RoutingTableSnapshot _routingSnapshot = RoutingTableSnapshot.Empty;

    /// <summary>
    /// Creates a router for the supplied brain.
    /// </summary>
    /// <param name="brainId">Owning brain identifier.</param>
    public BrainSignalRouterActor(Guid brainId)
    {
        _brainId = brainId;
        _brainIdProto = brainId.ToProtoUuid();
    }

    /// <summary>
    /// Dispatches router messages for tick compute, tick deliver, IO coordination, and ack tracking.
    /// </summary>
    public async Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case SetRoutingTable setRouting:
                ApplyRoutingTable(setRouting.Table);
                break;
            case GetRoutingTable:
                context.Respond(_routingSnapshot);
                break;
            case TickCompute tickCompute:
                HandleTickCompute(context, tickCompute);
                break;
            case OutboxBatch outboxBatch:
                HandleOutboxBatch(outboxBatch);
                break;
            case TickDeliver tickDeliver:
                HandleTickDeliver(context, tickDeliver);
                break;
            case InputWrite inputWrite:
                HandleInputWrite(context, inputWrite);
                break;
            case InputVector inputVector:
                HandleInputVector(context, inputVector);
                break;
            case RuntimeNeuronPulse runtimePulse:
                HandleRuntimeNeuronPulse(context, runtimePulse);
                break;
            case RuntimeNeuronStateWrite runtimeStateWrite:
                HandleRuntimeNeuronStateWrite(context, runtimeStateWrite);
                break;
            case ResetBrainRuntimeState resetBrainRuntimeState:
                await HandleResetBrainRuntimeStateAsync(context, resetBrainRuntimeState);
                break;
            case ApplyBrainRuntimeResetAtBarrier resetAtBarrier:
                await HandleBarrierRuntimeStateResetAsync(context, resetAtBarrier);
                break;
            case InputDrain inputDrain:
                HandleInputDrain(context, inputDrain);
                break;
            case RegisterIoGateway registerIoGateway:
                HandleRegisterIoGateway(context, registerIoGateway);
                break;
            case SignalBatchAck ack:
                HandleSignalBatchAck(context, ack);
                break;
            case TickComputeDone tickComputeDone:
                ForwardToParent(context, tickComputeDone);
                break;
        }
    }

    private void ApplyRoutingTable(RoutingTableSnapshot? snapshot)
    {
        _routingSnapshot = snapshot ?? RoutingTableSnapshot.Empty;
        _routingTable.Replace(_routingSnapshot.Routes);
    }

    private void HandleTickCompute(IContext context, TickCompute tickCompute)
    {
        if (tickCompute.TickId < _minimumAcceptedTickId)
        {
            return;
        }

        if (_routingTable.Count == 0)
        {
            return;
        }

        foreach (var route in _routingTable.Entries)
        {
            context.Send(route.Pid, tickCompute);
        }
    }

    private void HandleOutboxBatch(OutboxBatch outboxBatch)
    {
        if (!IsForBrain(outboxBatch.BrainId))
        {
            return;
        }

        var tickId = outboxBatch.TickId;
        if (tickId < _minimumAcceptedTickId)
        {
            return;
        }

        if (!_pendingOutboxes.TryGetValue(tickId, out var outbox))
        {
            outbox = new TickOutbox();
            _pendingOutboxes[tickId] = outbox;
        }

        outbox.Add(outboxBatch);
    }

    private void DispatchToRegionShards(IContext context, uint regionId, object message)
    {
        if (_routingTable.Count == 0)
        {
            return;
        }

        foreach (var entry in _routingTable.Entries)
        {
            if (entry.ShardId.RegionId != (int)regionId)
            {
                continue;
            }

            context.Send(entry.Pid, message);
        }
    }

    private static void ForwardToParent(IContext context, object message)
    {
        if (context.Parent is null)
        {
            return;
        }

        context.Request(context.Parent, message);
    }

    private bool IsForBrain(Nbn.Proto.Uuid? brainId)
    {
        if (brainId is null)
        {
            return true;
        }

        return brainId.TryToGuid(out var guid) && guid == _brainId;
    }

    private static void Log(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [BrainSignalRouter] {message}");

    private void LogInput(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [BrainSignalRouterInput] brain={_brainId:D} {message}");

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Equals("1", StringComparison.OrdinalIgnoreCase)
               || value.Equals("true", StringComparison.OrdinalIgnoreCase)
               || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
               || value.Equals("y", StringComparison.OrdinalIgnoreCase);
    }
}
