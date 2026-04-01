using Nbn.Proto.Io;
using Proto;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private Task HandlePlacementWorkerInventoryAsync(IContext context)
    {
        if (_hiveMindPid is null)
        {
            context.Respond(BuildPlacementWorkerInventoryFailure(
                reasonCode: "capacity_unavailable",
                failureMessage: "Worker capacity query failed: HiveMind endpoint is not configured."));
            return Task.CompletedTask;
        }

        var inventoryTask = context.RequestAsync<ProtoControl.PlacementWorkerInventory>(
            _hiveMindPid,
            new ProtoControl.PlacementWorkerInventoryRequest(),
            DefaultRequestTimeout);
        context.ReenterAfter(inventoryTask, completed =>
        {
            if (completed.IsFaulted)
            {
                var ex = completed.Exception?.GetBaseException();
                Console.WriteLine($"GetPlacementWorkerInventory failed: {ex?.Message}");
                context.Respond(BuildPlacementWorkerInventoryFailure(
                    reasonCode: "capacity_request_failed",
                    failureMessage: $"Worker capacity query failed: request forwarding to HiveMind failed ({ex?.Message ?? "unknown"})."));
                return;
            }

            var inventory = completed.Result;
            if (inventory is null)
            {
                context.Respond(BuildPlacementWorkerInventoryFailure(
                    reasonCode: "capacity_empty_response",
                    failureMessage: "Worker capacity query failed: HiveMind returned an empty placement inventory response."));
                return;
            }

            context.Respond(new PlacementWorkerInventoryResult
            {
                Success = true,
                Inventory = inventory
            });
        });
        return Task.CompletedTask;
    }

    private static PlacementWorkerInventoryResult BuildPlacementWorkerInventoryFailure(
        string reasonCode,
        string failureMessage)
        => new()
        {
            Success = false,
            FailureReasonCode = reasonCode,
            FailureMessage = failureMessage,
            Inventory = new ProtoControl.PlacementWorkerInventory()
        };
}
