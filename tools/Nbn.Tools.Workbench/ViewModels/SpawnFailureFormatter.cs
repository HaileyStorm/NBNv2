using Nbn.Proto.Control;

namespace Nbn.Tools.Workbench.ViewModels;

internal static class SpawnFailureFormatter
{
    public static string Format(string prefix, SpawnBrainAck? ack, string fallbackMessage)
    {
        if (ack is null)
        {
            return fallbackMessage;
        }

        var reasonCode = (ack.FailureReasonCode ?? string.Empty).Trim();
        var failureMessage = (ack.FailureMessage ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(failureMessage))
        {
            if (!string.IsNullOrWhiteSpace(reasonCode))
            {
                return $"{prefix}: {failureMessage} (code: {reasonCode}).";
            }

            return $"{prefix}: {failureMessage}";
        }

        if (!string.IsNullOrWhiteSpace(reasonCode))
        {
            return $"{prefix}: {reasonCode.Replace('_', ' ')} (code: {reasonCode}).";
        }

        return fallbackMessage;
    }
}
