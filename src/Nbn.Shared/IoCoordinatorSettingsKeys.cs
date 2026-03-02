namespace Nbn.Shared;

public static class IoCoordinatorSettingsKeys
{
    public const string InputCoordinatorModeKey = "io.input_coordinator.mode";
    public const string OutputVectorSourceKey = "io.output.vector_source";

    public static readonly IReadOnlyList<string> AllKeys = new[]
    {
        InputCoordinatorModeKey,
        OutputVectorSourceKey
    };
}
