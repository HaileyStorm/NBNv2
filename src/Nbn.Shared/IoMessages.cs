namespace Nbn.Shared.IO;

public sealed record EmitOutputVectorSegment(uint OutputIndexStart, IReadOnlyList<float> Values, ulong TickId);
