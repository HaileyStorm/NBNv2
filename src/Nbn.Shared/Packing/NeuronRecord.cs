using System;

namespace Nbn.Shared.Packing;

public readonly struct NeuronRecord
{
    private const int AxonCountMask = 0x1FF;
    private const int ParamMask = 0x3F;
    private const int FunctionMask = 0x3F;
    private const int AccumulationMask = 0x03;

    private const int ParamBShift = 9;
    private const int ParamAShift = 15;
    private const int ActivationThresholdShift = 21;
    private const int PreActivationThresholdShift = 27;
    private const int ResetFunctionShift = 33;
    private const int ActivationFunctionShift = 39;
    private const int AccumulationShift = 45;
    private const int ExistsShift = 47;

    public NeuronRecord(
        ushort axonCount,
        byte paramBCode,
        byte paramACode,
        byte activationThresholdCode,
        byte preActivationThresholdCode,
        byte resetFunctionId,
        byte activationFunctionId,
        byte accumulationFunctionId,
        bool exists)
    {
        if (axonCount > AxonCountMask)
        {
            throw new ArgumentOutOfRangeException(nameof(axonCount), axonCount, "AxonCount must fit in 9 bits.");
        }

        if (paramBCode > ParamMask)
        {
            throw new ArgumentOutOfRangeException(nameof(paramBCode), paramBCode, "ParamB code must fit in 6 bits.");
        }

        if (paramACode > ParamMask)
        {
            throw new ArgumentOutOfRangeException(nameof(paramACode), paramACode, "ParamA code must fit in 6 bits.");
        }

        if (activationThresholdCode > ParamMask)
        {
            throw new ArgumentOutOfRangeException(nameof(activationThresholdCode), activationThresholdCode, "Activation threshold code must fit in 6 bits.");
        }

        if (preActivationThresholdCode > ParamMask)
        {
            throw new ArgumentOutOfRangeException(nameof(preActivationThresholdCode), preActivationThresholdCode, "Pre-activation threshold code must fit in 6 bits.");
        }

        if (resetFunctionId > FunctionMask)
        {
            throw new ArgumentOutOfRangeException(nameof(resetFunctionId), resetFunctionId, "Reset function id must fit in 6 bits.");
        }

        if (activationFunctionId > FunctionMask)
        {
            throw new ArgumentOutOfRangeException(nameof(activationFunctionId), activationFunctionId, "Activation function id must fit in 6 bits.");
        }

        if (accumulationFunctionId > AccumulationMask)
        {
            throw new ArgumentOutOfRangeException(nameof(accumulationFunctionId), accumulationFunctionId, "Accumulation function id must fit in 2 bits.");
        }

        AxonCount = axonCount;
        ParamBCode = paramBCode;
        ParamACode = paramACode;
        ActivationThresholdCode = activationThresholdCode;
        PreActivationThresholdCode = preActivationThresholdCode;
        ResetFunctionId = resetFunctionId;
        ActivationFunctionId = activationFunctionId;
        AccumulationFunctionId = accumulationFunctionId;
        Exists = exists;
    }

    public ushort AxonCount { get; }
    public byte ParamBCode { get; }
    public byte ParamACode { get; }
    public byte ActivationThresholdCode { get; }
    public byte PreActivationThresholdCode { get; }
    public byte ResetFunctionId { get; }
    public byte ActivationFunctionId { get; }
    public byte AccumulationFunctionId { get; }
    public bool Exists { get; }

    public ulong Pack()
    {
        ulong packed = 0;
        packed |= AxonCount;
        packed |= (ulong)(ParamBCode & ParamMask) << ParamBShift;
        packed |= (ulong)(ParamACode & ParamMask) << ParamAShift;
        packed |= (ulong)(ActivationThresholdCode & ParamMask) << ActivationThresholdShift;
        packed |= (ulong)(PreActivationThresholdCode & ParamMask) << PreActivationThresholdShift;
        packed |= (ulong)(ResetFunctionId & FunctionMask) << ResetFunctionShift;
        packed |= (ulong)(ActivationFunctionId & FunctionMask) << ActivationFunctionShift;
        packed |= (ulong)(AccumulationFunctionId & AccumulationMask) << AccumulationShift;
        if (Exists)
        {
            packed |= 1UL << ExistsShift;
        }

        return packed;
    }

    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < NbnConstants.NeuronRecordBytes)
        {
            throw new ArgumentException("Destination span is too small.", nameof(destination));
        }

        var packed = Pack();
        destination[0] = (byte)packed;
        destination[1] = (byte)(packed >> 8);
        destination[2] = (byte)(packed >> 16);
        destination[3] = (byte)(packed >> 24);
        destination[4] = (byte)(packed >> 32);
        destination[5] = (byte)(packed >> 40);
    }

    public static NeuronRecord FromPacked(ulong packed)
    {
        var axonCount = (ushort)(packed & AxonCountMask);
        var paramB = (byte)((packed >> ParamBShift) & ParamMask);
        var paramA = (byte)((packed >> ParamAShift) & ParamMask);
        var activationThreshold = (byte)((packed >> ActivationThresholdShift) & ParamMask);
        var preActivationThreshold = (byte)((packed >> PreActivationThresholdShift) & ParamMask);
        var resetFunction = (byte)((packed >> ResetFunctionShift) & FunctionMask);
        var activationFunction = (byte)((packed >> ActivationFunctionShift) & FunctionMask);
        var accumulationFunction = (byte)((packed >> AccumulationShift) & AccumulationMask);
        var exists = ((packed >> ExistsShift) & 0x1) == 1;

        return new NeuronRecord(
            axonCount,
            paramB,
            paramA,
            activationThreshold,
            preActivationThreshold,
            resetFunction,
            activationFunction,
            accumulationFunction,
            exists);
    }

    public static NeuronRecord Read(ReadOnlySpan<byte> source)
    {
        if (source.Length < NbnConstants.NeuronRecordBytes)
        {
            throw new ArgumentException("Source span is too small.", nameof(source));
        }

        ulong packed = source[0]
                       | ((ulong)source[1] << 8)
                       | ((ulong)source[2] << 16)
                       | ((ulong)source[3] << 24)
                       | ((ulong)source[4] << 32)
                       | ((ulong)source[5] << 40);

        return FromPacked(packed);
    }
}