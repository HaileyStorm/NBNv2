namespace Nbn.Shared;

public static class NbnConstants
{
    public const int RegionCount = 32;
    public const int RegionMinId = 0;
    public const int RegionMaxId = 31;
    public const int InputRegionId = 0;
    public const int OutputRegionId = 31;

    public const int AddressNeuronBits = 27;
    public const uint AddressNeuronMask = (1u << AddressNeuronBits) - 1u;
    public const int MaxAddressNeuronId = (1 << AddressNeuronBits) - 1;

    public const int AxonTargetNeuronBits = 22;
    public const int MaxAxonTargetNeuronId = (1 << AxonTargetNeuronBits) - 1;

    public const int MaxAxonsPerNeuron = 511;

    public const int NeuronRecordBytes = 6;
    public const int AxonRecordBytes = 4;

    public const int DefaultAxonStride = 1024;
}