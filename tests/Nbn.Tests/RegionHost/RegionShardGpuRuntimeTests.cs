using Nbn.Runtime.RegionHost;

namespace Nbn.Tests.RegionHost;

public sealed class RegionShardGpuRuntimeTests
{
    [Theory]
    [InlineData("gpu_backend_unavailable")]
    [InlineData("expected_gpu_backend_but_observed_cpu:fallback=gpu_backend_unavailable")]
    [InlineData("gpu_compute_failed:CUDA_ERROR_OUT_OF_MEMORY")]
    [InlineData("gpu_init_failed:out of memory")]
    public void IsTransientRuntimeUnavailableReason_RecognizesRuntimeCapacityFailures(string reason)
    {
        Assert.True(RegionShardGpuRuntime.IsTransientRuntimeUnavailableReason(reason));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("gpu_not_detected")]
    [InlineData("unsupported_gpu_accelerator_type")]
    [InlineData("opencl_float64_not_supported")]
    [InlineData("memory allocation failed")]
    public void IsTransientRuntimeUnavailableReason_IgnoresStaticCapabilityFailures(string? reason)
    {
        Assert.False(RegionShardGpuRuntime.IsTransientRuntimeUnavailableReason(reason));
    }
}
