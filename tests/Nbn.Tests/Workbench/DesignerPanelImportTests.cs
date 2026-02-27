using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class DesignerPanelImportTests
{
    [Fact]
    public void TryImportNbnFromBytes_RejectsInputTargetInvariantViolation()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateViewModel();
            var statusBefore = vm.Status;
            var summaryBefore = vm.LoadedSummary;
            var brainBefore = vm.Brain;

            var imported = vm.TryImportNbnFromBytes(
                CreateInvalidInputTargetNbn(),
                "invalid-input-target.nbn");

            Assert.False(imported);
            Assert.Same(brainBefore, vm.Brain);
            Assert.Equal(summaryBefore, vm.LoadedSummary);
            Assert.NotEqual(statusBefore, vm.Status);
            Assert.Contains("Import failed: Invalid .nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("invalid-input-target.nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("input region", vm.Status, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public void TryImportNbnFromBytes_RejectsOutputToOutputInvariantViolation()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateViewModel();
            var brainBefore = vm.Brain;
            var summaryBefore = vm.LoadedSummary;

            var imported = vm.TryImportNbnFromBytes(
                CreateInvalidOutputToOutputNbn(),
                "invalid-output-output.nbn");

            Assert.False(imported);
            Assert.Same(brainBefore, vm.Brain);
            Assert.Equal(summaryBefore, vm.LoadedSummary);
            Assert.Contains("Import failed: Invalid .nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("output region", vm.Status, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public void TryImportNbnFromBytes_RejectsDuplicateAxonInvariantViolation()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateViewModel();
            var brainBefore = vm.Brain;
            var summaryBefore = vm.LoadedSummary;

            var imported = vm.TryImportNbnFromBytes(
                CreateInvalidDuplicateAxonNbn(),
                "invalid-duplicate-axon.nbn");

            Assert.False(imported);
            Assert.Same(brainBefore, vm.Brain);
            Assert.Equal(summaryBefore, vm.LoadedSummary);
            Assert.Contains("Import failed: Invalid .nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("duplicate axons", vm.Status, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public void TryImportNbnFromBytes_RejectsTargetSpanInvariantViolation()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateViewModel();
            var brainBefore = vm.Brain;
            var summaryBefore = vm.LoadedSummary;

            var imported = vm.TryImportNbnFromBytes(
                CreateInvalidTargetSpanNbn(),
                "invalid-target-span.nbn");

            Assert.False(imported);
            Assert.Same(brainBefore, vm.Brain);
            Assert.Equal(summaryBefore, vm.LoadedSummary);
            Assert.Contains("Import failed: Invalid .nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("destination region span", vm.Status, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public void TryImportNbnFromBytes_WhenMultipleValidationIssues_ReportsIssueCountSummary()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateViewModel();
            var brainBefore = vm.Brain;
            var summaryBefore = vm.LoadedSummary;

            var imported = vm.TryImportNbnFromBytes(
                CreateInvalidMultiIssueNbn(),
                "invalid-multi-issue.nbn");

            Assert.False(imported);
            Assert.Same(brainBefore, vm.Brain);
            Assert.Equal(summaryBefore, vm.LoadedSummary);
            Assert.Contains("Import failed: Invalid .nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("invalid-multi-issue.nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("(+", vm.Status, StringComparison.Ordinal);
            Assert.Contains("more issue(s))", vm.Status, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void TryImportNbnFromBytes_RejectsMissingRequiredOutputRegion()
    {
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            var vm = CreateViewModel();
            var brainBefore = vm.Brain;
            var summaryBefore = vm.LoadedSummary;

            var imported = vm.TryImportNbnFromBytes(
                CreateInvalidMissingOutputRegionNbn(),
                "invalid-missing-output-region.nbn");

            Assert.False(imported);
            Assert.Same(brainBefore, vm.Brain);
            Assert.Equal(summaryBefore, vm.LoadedSummary);
            Assert.Contains("Import failed: Invalid .nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("invalid-missing-output-region.nbn", vm.Status, StringComparison.Ordinal);
            Assert.Contains("output region must be present", vm.Status, StringComparison.OrdinalIgnoreCase);
        });
    }

    private static DesignerPanelViewModel CreateViewModel()
    {
        AvaloniaTestHost.EnsureInitialized();
        var connections = new ConnectionViewModel();
        var client = new WorkbenchClient(new NullWorkbenchEventSink());
        var vm = new DesignerPanelViewModel(connections, client);
        vm.NewBrainCommand.Execute(null);
        return vm;
    }

    private static byte[] CreateInvalidInputTargetNbn()
    {
        var region0Neurons = new[]
        {
            new NeuronRecord(1, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region0Axons = new[]
        {
            new AxonRecord(8, 0, (byte)NbnConstants.InputRegionId)
        };
        var region31Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };

        return BuildNbn(
            new RegionSpec((byte)NbnConstants.InputRegionId, region0Neurons, region0Axons),
            new RegionSpec((byte)NbnConstants.OutputRegionId, region31Neurons, Array.Empty<AxonRecord>()));
    }

    private static byte[] CreateInvalidOutputToOutputNbn()
    {
        var region0Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region31Neurons = new[]
        {
            new NeuronRecord(1, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region31Axons = new[]
        {
            new AxonRecord(12, 0, (byte)NbnConstants.OutputRegionId)
        };

        return BuildNbn(
            new RegionSpec((byte)NbnConstants.InputRegionId, region0Neurons, Array.Empty<AxonRecord>()),
            new RegionSpec((byte)NbnConstants.OutputRegionId, region31Neurons, region31Axons));
    }

    private static byte[] CreateInvalidDuplicateAxonNbn()
    {
        var region0Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region1Neurons = new[]
        {
            new NeuronRecord(2, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region1Axons = new[]
        {
            new AxonRecord(3, 0, (byte)NbnConstants.OutputRegionId),
            new AxonRecord(7, 0, (byte)NbnConstants.OutputRegionId)
        };
        var region31Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };

        return BuildNbn(
            new RegionSpec((byte)NbnConstants.InputRegionId, region0Neurons, Array.Empty<AxonRecord>()),
            new RegionSpec(1, region1Neurons, region1Axons),
            new RegionSpec((byte)NbnConstants.OutputRegionId, region31Neurons, Array.Empty<AxonRecord>()));
    }

    private static byte[] CreateInvalidTargetSpanNbn()
    {
        var region0Neurons = new[]
        {
            new NeuronRecord(1, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region0Axons = new[]
        {
            new AxonRecord(9, 1, (byte)NbnConstants.OutputRegionId)
        };
        var region31Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };

        return BuildNbn(
            new RegionSpec((byte)NbnConstants.InputRegionId, region0Neurons, region0Axons),
            new RegionSpec((byte)NbnConstants.OutputRegionId, region31Neurons, Array.Empty<AxonRecord>()));
    }

    private static byte[] CreateInvalidMultiIssueNbn()
    {
        var region0Neurons = new[]
        {
            new NeuronRecord(1, 0, 0, 0, 0, 0, 0, 0, true)
        };
        var region0Axons = new[]
        {
            new AxonRecord(9, 0, (byte)NbnConstants.InputRegionId)
        };
        var region31Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };

        return BuildNbnWithHeaderFlags(
            headerFlags: 1,
            new RegionSpec((byte)NbnConstants.InputRegionId, region0Neurons, region0Axons),
            new RegionSpec((byte)NbnConstants.OutputRegionId, region31Neurons, Array.Empty<AxonRecord>()));
    }

    private static byte[] CreateInvalidMissingOutputRegionNbn()
    {
        var region0Neurons = new[]
        {
            new NeuronRecord(0, 0, 0, 0, 0, 0, 0, 0, true)
        };

        return BuildNbn(new RegionSpec((byte)NbnConstants.InputRegionId, region0Neurons, Array.Empty<AxonRecord>()));
    }

    private static byte[] BuildNbn(params RegionSpec[] regionSpecs)
        => BuildNbnWithHeaderFlags(0, regionSpecs);

    private static byte[] BuildNbnWithHeaderFlags(uint headerFlags, params RegionSpec[] regionSpecs)
    {
        var stride = (uint)NbnConstants.DefaultAxonStride;
        var sections = new List<NbnRegionSection>(regionSpecs.Length);
        foreach (var spec in regionSpecs.OrderBy(spec => spec.RegionId))
        {
            var checkpoints = NbnBinary.BuildCheckpoints(spec.Neurons, stride);
            sections.Add(new NbnRegionSection(
                spec.RegionId,
                (uint)spec.Neurons.Length,
                (ulong)spec.Axons.Length,
                stride,
                (uint)checkpoints.Length,
                checkpoints,
                spec.Neurons,
                spec.Axons));
        }

        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        ulong offset = NbnBinary.NbnHeaderBytes;
        foreach (var section in sections)
        {
            directory[section.RegionId] = new NbnRegionDirectoryEntry(
                section.NeuronSpan,
                section.TotalAxons,
                offset,
                0);
            offset += (ulong)section.ByteLength;
        }

        var header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            0,
            stride,
            headerFlags,
            QuantizationSchemas.DefaultNbn,
            directory);

        return NbnBinary.WriteNbn(header, sections);
    }

    private readonly record struct RegionSpec(byte RegionId, NeuronRecord[] Neurons, AxonRecord[] Axons);

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnSettingChanged(SettingItem item) { }
    }
}
