using System.ComponentModel;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

public class BooleanPropertySyncBridgeTests
{
    [Fact]
    public void Constructor_WithInitializeRightFromLeft_SynchronizesInitialState()
    {
        var left = new TestBoolState(true);
        var right = new TestBoolState(false);

        using var _ = CreateBridge(left, right, initializeRightFromLeft: true);

        Assert.True(left.Value);
        Assert.True(right.Value);
    }

    [Fact]
    public void LeftPropertyChange_PropagatesToRight()
    {
        var left = new TestBoolState(false);
        var right = new TestBoolState(false);

        using var _ = CreateBridge(left, right, initializeRightFromLeft: false);

        left.Value = true;

        Assert.True(left.Value);
        Assert.True(right.Value);
    }

    [Fact]
    public void RightPropertyChange_PropagatesToLeft()
    {
        var left = new TestBoolState(false);
        var right = new TestBoolState(false);

        using var _ = CreateBridge(left, right, initializeRightFromLeft: false);

        right.Value = true;

        Assert.True(left.Value);
        Assert.True(right.Value);
    }

    [Fact]
    public void PropertyPropagation_DoesNotCreateReentrantLoop()
    {
        var left = new TestBoolState(false);
        var right = new TestBoolState(false);

        using var _ = CreateBridge(left, right, initializeRightFromLeft: false);

        left.Value = true;

        Assert.Equal(1, left.SetCount);
        Assert.Equal(1, right.SetCount);
    }

    [Fact]
    public void Dispose_StopsFurtherSynchronization()
    {
        var left = new TestBoolState(false);
        var right = new TestBoolState(false);

        var bridge = CreateBridge(left, right, initializeRightFromLeft: false);
        bridge.Dispose();

        left.Value = true;

        Assert.True(left.Value);
        Assert.False(right.Value);
    }

    private static BooleanPropertySyncBridge CreateBridge(TestBoolState left, TestBoolState right, bool initializeRightFromLeft)
    {
        return new BooleanPropertySyncBridge(
            left,
            nameof(TestBoolState.Value),
            () => left.Value,
            value => left.Value = value,
            right,
            nameof(TestBoolState.Value),
            () => right.Value,
            value => right.Value = value,
            initializeRightFromLeft);
    }

    private sealed class TestBoolState : INotifyPropertyChanged
    {
        private bool _value;

        public TestBoolState(bool value)
        {
            _value = value;
        }

        public int SetCount { get; private set; }

        public bool Value
        {
            get => _value;
            set
            {
                if (_value == value)
                {
                    return;
                }

                _value = value;
                SetCount++;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;
    }
}
