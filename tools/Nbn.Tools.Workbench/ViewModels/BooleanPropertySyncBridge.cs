using System;
using System.ComponentModel;

namespace Nbn.Tools.Workbench.ViewModels;

/// <summary>
/// Synchronizes two boolean properties across two property-changed publishers.
/// </summary>
public sealed class BooleanPropertySyncBridge : IDisposable
{
    private readonly INotifyPropertyChanged _leftSource;
    private readonly string _leftPropertyName;
    private readonly Func<bool> _readLeft;
    private readonly Action<bool> _writeLeft;
    private readonly INotifyPropertyChanged _rightSource;
    private readonly string _rightPropertyName;
    private readonly Func<bool> _readRight;
    private readonly Action<bool> _writeRight;
    private bool _isSynchronizing;
    private bool _disposed;

    public BooleanPropertySyncBridge(
        INotifyPropertyChanged leftSource,
        string leftPropertyName,
        Func<bool> readLeft,
        Action<bool> writeLeft,
        INotifyPropertyChanged rightSource,
        string rightPropertyName,
        Func<bool> readRight,
        Action<bool> writeRight,
        bool initializeRightFromLeft = true)
    {
        _leftSource = leftSource ?? throw new ArgumentNullException(nameof(leftSource));
        _leftPropertyName = leftPropertyName ?? throw new ArgumentNullException(nameof(leftPropertyName));
        _readLeft = readLeft ?? throw new ArgumentNullException(nameof(readLeft));
        _writeLeft = writeLeft ?? throw new ArgumentNullException(nameof(writeLeft));
        _rightSource = rightSource ?? throw new ArgumentNullException(nameof(rightSource));
        _rightPropertyName = rightPropertyName ?? throw new ArgumentNullException(nameof(rightPropertyName));
        _readRight = readRight ?? throw new ArgumentNullException(nameof(readRight));
        _writeRight = writeRight ?? throw new ArgumentNullException(nameof(writeRight));

        _leftSource.PropertyChanged += OnLeftPropertyChanged;
        _rightSource.PropertyChanged += OnRightPropertyChanged;

        if (initializeRightFromLeft)
        {
            SyncLeftToRight();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _leftSource.PropertyChanged -= OnLeftPropertyChanged;
        _rightSource.PropertyChanged -= OnRightPropertyChanged;
    }

    private void OnLeftPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (!string.Equals(e.PropertyName, _leftPropertyName, StringComparison.Ordinal))
        {
            return;
        }

        SyncLeftToRight();
    }

    private void OnRightPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (!string.Equals(e.PropertyName, _rightPropertyName, StringComparison.Ordinal))
        {
            return;
        }

        SyncRightToLeft();
    }

    private void SyncLeftToRight()
    {
        if (_isSynchronizing)
        {
            return;
        }

        _isSynchronizing = true;
        try
        {
            var left = _readLeft();
            if (_readRight() != left)
            {
                _writeRight(left);
            }
        }
        finally
        {
            _isSynchronizing = false;
        }
    }

    private void SyncRightToLeft()
    {
        if (_isSynchronizing)
        {
            return;
        }

        _isSynchronizing = true;
        try
        {
            var right = _readRight();
            if (_readLeft() != right)
            {
                _writeLeft(right);
            }
        }
        finally
        {
            _isSynchronizing = false;
        }
    }
}
