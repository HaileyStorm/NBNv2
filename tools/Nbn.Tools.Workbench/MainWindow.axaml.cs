using System;
using Avalonia.Controls;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench;

public partial class MainWindow : Window
{
    private readonly ShellViewModel _viewModel = new();

    public MainWindow()
    {
        InitializeComponent();
        DataContext = _viewModel;
    }

    protected override void OnClosed(EventArgs e)
    {
        _viewModel.DisposeAsync().AsTask().GetAwaiter().GetResult();
        base.OnClosed(e);
    }
}
