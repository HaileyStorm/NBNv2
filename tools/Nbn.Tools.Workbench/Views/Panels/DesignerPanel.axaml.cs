using Avalonia.Controls;
using Avalonia.Input;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tools.Workbench.Views.Panels;

public partial class DesignerPanel : UserControl
{
    public DesignerPanel()
    {
        InitializeComponent();
    }

    private void NeuronPointerEntered(object? sender, PointerEventArgs e)
    {
        if (DataContext is not DesignerPanelViewModel vm)
        {
            return;
        }

        if (sender is Control { DataContext: DesignerNeuronViewModel neuron })
        {
            vm.SetHoveredNeuron(neuron);
        }
    }

    private void NeuronPointerExited(object? sender, PointerEventArgs e)
    {
        if (DataContext is DesignerPanelViewModel vm)
        {
            vm.SetHoveredNeuron(null);
        }
    }
}
