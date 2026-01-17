namespace Nbn.Tools.Workbench.ViewModels;

public sealed class NavItemViewModel : ViewModelBase
{
    public NavItemViewModel(string title, string subtitle, string glyph, object panel)
    {
        Title = title;
        Subtitle = subtitle;
        Glyph = glyph;
        Panel = panel;
    }

    public string Title { get; }
    public string Subtitle { get; }
    public string Glyph { get; }
    public object Panel { get; }
}
