using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.Services;

public interface IWorkbenchEventSink
{
    void OnOutputEvent(OutputEventItem item);
    void OnOutputVectorEvent(OutputVectorEventItem item);
    void OnDebugEvent(DebugEventItem item);
    void OnVizEvent(VizEventItem item);
    void OnBrainTerminated(BrainTerminatedItem item);
    void OnIoStatus(string status, bool connected);
    void OnObsStatus(string status, bool connected);
}
