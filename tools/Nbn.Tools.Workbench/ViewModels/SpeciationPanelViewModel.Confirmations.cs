namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class SpeciationPanelViewModel
{
    /// <summary>
    /// Gets the current start-new-epoch command label, including its confirmation state.
    /// </summary>
    public string StartNewEpochLabel => _startNewEpochConfirmPending ? "Confirm New Epoch" : "Start New Epoch";

    /// <summary>
    /// Gets the current clear-history command label, including its confirmation state.
    /// </summary>
    public string ClearAllHistoryLabel => _clearAllHistoryConfirmPending ? "Confirm Delete All Epochs" : "Delete All Epochs";

    /// <summary>
    /// Gets the current delete-epoch command label, including its confirmation state.
    /// </summary>
    public string DeleteEpochLabel => _deleteEpochConfirmPending ? "Confirm Delete Epoch" : "Delete Epoch";

    private void SetStartNewEpochConfirmation(bool pending)
    {
        if (_startNewEpochConfirmPending == pending)
        {
            return;
        }

        _startNewEpochConfirmPending = pending;
        OnPropertyChanged(nameof(StartNewEpochLabel));
    }

    private void SetClearAllHistoryConfirmation(bool pending)
    {
        if (_clearAllHistoryConfirmPending == pending)
        {
            return;
        }

        _clearAllHistoryConfirmPending = pending;
        OnPropertyChanged(nameof(ClearAllHistoryLabel));
    }

    private void ArmDeleteEpochConfirmation(long epochId)
    {
        var labelChanged = !_deleteEpochConfirmPending;
        _deleteEpochConfirmPending = true;
        _deleteEpochConfirmTarget = epochId;
        if (labelChanged)
        {
            OnPropertyChanged(nameof(DeleteEpochLabel));
        }
    }

    private void ResetDeleteEpochConfirmation()
    {
        if (!_deleteEpochConfirmPending && !_deleteEpochConfirmTarget.HasValue)
        {
            return;
        }

        _deleteEpochConfirmPending = false;
        _deleteEpochConfirmTarget = null;
        OnPropertyChanged(nameof(DeleteEpochLabel));
    }

    private void ResetHistoryMutationConfirmations()
    {
        SetClearAllHistoryConfirmation(false);
        ResetDeleteEpochConfirmation();
    }
}
