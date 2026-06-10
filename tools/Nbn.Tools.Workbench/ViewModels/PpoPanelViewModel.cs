using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Nbn.Proto;
using Nbn.Proto.Ppo;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed class PpoPanelViewModel : ViewModelBase
{
    private const double ChartWidth = 340d;
    private const double ChartHeight = 130d;
    private const double ChartPaddingX = 10d;
    private const double ChartPaddingY = 10d;
    private const int MaxUpdateHistory = 64;
    private readonly WorkbenchClient _client;
    private PpoStatusResponse? _lastStatus;
    private PpoRunDescriptor? _activeRun;
    private PpoRunDescriptor? _lastRun;
    private PpoPolicyUpdateReport? _lastPolicyUpdate;
    private string _status = "PPO status not loaded.";
    private string _runSummary = "No PPO run loaded.";
    private string _policySummary = "No PPO reward update recorded.";
    private string _candidateSummary = "No rollout candidates.";
    private string _parentAGuidText = string.Empty;
    private string _parentBGuidText = string.Empty;
    private string _runIdText = string.Empty;
    private string _objectiveName = "reward";
    private string _metadataJson = "{\"source\":\"workbench\"}";
    private string _rolloutTickCountText = "128";
    private string _rolloutBatchCountText = "4";
    private string _clipEpsilonText = "0.2";
    private string _discountGammaText = "0.99";
    private string _gaeLambdaText = "0.95";
    private string _learningRateText = "0.0003";
    private string _optimizationEpochCountText = "4";
    private string _minibatchSizeText = "32";
    private string _seedText = string.Empty;
    private string _rewardSignal = "output.reward";
    private string _rewardText = string.Empty;
    private string _accuracyText = "0";
    private string _fitnessText = "0";
    private string _generationText = "0";
    private string _rewardMetadataJson = string.Empty;
    private bool _rewardTerminal = true;
    private string _rewardChartRangeLabel = "No reward updates.";
    private string _lossChartRangeLabel = "No loss updates.";
    private string _trustChartRangeLabel = "No trust updates.";
    private string _actionBarsSummary = "No candidate action selected.";
    private string _candidateBarsSummary = "No candidate metrics.";
    private string _rewardChartTopLabel = string.Empty;
    private string _rewardChartMidLabel = string.Empty;
    private string _rewardChartBottomLabel = string.Empty;
    private string _lossChartTopLabel = string.Empty;
    private string _lossChartMidLabel = string.Empty;
    private string _lossChartBottomLabel = string.Empty;
    private string _trustChartTopLabel = string.Empty;
    private string _trustChartMidLabel = string.Empty;
    private string _trustChartBottomLabel = string.Empty;
    private PpoBrainOption? _selectedParentABrain;
    private PpoBrainOption? _selectedParentBBrain;
    private PpoCandidateRow? _selectedCandidate;
    private StrengthSourceOption _selectedStrengthSource;

    public PpoPanelViewModel(WorkbenchClient client, ConnectionViewModel connections)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        Connections = connections ?? throw new ArgumentNullException(nameof(connections));
        ActiveBrains = new ObservableCollection<PpoBrainOption>();
        Candidates = new ObservableCollection<PpoCandidateRow>();
        ObservedParents = new ObservableCollection<PpoObservedParentRow>();
        RewardChartSeries = new ObservableCollection<PpoChartSeriesItem>();
        LossChartSeries = new ObservableCollection<PpoChartSeriesItem>();
        TrustChartSeries = new ObservableCollection<PpoChartSeriesItem>();
        ActionBars = new ObservableCollection<PpoBarItem>();
        CandidateMetricBars = new ObservableCollection<PpoBarItem>();
        PolicyUpdates = new ObservableCollection<PpoPolicyUpdateReport>();
        StrengthSources = new List<StrengthSourceOption>
        {
            new("Base only", StrengthSource.StrengthBaseOnly),
            new("Live codes", StrengthSource.StrengthLiveCodes)
        };
        _selectedStrengthSource = StrengthSources[1];

        RefreshStatusCommand = new AsyncRelayCommand(RefreshStatusAsync);
        SubmitRunCommand = new AsyncRelayCommand(SubmitRunAsync);
        CancelRunCommand = new AsyncRelayCommand(CancelRunAsync, () => ActiveRun is not null);
        RecordRewardCommand = new AsyncRelayCommand(RecordRewardAsync, () => SelectedCandidate is not null);
        ClearHistoryCommand = new RelayCommand(ClearPolicyHistory);
    }

    public ConnectionViewModel Connections { get; }

    public ObservableCollection<PpoBrainOption> ActiveBrains { get; }

    public ObservableCollection<PpoCandidateRow> Candidates { get; }

    public ObservableCollection<PpoObservedParentRow> ObservedParents { get; }

    public ObservableCollection<PpoPolicyUpdateReport> PolicyUpdates { get; }

    public ObservableCollection<PpoChartSeriesItem> RewardChartSeries { get; }

    public ObservableCollection<PpoChartSeriesItem> LossChartSeries { get; }

    public ObservableCollection<PpoChartSeriesItem> TrustChartSeries { get; }

    public ObservableCollection<PpoBarItem> ActionBars { get; }

    public ObservableCollection<PpoBarItem> CandidateMetricBars { get; }

    public IReadOnlyList<StrengthSourceOption> StrengthSources { get; }

    public AsyncRelayCommand RefreshStatusCommand { get; }

    public AsyncRelayCommand SubmitRunCommand { get; }

    public AsyncRelayCommand CancelRunCommand { get; }

    public AsyncRelayCommand RecordRewardCommand { get; }

    public RelayCommand ClearHistoryCommand { get; }

    public double ChartPlotWidth => ChartWidth;

    public double ChartPlotHeight => ChartHeight;

    public bool HasCandidates => Candidates.Count > 0;

    public bool HasObservedParents => ObservedParents.Count > 0;

    public bool HasPolicyUpdates => PolicyUpdates.Count > 0;

    public bool HasActionBars => ActionBars.Count > 0;

    public bool HasCandidateMetricBars => CandidateMetricBars.Count > 0;

    public PpoRunDescriptor? ActiveRun
    {
        get => _activeRun;
        private set
        {
            if (SetProperty(ref _activeRun, value))
            {
                OnPropertyChanged(nameof(HasActiveRun));
                CancelRunCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public PpoRunDescriptor? LastRun
    {
        get => _lastRun;
        private set => SetProperty(ref _lastRun, value);
    }

    public PpoPolicyUpdateReport? LastPolicyUpdate
    {
        get => _lastPolicyUpdate;
        private set => SetProperty(ref _lastPolicyUpdate, value);
    }

    public bool HasActiveRun => ActiveRun is not null;

    public PpoBrainOption? SelectedParentABrain
    {
        get => _selectedParentABrain;
        set => SetProperty(ref _selectedParentABrain, value);
    }

    public PpoBrainOption? SelectedParentBBrain
    {
        get => _selectedParentBBrain;
        set => SetProperty(ref _selectedParentBBrain, value);
    }

    public PpoCandidateRow? SelectedCandidate
    {
        get => _selectedCandidate;
        set
        {
            if (SetProperty(ref _selectedCandidate, value))
            {
                ApplySelectedCandidate(value);
                RecordRewardCommand.RaiseCanExecuteChanged();
            }
        }
    }

    public StrengthSourceOption SelectedStrengthSource
    {
        get => _selectedStrengthSource;
        set => SetProperty(ref _selectedStrengthSource, value);
    }

    public string Status
    {
        get => _status;
        private set => SetProperty(ref _status, value);
    }

    public string RunSummary
    {
        get => _runSummary;
        private set => SetProperty(ref _runSummary, value);
    }

    public string PolicySummary
    {
        get => _policySummary;
        private set => SetProperty(ref _policySummary, value);
    }

    public string CandidateSummary
    {
        get => _candidateSummary;
        private set => SetProperty(ref _candidateSummary, value);
    }

    public string ParentAGuidText
    {
        get => _parentAGuidText;
        set => SetProperty(ref _parentAGuidText, value);
    }

    public string ParentBGuidText
    {
        get => _parentBGuidText;
        set => SetProperty(ref _parentBGuidText, value);
    }

    public string RunIdText
    {
        get => _runIdText;
        set => SetProperty(ref _runIdText, value);
    }

    public string ObjectiveName
    {
        get => _objectiveName;
        set => SetProperty(ref _objectiveName, value);
    }

    public string MetadataJson
    {
        get => _metadataJson;
        set => SetProperty(ref _metadataJson, value);
    }

    public string RolloutTickCountText
    {
        get => _rolloutTickCountText;
        set => SetProperty(ref _rolloutTickCountText, value);
    }

    public string RolloutBatchCountText
    {
        get => _rolloutBatchCountText;
        set => SetProperty(ref _rolloutBatchCountText, value);
    }

    public string ClipEpsilonText
    {
        get => _clipEpsilonText;
        set => SetProperty(ref _clipEpsilonText, value);
    }

    public string DiscountGammaText
    {
        get => _discountGammaText;
        set => SetProperty(ref _discountGammaText, value);
    }

    public string GaeLambdaText
    {
        get => _gaeLambdaText;
        set => SetProperty(ref _gaeLambdaText, value);
    }

    public string LearningRateText
    {
        get => _learningRateText;
        set => SetProperty(ref _learningRateText, value);
    }

    public string OptimizationEpochCountText
    {
        get => _optimizationEpochCountText;
        set => SetProperty(ref _optimizationEpochCountText, value);
    }

    public string MinibatchSizeText
    {
        get => _minibatchSizeText;
        set => SetProperty(ref _minibatchSizeText, value);
    }

    public string SeedText
    {
        get => _seedText;
        set => SetProperty(ref _seedText, value);
    }

    public string RewardSignal
    {
        get => _rewardSignal;
        set => SetProperty(ref _rewardSignal, value);
    }

    public string RewardText
    {
        get => _rewardText;
        set => SetProperty(ref _rewardText, value);
    }

    public string AccuracyText
    {
        get => _accuracyText;
        set => SetProperty(ref _accuracyText, value);
    }

    public string FitnessText
    {
        get => _fitnessText;
        set => SetProperty(ref _fitnessText, value);
    }

    public string GenerationText
    {
        get => _generationText;
        set => SetProperty(ref _generationText, value);
    }

    public string RewardMetadataJson
    {
        get => _rewardMetadataJson;
        set => SetProperty(ref _rewardMetadataJson, value);
    }

    public bool RewardTerminal
    {
        get => _rewardTerminal;
        set => SetProperty(ref _rewardTerminal, value);
    }

    public string RewardChartRangeLabel
    {
        get => _rewardChartRangeLabel;
        private set => SetProperty(ref _rewardChartRangeLabel, value);
    }

    public string LossChartRangeLabel
    {
        get => _lossChartRangeLabel;
        private set => SetProperty(ref _lossChartRangeLabel, value);
    }

    public string TrustChartRangeLabel
    {
        get => _trustChartRangeLabel;
        private set => SetProperty(ref _trustChartRangeLabel, value);
    }

    public string ActionBarsSummary
    {
        get => _actionBarsSummary;
        private set => SetProperty(ref _actionBarsSummary, value);
    }

    public string CandidateBarsSummary
    {
        get => _candidateBarsSummary;
        private set => SetProperty(ref _candidateBarsSummary, value);
    }

    public string RewardChartTopLabel
    {
        get => _rewardChartTopLabel;
        private set => SetProperty(ref _rewardChartTopLabel, value);
    }

    public string RewardChartMidLabel
    {
        get => _rewardChartMidLabel;
        private set => SetProperty(ref _rewardChartMidLabel, value);
    }

    public string RewardChartBottomLabel
    {
        get => _rewardChartBottomLabel;
        private set => SetProperty(ref _rewardChartBottomLabel, value);
    }

    public string LossChartTopLabel
    {
        get => _lossChartTopLabel;
        private set => SetProperty(ref _lossChartTopLabel, value);
    }

    public string LossChartMidLabel
    {
        get => _lossChartMidLabel;
        private set => SetProperty(ref _lossChartMidLabel, value);
    }

    public string LossChartBottomLabel
    {
        get => _lossChartBottomLabel;
        private set => SetProperty(ref _lossChartBottomLabel, value);
    }

    public string TrustChartTopLabel
    {
        get => _trustChartTopLabel;
        private set => SetProperty(ref _trustChartTopLabel, value);
    }

    public string TrustChartMidLabel
    {
        get => _trustChartMidLabel;
        private set => SetProperty(ref _trustChartMidLabel, value);
    }

    public string TrustChartBottomLabel
    {
        get => _trustChartBottomLabel;
        private set => SetProperty(ref _trustChartBottomLabel, value);
    }

    public void UpdateActiveBrains(IReadOnlyList<BrainListItem> brains)
    {
        var selectedA = SelectedParentABrain?.BrainId;
        var selectedB = SelectedParentBBrain?.BrainId;
        var active = brains
            .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            .OrderBy(entry => entry.BrainId)
            .Select(entry => new PpoBrainOption(entry.BrainId, entry.Display))
            .ToList();

        ActiveBrains.Clear();
        foreach (var item in active)
        {
            ActiveBrains.Add(item);
        }

        SelectedParentABrain = selectedA.HasValue
            ? ActiveBrains.FirstOrDefault(item => item.BrainId == selectedA.Value)
            : ActiveBrains.FirstOrDefault();
        SelectedParentBBrain = selectedB.HasValue
            ? ActiveBrains.FirstOrDefault(item => item.BrainId == selectedB.Value)
            : ActiveBrains.FirstOrDefault(item => SelectedParentABrain is null || item.BrainId != SelectedParentABrain.BrainId);
    }

    public async Task RefreshStatusAsync()
    {
        Status = "Refreshing PPO status...";
        ApplyStatus(await _client.GetPpoStatusAsync().ConfigureAwait(false));
    }

    public async Task SubmitRunAsync()
    {
        if (!TryBuildStartRequest(out var request, out var reason))
        {
            Status = reason;
            return;
        }

        Status = "Submitting PPO rollout...";
        var response = await _client.StartPpoRunAsync(request).ConfigureAwait(false);
        if (!response.Accepted || response.FailureReason != PpoFailureReason.PpoFailureNone)
        {
            Status = BuildFailureStatus("PPO rollout rejected", response.FailureReason, response.FailureDetail);
            return;
        }

        ActiveRun = response.Run?.Clone();
        Status = $"PPO rollout accepted: {ActiveRun?.RunId ?? "unknown"}.";
        UpdateSummaries();
    }

    public async Task CancelRunAsync()
    {
        if (ActiveRun is null)
        {
            Status = "No active PPO run to cancel.";
            return;
        }

        var response = await _client.StopPpoRunAsync(new PpoStopRunRequest
        {
            RunId = ActiveRun.RunId,
            Reason = "workbench_cancel"
        }).ConfigureAwait(false);

        if (!response.Stopped || response.FailureReason != PpoFailureReason.PpoFailureNone)
        {
            Status = BuildFailureStatus("PPO cancel rejected", response.FailureReason, response.FailureDetail);
            return;
        }

        ActiveRun = null;
        LastRun = response.Run?.Clone();
        Status = $"PPO run cancelled: {LastRun?.RunId ?? "unknown"}.";
        ApplyRun(LastRun);
        UpdateSummaries();
    }

    public async Task RecordRewardAsync()
    {
        if (SelectedCandidate is null)
        {
            Status = "Select a PPO candidate before recording reward.";
            return;
        }

        if (!TryParseFiniteFloat(RewardText, out var reward)
            || !TryParseFiniteFloat(AccuracyText, out var accuracy)
            || !TryParseFiniteFloat(FitnessText, out var fitness))
        {
            Status = "Reward, accuracy, and fitness must be finite numbers.";
            return;
        }

        if (!TryParseUlong(GenerationText, out var generation))
        {
            Status = "Generation must be a non-negative integer.";
            return;
        }

        var hyperparameters = LastRun?.Hyperparameters?.Clone()
                              ?? ActiveRun?.Hyperparameters?.Clone()
                              ?? BuildHyperparameters();
        var request = new PpoRecordRewardsRequest
        {
            ObjectiveName = ResolveObjectiveName(),
            RewardSignal = ResolveRewardSignal(hyperparameters),
            Hyperparameters = hyperparameters,
            MetadataJson = RewardMetadataJson.Trim()
        };
        request.Samples.Add(new PpoRewardSample
        {
            RunId = SelectedCandidate.RunId,
            RunIndex = SelectedCandidate.RunIndex,
            ChildDef = SelectedCandidate.ChildDef?.Clone(),
            Reward = reward,
            Accuracy = accuracy,
            Fitness = fitness,
            Generation = generation,
            Terminal = RewardTerminal,
            MetadataJson = RewardMetadataJson.Trim()
        });

        Status = "Recording PPO reward sample...";
        var response = await _client.RecordPpoRewardsAsync(request).ConfigureAwait(false);
        if (!response.Accepted || response.FailureReason != PpoFailureReason.PpoFailureNone)
        {
            Status = BuildFailureStatus("PPO reward rejected", response.FailureReason, response.FailureDetail);
            return;
        }

        AddPolicyUpdate(response.Update);
        LastPolicyUpdate = response.Update?.Clone();
        Status = $"PPO reward accepted: update {response.Update?.UpdateIndex ?? 0}.";
        UpdatePolicySummary();
    }

    internal bool TryBuildStartRequest(out PpoStartRunRequest request, out string failureReason)
    {
        request = new PpoStartRunRequest();
        failureReason = string.Empty;
        if (!TryResolveParentId(SelectedParentABrain, ParentAGuidText, out var parentA)
            || !TryResolveParentId(SelectedParentBBrain, ParentBGuidText, out var parentB))
        {
            failureReason = "Select active parent brains or provide valid parent GUIDs.";
            return false;
        }

        if (parentA == parentB)
        {
            failureReason = "Parent A and Parent B must be different.";
            return false;
        }

        var hyperparameters = BuildHyperparameters();
        if (hyperparameters.RolloutTickCount == 0 || hyperparameters.RolloutBatchCount == 0)
        {
            failureReason = "Rollout ticks and batch count must be greater than zero.";
            return false;
        }

        if (hyperparameters.ClipEpsilon <= 0f || hyperparameters.ClipEpsilon > 1f
            || hyperparameters.DiscountGamma <= 0f || hyperparameters.DiscountGamma > 1f
            || hyperparameters.GaeLambda <= 0f || hyperparameters.GaeLambda > 1f
            || hyperparameters.LearningRate <= 0f
            || hyperparameters.OptimizationEpochCount == 0
            || hyperparameters.MinibatchSize == 0)
        {
            failureReason = "PPO hyperparameters are outside accepted ranges.";
            return false;
        }

        request = new PpoStartRunRequest
        {
            RunId = RunIdText.Trim(),
            ObjectiveName = ResolveObjectiveName(),
            MetadataJson = MetadataJson.Trim(),
            Hyperparameters = hyperparameters,
            ReproduceConfig = BuildReproduceConfig(),
            StrengthSource = SelectedStrengthSource.Value
        };
        request.ParentBrainIds.Add(parentA.ToProtoUuid());
        request.ParentBrainIds.Add(parentB.ToProtoUuid());
        return true;
    }

    private void ApplyStatus(PpoStatusResponse response)
    {
        _lastStatus = response;
        if (response.FailureReason != PpoFailureReason.PpoFailureNone)
        {
            Status = BuildFailureStatus("PPO status failed", response.FailureReason, response.FailureDetail);
        }
        else
        {
            var dependencies = response.Dependencies;
            var ready = dependencies?.IoAvailable == true
                        && dependencies.ReproductionAvailable
                        && dependencies.SpeciationAvailable;
            Status = ready ? "PPO ready." : "PPO reachable; waiting on dependencies.";
        }

        ActiveRun = response.ActiveRun?.Clone();
        LastRun = response.LastRun?.Clone();
        LastPolicyUpdate = response.LastPolicyUpdate?.Clone();
        AddPolicyUpdate(response.LastPolicyUpdate);
        ApplyRun(LastRun ?? ActiveRun);
        UpdateSummaries();
    }

    private void ApplyRun(PpoRunDescriptor? run)
    {
        Candidates.Clear();
        ObservedParents.Clear();
        CandidateMetricBars.Clear();
        SelectedCandidate = null;

        var report = run?.ExecutionReport;
        if (run is null || report is null)
        {
            CandidateSummary = "No rollout candidates.";
            OnPropertyChanged(nameof(HasCandidates));
            OnPropertyChanged(nameof(HasObservedParents));
            OnPropertyChanged(nameof(HasCandidateMetricBars));
            UpdateActionBars(null);
            return;
        }

        foreach (var parent in report.ObservedParents)
        {
            ObservedParents.Add(PpoObservedParentRow.From(parent));
        }

        foreach (var candidate in report.Candidates.OrderBy(item => item.RunIndex))
        {
            Candidates.Add(PpoCandidateRow.From(run.RunId, candidate));
        }

        SelectedCandidate = Candidates.FirstOrDefault();
        UpdateCandidateMetricBars();
        CandidateSummary = Candidates.Count == 0
            ? "No rollout candidates."
            : $"{Candidates.Count} candidate artifact(s), {ObservedParents.Count} observed parent snapshot(s).";
        OnPropertyChanged(nameof(HasCandidates));
        OnPropertyChanged(nameof(HasObservedParents));
        OnPropertyChanged(nameof(HasCandidateMetricBars));
    }

    private void ApplySelectedCandidate(PpoCandidateRow? candidate)
    {
        UpdateActionBars(candidate);
        if (candidate is not null && string.IsNullOrWhiteSpace(RewardText))
        {
            RewardText = candidate.SimilarityScore > 0f
                ? candidate.SimilarityScore.ToString("0.###", CultureInfo.InvariantCulture)
                : "0";
            FitnessText = RewardText;
            AccuracyText = candidate.Compatible ? "1" : "0";
        }
    }

    private void AddPolicyUpdate(PpoPolicyUpdateReport? update)
    {
        if (update is null || update.UpdateIndex == 0)
        {
            return;
        }

        var existing = PolicyUpdates.FirstOrDefault(item => item.UpdateIndex == update.UpdateIndex);
        if (existing is not null)
        {
            PolicyUpdates.Remove(existing);
        }

        PolicyUpdates.Add(update.Clone());
        while (PolicyUpdates.Count > MaxUpdateHistory)
        {
            PolicyUpdates.RemoveAt(0);
        }

        OnPropertyChanged(nameof(HasPolicyUpdates));
        RebuildCharts();
    }

    private void RebuildCharts()
    {
        var updates = PolicyUpdates.OrderBy(item => item.UpdateIndex).ToList();
        BuildLineChart(
            RewardChartSeries,
            updates,
            [
                new("Mean reward", "#2F9C8A", static update => update.MeanReward),
                new("Max reward", "#F16D3A", static update => update.MaxReward)
            ],
            "Reward updates",
            out var rewardRange,
            out var rewardTop,
            out var rewardMid,
            out var rewardBottom);
        RewardChartRangeLabel = rewardRange;
        RewardChartTopLabel = rewardTop;
        RewardChartMidLabel = rewardMid;
        RewardChartBottomLabel = rewardBottom;

        BuildLineChart(
            LossChartSeries,
            updates,
            [
                new("Policy loss", "#2F6F9C", static update => update.PolicyLoss),
                new("Value loss", "#E2B548", static update => update.ValueLoss)
            ],
            "Loss updates",
            out var lossRange,
            out var lossTop,
            out var lossMid,
            out var lossBottom);
        LossChartRangeLabel = lossRange;
        LossChartTopLabel = lossTop;
        LossChartMidLabel = lossMid;
        LossChartBottomLabel = lossBottom;

        BuildLineChart(
            TrustChartSeries,
            updates,
            [
                new("Entropy", "#7B6FB0", static update => update.Entropy),
                new("Approx KL", "#8A6A38", static update => update.ApproximateKl)
            ],
            "Trust updates",
            out var trustRange,
            out var trustTop,
            out var trustMid,
            out var trustBottom);
        TrustChartRangeLabel = trustRange;
        TrustChartTopLabel = trustTop;
        TrustChartMidLabel = trustMid;
        TrustChartBottomLabel = trustBottom;
    }

    private void UpdateActionBars(PpoCandidateRow? candidate)
    {
        ActionBars.Clear();
        if (candidate is null)
        {
            ActionBarsSummary = "No candidate action selected.";
            OnPropertyChanged(nameof(HasActionBars));
            return;
        }

        foreach (var item in ReadActionProbabilities(candidate.ActionJson).OrderByDescending(item => item.Value).Take(11))
        {
            ActionBars.Add(PpoBarItem.FromProbability(FormatActionLabel(item.Key), item.Value, "#2F9C8A"));
        }

        ActionBarsSummary = ActionBars.Count == 0
            ? "Candidate did not include action probabilities."
            : $"Candidate {candidate.RunIndex} action probabilities.";
        OnPropertyChanged(nameof(HasActionBars));
    }

    private void UpdateCandidateMetricBars()
    {
        CandidateMetricBars.Clear();
        if (Candidates.Count == 0)
        {
            CandidateBarsSummary = "No candidate metrics.";
            return;
        }

        var top = Candidates
            .OrderByDescending(item => item.SimilarityScore)
            .ThenBy(item => item.RunIndex)
            .Take(10)
            .ToList();
        foreach (var candidate in top)
        {
            CandidateMetricBars.Add(PpoBarItem.FromProbability($"#{candidate.RunIndex}", candidate.SimilarityScore, candidate.Compatible ? "#2F9C8A" : "#F16D3A"));
        }

        CandidateBarsSummary = "Candidate similarity score by rollout index.";
    }

    private void UpdateSummaries()
    {
        var dependencies = _lastStatus?.Dependencies;
        var depText = dependencies is null
            ? "Dependencies unknown"
            : $"IO {(dependencies.IoAvailable ? "ready" : "missing")} | Repro {(dependencies.ReproductionAvailable ? "ready" : "missing")} | Speciation {(dependencies.SpeciationAvailable ? "ready" : "missing")}";
        var run = ActiveRun ?? LastRun;
        RunSummary = run is null
            ? $"{depText}. No active or completed PPO run loaded."
            : $"{depText}. {run.State} run {run.RunId} | {run.StatusDetail}";
        UpdatePolicySummary();
    }

    private void UpdatePolicySummary()
    {
        var update = LastPolicyUpdate;
        PolicySummary = update is null || update.UpdateIndex == 0
            ? "No PPO reward update recorded."
            : string.Create(
                CultureInfo.InvariantCulture,
                $"Update {update.UpdateIndex} | samples={update.AcceptedSampleCount} | mean={update.MeanReward:0.###} | max={update.MaxReward:0.###} | entropy={update.Entropy:0.###} | KL={update.ApproximateKl:0.####}");
    }

    private PpoHyperparameters BuildHyperparameters()
        => new()
        {
            RolloutTickCount = ParseUlong(RolloutTickCountText, 128),
            RolloutBatchCount = ParseUlong(RolloutBatchCountText, 4),
            ClipEpsilon = ParseFloat(ClipEpsilonText, 0.2f),
            DiscountGamma = ParseFloat(DiscountGammaText, 0.99f),
            GaeLambda = ParseFloat(GaeLambdaText, 0.95f),
            LearningRate = ParseFloat(LearningRateText, 0.0003f),
            OptimizationEpochCount = ParseUint(OptimizationEpochCountText, 4),
            MinibatchSize = ParseUint(MinibatchSizeText, 32),
            Seed = ParseUlong(SeedText, (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
            RewardSignal = RewardSignal.Trim()
        };

    private ReproduceConfig BuildReproduceConfig()
        => new()
        {
            SpawnChild = SpawnChildPolicy.SpawnChildNever,
            ProtectIoRegionNeuronCounts = true,
            MaxRegionSpanDiffRatio = ReproductionSettings.DefaultMaxRegionSpanDiffRatio,
            MaxFunctionHistDistance = ReproductionSettings.DefaultMaxFunctionHistDistance,
            MaxConnectivityHistDistance = ReproductionSettings.DefaultMaxConnectivityHistDistance,
            ProbMutate = 0.05f,
            ProbMutateFunc = 0.02f,
            ProbAddAxon = 0.05f,
            ProbRemoveAxon = 0.02f,
            ProbRerouteAxon = 0.02f,
            ProbDisableNeuron = 0.01f,
            ProbReactivateNeuron = 0.01f,
            ProbAddNeuronToEmptyRegion = 0.01f,
            ProbRemoveLastNeuronFromRegion = 0.005f,
            ProbRerouteInboundAxonOnDelete = 0.50f,
            StrengthTransformEnabled = true,
            ProbStrengthMutate = 0.05f
        };

    private string ResolveObjectiveName()
        => string.IsNullOrWhiteSpace(ObjectiveName) ? "reward" : ObjectiveName.Trim();

    private string ResolveRewardSignal(PpoHyperparameters hyperparameters)
        => string.IsNullOrWhiteSpace(hyperparameters.RewardSignal) ? "output.reward" : hyperparameters.RewardSignal.Trim();

    private void ClearPolicyHistory()
    {
        PolicyUpdates.Clear();
        LastPolicyUpdate = null;
        OnPropertyChanged(nameof(HasPolicyUpdates));
        RebuildCharts();
        UpdatePolicySummary();
    }

    private static void BuildLineChart(
        ObservableCollection<PpoChartSeriesItem> target,
        IReadOnlyList<PpoPolicyUpdateReport> updates,
        IReadOnlyList<ChartMetric> metrics,
        string emptyLabel,
        out string rangeLabel,
        out string topLabel,
        out string midLabel,
        out string bottomLabel)
    {
        target.Clear();
        if (updates.Count == 0)
        {
            rangeLabel = emptyLabel + ": no samples.";
            topLabel = midLabel = bottomLabel = string.Empty;
            return;
        }

        var values = updates.SelectMany(update => metrics.Select(metric => metric.Value(update))).Where(float.IsFinite).ToArray();
        if (values.Length == 0)
        {
            rangeLabel = emptyLabel + ": non-finite samples.";
            topLabel = midLabel = bottomLabel = string.Empty;
            return;
        }

        var min = values.Min();
        var max = values.Max();
        if (Math.Abs(max - min) < 0.000001f)
        {
            min -= 1f;
            max += 1f;
        }

        foreach (var metric in metrics)
        {
            var points = updates
                .Select((update, index) => (Index: index, Value: metric.Value(update)))
                .Where(item => float.IsFinite(item.Value))
                .ToArray();
            var path = BuildPath(points, updates.Count, min, max);
            if (!string.IsNullOrWhiteSpace(path))
            {
                target.Add(new PpoChartSeriesItem(metric.Label, metric.Stroke, path));
            }
        }

        rangeLabel = updates.Count == 1
            ? $"Update {updates[0].UpdateIndex}"
            : $"Updates {updates[0].UpdateIndex}..{updates[^1].UpdateIndex}";
        topLabel = FormatFloat(max);
        midLabel = FormatFloat((min + max) / 2f);
        bottomLabel = FormatFloat(min);
    }

    private static string BuildPath(IReadOnlyList<(int Index, float Value)> points, int totalCount, float min, float max)
    {
        if (points.Count == 0)
        {
            return string.Empty;
        }

        var span = Math.Max(0.000001f, max - min);
        var xDenom = Math.Max(1, totalCount - 1);
        var parts = new List<string>(points.Count);
        for (var i = 0; i < points.Count; i++)
        {
            var x = ChartPaddingX + ((ChartWidth - (ChartPaddingX * 2d)) * points[i].Index / xDenom);
            var normalized = (points[i].Value - min) / span;
            var y = ChartHeight - ChartPaddingY - ((ChartHeight - (ChartPaddingY * 2d)) * normalized);
            parts.Add(string.Create(CultureInfo.InvariantCulture, $"{(i == 0 ? "M" : "L")} {x:0.##} {y:0.##}"));
        }

        return string.Join(" ", parts);
    }

    private static IReadOnlyList<KeyValuePair<string, float>> ReadActionProbabilities(string actionJson)
    {
        if (string.IsNullOrWhiteSpace(actionJson))
        {
            return Array.Empty<KeyValuePair<string, float>>();
        }

        try
        {
            using var document = JsonDocument.Parse(actionJson);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return Array.Empty<KeyValuePair<string, float>>();
            }

            var values = new List<KeyValuePair<string, float>>();
            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (property.Value.ValueKind == JsonValueKind.Number
                    && property.Value.TryGetSingle(out var value)
                    && float.IsFinite(value))
                {
                    values.Add(new KeyValuePair<string, float>(property.Name, Math.Clamp(value, 0f, 1f)));
                }
            }

            return values;
        }
        catch (JsonException)
        {
            return Array.Empty<KeyValuePair<string, float>>();
        }
    }

    private static string FormatActionLabel(string key)
        => key.Replace('_', ' ');

    private static bool TryResolveParentId(PpoBrainOption? selected, string rawText, out Guid brainId)
    {
        if (selected is not null)
        {
            brainId = selected.BrainId;
            return true;
        }

        if (Guid.TryParse(rawText, out brainId) && brainId != Guid.Empty)
        {
            return true;
        }

        brainId = Guid.Empty;
        return false;
    }

    private static float ParseFloat(string value, float fallback)
        => float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && float.IsFinite(parsed)
            ? parsed
            : fallback;

    private static bool TryParseFiniteFloat(string value, out float parsed)
        => float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out parsed) && float.IsFinite(parsed);

    private static ulong ParseUlong(string value, ulong fallback)
        => ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;

    private static bool TryParseUlong(string value, out ulong parsed)
        => ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed);

    private static uint ParseUint(string value, uint fallback)
        => uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;

    private static string BuildFailureStatus(string prefix, PpoFailureReason reason, string detail)
        => string.IsNullOrWhiteSpace(detail) ? $"{prefix}: {reason}." : $"{prefix}: {detail}";

    private static string FormatFloat(float value)
        => value.ToString("0.###", CultureInfo.InvariantCulture);

    private sealed record ChartMetric(string Label, string Stroke, Func<PpoPolicyUpdateReport, float> Value);
}

public sealed record PpoBrainOption(Guid BrainId, string Label)
{
    public string BrainIdLabel => BrainId.ToString("D");
}

public sealed record PpoChartSeriesItem(string Label, string Stroke, string PathData);

public sealed record PpoBarItem(string Label, string ValueLabel, double Width, string Fill)
{
    public static PpoBarItem FromProbability(string label, float value, string fill)
    {
        var clamped = Math.Clamp(float.IsFinite(value) ? value : 0f, 0f, 1f);
        return new PpoBarItem(
            label,
            clamped.ToString("0.###", CultureInfo.InvariantCulture),
            Math.Max(2d, clamped * 180d),
            fill);
    }
}

public sealed record PpoObservedParentRow(
    string BrainId,
    string SnapshotTick,
    string SnapshotSource,
    string DefinitionSha,
    string SnapshotSha)
{
    public static PpoObservedParentRow From(PpoObservedParent parent)
    {
        var brainId = parent.BrainId is not null && parent.BrainId.TryToGuid(out var guid)
            ? guid.ToString("D")
            : "unknown";
        return new PpoObservedParentRow(
            brainId,
            parent.SnapshotTickId.ToString(CultureInfo.InvariantCulture),
            string.IsNullOrWhiteSpace(parent.SnapshotSource) ? "live_tick_boundary" : parent.SnapshotSource,
            ShortSha(parent.BrainDef),
            ShortSha(parent.Snapshot));
    }

    private static string ShortSha(ArtifactRef? artifact)
        => artifact is not null && artifact.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "missing";
}

public sealed record PpoCandidateRow(
    string RunId,
    uint RunIndex,
    ulong Seed,
    string ChildSha,
    string ChildStore,
    string CompatibleLabel,
    bool Compatible,
    float SimilarityScore,
    string SimilarityLabel,
    string MutationLabel,
    string SpeciesLabel,
    float OldLogProbability,
    float ValueEstimate,
    string ActionJson,
    ArtifactRef? ChildDef)
{
    public static PpoCandidateRow From(string runId, PpoCandidateResult candidate)
    {
        var report = candidate.ReproductionReport;
        var summary = candidate.MutationSummary;
        var decision = candidate.SpeciationDecision;
        var similarity = ResolveSimilarityScore(report);
        return new PpoCandidateRow(
            runId,
            candidate.RunIndex,
            candidate.Seed,
            ShortSha(candidate.ChildDef),
            candidate.ChildDef?.StoreUri ?? string.Empty,
            report is null ? "Unknown" : report.Compatible ? "Compatible" : "Rejected",
            report?.Compatible == true,
            similarity,
            similarity.ToString("0.###", CultureInfo.InvariantCulture),
            $"+N{summary?.NeuronsAdded ?? 0u} -N{summary?.NeuronsRemoved ?? 0u} +A{summary?.AxonsAdded ?? 0u} -A{summary?.AxonsRemoved ?? 0u} reroute={summary?.AxonsRerouted ?? 0u}",
            string.IsNullOrWhiteSpace(decision?.SpeciesId) ? "unassigned" : decision.SpeciesId,
            candidate.OldLogProbability,
            candidate.ValueEstimate,
            candidate.ActionJson ?? string.Empty,
            candidate.ChildDef?.Clone());
    }

    private static float ResolveSimilarityScore(SimilarityReport? report)
    {
        if (report is null)
        {
            return 0f;
        }

        if (report.SimilarityScore > 0f)
        {
            return Math.Clamp(report.SimilarityScore, 0f, 1f);
        }

        var values = new[] { report.RegionSpanScore, report.FunctionScore, report.ConnectivityScore }
            .Where(value => value > 0f && float.IsFinite(value))
            .ToArray();
        return values.Length == 0
            ? report.Compatible ? 1f : 0f
            : Math.Clamp(values.Average(), 0f, 1f);
    }

    private static string ShortSha(ArtifactRef? artifact)
        => artifact is not null && artifact.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "missing";
}
