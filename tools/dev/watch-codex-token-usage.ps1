param(
    [string]$SessionFile = "",
    [int]$ThresholdPercent = 85,
    [string]$IssueId = "",
    [switch]$AutoCheckpoint,
    [string]$DbPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-LatestSessionFile {
    $root = Join-Path $env:USERPROFILE ".codex\sessions"
    if (-not (Test-Path $root)) {
        throw "Codex sessions root not found: $root"
    }

    $file = Get-ChildItem $root -Recurse -Filter *.jsonl -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    return $file
}

function Resolve-SessionFile {
    param([string]$Candidate)

    if (-not [string]::IsNullOrWhiteSpace($Candidate)) {
        $resolved = Resolve-Path $Candidate -ErrorAction Stop
        return $resolved.Path
    }

    while ($true) {
        $latest = Get-LatestSessionFile
        if ($latest) {
            return $latest.FullName
        }

        Start-Sleep -Milliseconds 500
    }
}

function Add-CheckpointComment {
    param(
        [string]$BeadsIssueId,
        [double]$PercentUsed,
        [double]$TotalTokens,
        [double]$WindowTokens
    )

    if ([string]::IsNullOrWhiteSpace($BeadsIssueId)) {
        return
    }

    $timestamp = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
    $text = "Auto-checkpoint: token usage reached $([math]::Round($PercentUsed,2))% ($([int64]$TotalTokens)/$([int64]$WindowTokens)) at $timestamp UTC. Capture current implementation status before next large turn."
    $args = @("comments", "add", $BeadsIssueId, $text)
    if (-not [string]::IsNullOrWhiteSpace($DbPath)) {
        $args += @("--db", $DbPath)
    }

    & bd @args | Out-Null
}

$session = Resolve-SessionFile -Candidate $SessionFile
Write-Output "Watching Codex token usage from: $session"
Write-Output "Threshold: $ThresholdPercent%"
if ($AutoCheckpoint -and -not [string]::IsNullOrWhiteSpace($IssueId)) {
    Write-Output "Auto-checkpoint enabled for issue: $IssueId"
}

$lastTurnId = ""
$checkpointedTurnIds = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)

Get-Content $session -Wait | ForEach-Object {
    if ([string]::IsNullOrWhiteSpace($_)) {
        return
    }

    $line = $null
    try {
        $line = $_ | ConvertFrom-Json
    }
    catch {
        return
    }

    if ($line.type -ne "event_msg") {
        return
    }

    if ($null -eq $line.payload -or $line.payload.type -ne "token_count") {
        return
    }

    if ($null -eq $line.payload.info) {
        return
    }

    $usage = $line.payload.info.total_token_usage
    $window = [double]$line.payload.info.model_context_window
    if ($null -eq $usage -or $window -le 0) {
        return
    }

    $total = [double]$usage.total_tokens
    $percentUsed = [math]::Round((100.0 * $total / $window), 2)

    $turnId = ""
    if ($line.payload.PSObject.Properties.Name -contains "turn_id") {
        $turnId = [string]$line.payload.turn_id
    }

    if (-not [string]::IsNullOrWhiteSpace($turnId)) {
        $lastTurnId = $turnId
    }

    $timestamp = Get-Date -Format "HH:mm:ss"
    Write-Output "$timestamp tokens=$([int64]$total) window=$([int64]$window) used=$percentUsed%"

    if (-not $AutoCheckpoint -or [string]::IsNullOrWhiteSpace($IssueId)) {
        return
    }

    if ($percentUsed -lt $ThresholdPercent) {
        return
    }

    $checkpointKey = if ([string]::IsNullOrWhiteSpace($lastTurnId)) { "__unknown_turn__" } else { $lastTurnId }
    if ($checkpointedTurnIds.Contains($checkpointKey)) {
        return
    }

    Add-CheckpointComment -BeadsIssueId $IssueId -PercentUsed $percentUsed -TotalTokens $total -WindowTokens $window
    $checkpointedTurnIds.Add($checkpointKey) | Out-Null
    Write-Output "Checkpoint comment added to $IssueId at $percentUsed%."
}
