param(
    [string]$DemoRoot = (Join-Path $PSScriptRoot "local-demo"),
    [string]$BindHost = "127.0.0.1",
    [int]$HiveMindPort = 12020,
    [int]$BrainHostPort = 12010,
    [int]$RegionHostPort = 12040,
    [int]$RegionId = 1,
    [int]$ShardIndex = 0,
    [string]$RouterId = "demo-router"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$artifactRoot = Join-Path $DemoRoot "artifacts"
$logRoot = Join-Path $DemoRoot "logs"

New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

$brainId = [guid]::NewGuid().ToString()
$hiveAddress = "$BindHost:$HiveMindPort"
$brainAddress = "$BindHost:$BrainHostPort"
$regionAddress = "$BindHost:$RegionHostPort"

Write-Host "Demo root: $DemoRoot"
Write-Host "BrainId: $brainId"

$artifactJson = & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -- init-artifacts --artifact-root "$artifactRoot" --json
$artifact = $artifactJson | ConvertFrom-Json

$hiveLog = Join-Path $logRoot "hivemind.log"
$brainLog = Join-Path $logRoot "brainhost.log"
$regionLog = Join-Path $logRoot "regionhost.log"

$hiveArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind"),
    "--",
    "--bind-host", $BindHost,
    "--port", $HiveMindPort
)

$brainArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost"),
    "--",
    "run-brain",
    "--bind-host", $BindHost,
    "--port", $BrainHostPort,
    "--brain-id", $brainId,
    "--hivemind-address", $hiveAddress,
    "--hivemind-id", "HiveMind",
    "--router-id", $RouterId
)

$regionArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.RegionHost"),
    "--",
    "--bind-host", $BindHost,
    "--port", $RegionHostPort,
    "--brain-id", $brainId,
    "--region", $RegionId,
    "--neuron-start", 0,
    "--neuron-count", 1,
    "--shard-index", $ShardIndex,
    "--router-address", $brainAddress,
    "--router-id", $RouterId,
    "--tick-address", $hiveAddress,
    "--tick-id", "HiveMind",
    "--nbn-sha256", $artifact.nbn_sha256,
    "--nbn-size", $artifact.nbn_size,
    "--artifact-root", $artifactRoot
)

$hiveProc = Start-Process -FilePath "dotnet" -ArgumentList $hiveArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $hiveLog -RedirectStandardError $hiveLog
Start-Sleep -Seconds 1
$brainProc = Start-Process -FilePath "dotnet" -ArgumentList $brainArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $brainLog -RedirectStandardError $brainLog
Start-Sleep -Seconds 1
$regionProc = Start-Process -FilePath "dotnet" -ArgumentList $regionArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionLog -RedirectStandardError $regionLog

Write-Host "HiveMind: $hiveAddress (pid $($hiveProc.Id))"
Write-Host "BrainHost: $brainAddress (pid $($brainProc.Id))"
Write-Host "RegionHost: $regionAddress (pid $($regionProc.Id))"
Write-Host "Logs: $logRoot"
Write-Host "Press Enter to stop the demo."

try {
    [void](Read-Host)
}
finally {
    foreach ($proc in @($regionProc, $brainProc, $hiveProc)) {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force
        }
    }
}
