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
$runRoot = Join-Path $DemoRoot (Get-Date -Format "yyyyMMdd_HHmmss")
$artifactRoot = Join-Path $runRoot "artifacts"
$logRoot = Join-Path $runRoot "logs"

New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

$brainId = [guid]::NewGuid().ToString()
$hiveAddress = "${BindHost}:${HiveMindPort}"
$brainAddress = "${BindHost}:${BrainHostPort}"
$regionAddress = "${BindHost}:${RegionHostPort}"

Write-Host "Demo root: $runRoot"
Write-Host "BrainId: $brainId"

& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind\Nbn.Runtime.HiveMind.csproj") -c Release | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.RegionHost\Nbn.Runtime.RegionHost.csproj") -c Release | Out-Null
& dotnet build (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost\Nbn.Tools.DemoHost.csproj") -c Release | Out-Null

$artifactJson = & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- init-artifacts --artifact-root "$artifactRoot" --json
$artifactLine = $artifactJson | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
if (-not $artifactLine) {
    throw "DemoHost did not return JSON output."
}

$artifact = $artifactLine | ConvertFrom-Json

$hiveLog = Join-Path $logRoot "hivemind.log"
$brainLog = Join-Path $logRoot "brainhost.log"
$regionLog = Join-Path $logRoot "regionhost.log"
$hiveErr = Join-Path $logRoot "hivemind.err.log"
$brainErr = Join-Path $logRoot "brainhost.err.log"
$regionErr = Join-Path $logRoot "regionhost.err.log"

$hiveArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $HiveMindPort
)

$brainArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost"),
    "-c", "Release",
    "--no-build",
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
    "-c", "Release",
    "--no-build",
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

$hiveProc = Start-Process -FilePath "dotnet" -ArgumentList $hiveArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $hiveLog -RedirectStandardError $hiveErr
Start-Sleep -Seconds 1
$brainProc = Start-Process -FilePath "dotnet" -ArgumentList $brainArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $brainLog -RedirectStandardError $brainErr
Start-Sleep -Seconds 1
$regionProc = Start-Process -FilePath "dotnet" -ArgumentList $regionArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionLog -RedirectStandardError $regionErr

Write-Host "HiveMind: $hiveAddress (pid $($hiveProc.Id))"
Write-Host "BrainHost: $brainAddress (pid $($brainProc.Id))"
Write-Host "RegionHost: $regionAddress (pid $($regionProc.Id))"
Write-Host "Logs: $logRoot"

$deadline = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $deadline) {
    $hiveReady = (Test-Path $hiveLog) -and ((Get-Item $hiveLog).Length -gt 0)
    $brainReady = (Test-Path $brainLog) -and ((Get-Item $brainLog).Length -gt 0)
    $regionReady = (Test-Path $regionLog) -and ((Get-Item $regionLog).Length -gt 0)

    if ($hiveReady -and $brainReady -and $regionReady) {
        break
    }

    Start-Sleep -Milliseconds 250
}

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
