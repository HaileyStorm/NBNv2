param(
    [string]$DemoRoot = (Join-Path $PSScriptRoot "local-demo"),
    [string]$BindHost = "127.0.0.1",
    [int]$HiveMindPort = 12020,
    [int]$BrainHostPort = 12011,
    [int]$RegionHostPort = 12040,
    [int]$IoPort = 12050,
    [int]$ObsPort = 12060,
    [int]$SettingsPort = 12010,
    [int]$RegionId = 1,
    [int]$ShardIndex = 0,
    [string]$RouterId = "demo-router"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$runRoot = Join-Path $DemoRoot (Get-Date -Format "yyyyMMdd_HHmmss")
$artifactRoot = Join-Path $runRoot "artifacts"
$logRoot = Join-Path $runRoot "logs"
$settingsDbPath = Join-Path $DemoRoot "settingsmonitor.db"

New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

$brainId = [guid]::NewGuid().ToString()
$hiveAddress = "${BindHost}:${HiveMindPort}"
$brainAddress = "${BindHost}:${BrainHostPort}"
$regionAddress = "${BindHost}:${RegionHostPort}"
$ioAddress = "${BindHost}:${IoPort}"
$obsAddress = "${BindHost}:${ObsPort}"
$settingsAddress = "${BindHost}:${SettingsPort}"

Write-Host "Demo root: $runRoot"
Write-Host "BrainId: $brainId"

Get-CimInstance Win32_Process -Filter "Name='dotnet.exe'" |
    Where-Object { $_.CommandLine -match 'Nbn.Runtime.HiveMind|Nbn.Runtime.RegionHost|Nbn.Runtime.IO|Nbn.Runtime.Observability|Nbn.Runtime.SettingsMonitor|Nbn.Tools.DemoHost' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind\Nbn.Runtime.HiveMind.csproj") -c Release | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.RegionHost\Nbn.Runtime.RegionHost.csproj") -c Release | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.IO\Nbn.Runtime.IO.csproj") -c Release | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.Observability\Nbn.Runtime.Observability.csproj") -c Release | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.SettingsMonitor\Nbn.Runtime.SettingsMonitor.csproj") -c Release | Out-Null
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
$ioLog = Join-Path $logRoot "io.log"
$obsLog = Join-Path $logRoot "observability.log"
$settingsLog = Join-Path $logRoot "settingsmonitor.log"
$hiveErr = Join-Path $logRoot "hivemind.err.log"
$brainErr = Join-Path $logRoot "brainhost.err.log"
$regionErr = Join-Path $logRoot "regionhost.err.log"
$ioErr = Join-Path $logRoot "io.err.log"
$obsErr = Join-Path $logRoot "observability.err.log"
$settingsErr = Join-Path $logRoot "settingsmonitor.err.log"

$hiveArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $HiveMindPort,
    "--settings-db", $settingsDbPath
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

$ioArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.IO"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $IoPort,
    "--hivemind-address", $hiveAddress,
    "--hivemind-name", "HiveMind"
)

$obsArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.Observability"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $ObsPort,
    "--enable-debug",
    "--enable-viz"
)

$settingsArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.SettingsMonitor"),
    "-c", "Release",
    "--no-build",
    "--",
    "--db", $settingsDbPath,
    "--bind-host", $BindHost,
    "--port", $SettingsPort
)

$settingsProc = Start-Process -FilePath "dotnet" -ArgumentList $settingsArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $settingsLog -RedirectStandardError $settingsErr

$hiveProc = Start-Process -FilePath "dotnet" -ArgumentList $hiveArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $hiveLog -RedirectStandardError $hiveErr
Start-Sleep -Seconds 1
$brainProc = Start-Process -FilePath "dotnet" -ArgumentList $brainArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $brainLog -RedirectStandardError $brainErr
Start-Sleep -Seconds 1
$regionProc = Start-Process -FilePath "dotnet" -ArgumentList $regionArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionLog -RedirectStandardError $regionErr
Start-Sleep -Seconds 1
$ioProc = Start-Process -FilePath "dotnet" -ArgumentList $ioArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $ioLog -RedirectStandardError $ioErr
Start-Sleep -Seconds 1
$obsProc = Start-Process -FilePath "dotnet" -ArgumentList $obsArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $obsLog -RedirectStandardError $obsErr

Write-Host "HiveMind: $hiveAddress (pid $($hiveProc.Id))"
Write-Host "BrainHost: $brainAddress (pid $($brainProc.Id))"
Write-Host "RegionHost: $regionAddress (pid $($regionProc.Id))"
Write-Host "IO Gateway: $ioAddress (pid $($ioProc.Id))"
Write-Host "Observability: $obsAddress (pid $($obsProc.Id))"
Write-Host "SettingsMonitor: $settingsAddress (pid $($settingsProc.Id))"
Write-Host "Settings DB: $settingsDbPath"
Write-Host "Logs: $logRoot"

$deadline = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $deadline) {
    $hiveReady = (Test-Path $hiveLog) -and ((Get-Item $hiveLog).Length -gt 0)
    $brainReady = (Test-Path $brainLog) -and ((Get-Item $brainLog).Length -gt 0)
    $regionReady = (Test-Path $regionLog) -and ((Get-Item $regionLog).Length -gt 0)
    $ioReady = (Test-Path $ioLog) -and ((Get-Item $ioLog).Length -gt 0)
    $obsReady = (Test-Path $obsLog) -and ((Get-Item $obsLog).Length -gt 0)

    if ($hiveReady -and $brainReady -and $regionReady -and $ioReady -and $obsReady) {
        break
    }

    Start-Sleep -Milliseconds 250
}

Write-Host "Press Enter to stop the demo."

try {
    [void](Read-Host)
}
finally {
    foreach ($proc in @($obsProc, $ioProc, $regionProc, $brainProc, $hiveProc, $settingsProc)) {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force
        }
    }
}
