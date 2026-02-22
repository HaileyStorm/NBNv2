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
    [string]$RouterId = "demo-router",
    [string]$PidFile = "",
    [switch]$RunEnergyPlasticityScenario = $true,
    [long]$ScenarioCredit = 500,
    [long]$ScenarioRate = 3,
    [double]$ScenarioPlasticityRate = 0.05,
    [switch]$ScenarioAbsolutePlasticity
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$runRoot = Join-Path $DemoRoot (Get-Date -Format "yyyyMMdd_HHmmss")
$artifactRoot = Join-Path $runRoot "artifacts"
$logRoot = Join-Path $runRoot "logs"
$settingsDbPath = Join-Path $DemoRoot "settingsmonitor.db"
$settingsDbWal = "$settingsDbPath-wal"
$settingsDbShm = "$settingsDbPath-shm"

foreach ($path in @($settingsDbPath, $settingsDbWal, $settingsDbShm)) {
    if (Test-Path $path) {
        Remove-Item -Path $path -Force -ErrorAction SilentlyContinue
    }
}

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

function Get-ExePath([string]$projectFolder, [string]$exeName) {
    return Join-Path $repoRoot (Join-Path $projectFolder ("bin\Release\net8.0\" + $exeName + ".exe"))
}

$demoExe = Get-ExePath "tools\Nbn.Tools.DemoHost" "Nbn.Tools.DemoHost"
$hiveExe = Get-ExePath "src\Nbn.Runtime.HiveMind" "Nbn.Runtime.HiveMind"
$regionExe = Get-ExePath "src\Nbn.Runtime.RegionHost" "Nbn.Runtime.RegionHost"
$ioExe = Get-ExePath "src\Nbn.Runtime.IO" "Nbn.Runtime.IO"
$obsExe = Get-ExePath "src\Nbn.Runtime.Observability" "Nbn.Runtime.Observability"
$settingsExe = Get-ExePath "src\Nbn.Runtime.SettingsMonitor" "Nbn.Runtime.SettingsMonitor"

$artifactJson = if (Test-Path $demoExe) {
    & $demoExe init-artifacts --artifact-root "$artifactRoot" --json
} else {
    & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- init-artifacts --artifact-root "$artifactRoot" --json
}
$artifactLine = $artifactJson | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
if (-not $artifactLine) {
    throw "DemoHost did not return JSON output."
}

$artifact = $artifactLine | ConvertFrom-Json

$hiveLog = Join-Path $logRoot "hivemind.log"
$brainLog = Join-Path $logRoot "brainhost.log"
$regionLog = Join-Path $logRoot "regionhost.log"
$regionInputLog = Join-Path $logRoot "regionhost-input.log"
$regionOutputLog = Join-Path $logRoot "regionhost-output.log"
$ioLog = Join-Path $logRoot "io.log"
$obsLog = Join-Path $logRoot "observability.log"
$settingsLog = Join-Path $logRoot "settingsmonitor.log"
$hiveErr = Join-Path $logRoot "hivemind.err.log"
$brainErr = Join-Path $logRoot "brainhost.err.log"
$regionErr = Join-Path $logRoot "regionhost.err.log"
$regionInputErr = Join-Path $logRoot "regionhost-input.err.log"
$regionOutputErr = Join-Path $logRoot "regionhost-output.err.log"
$ioErr = Join-Path $logRoot "io.err.log"
$obsErr = Join-Path $logRoot "observability.err.log"
$settingsErr = Join-Path $logRoot "settingsmonitor.err.log"
$scenarioLog = Join-Path $logRoot "energy-plasticity-scenario.log"

$hiveArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $HiveMindPort,
    "--settings-db", $settingsDbPath,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor"
)

$hiveServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", $HiveMindPort,
    "--settings-db", $settingsDbPath,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor"
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
    "--router-id", $RouterId,
    "--io-address", $ioAddress,
    "--io-id", "io-gateway",
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor"
)

$brainServiceArgs = @(
    "run-brain",
    "--bind-host", $BindHost,
    "--port", $BrainHostPort,
    "--brain-id", $brainId,
    "--hivemind-address", $hiveAddress,
    "--hivemind-id", "HiveMind",
    "--router-id", $RouterId,
    "--io-address", $ioAddress,
    "--io-id", "io-gateway",
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor"
)

$ioOutputId = "io-output-" + ($brainId -replace '-', '')

$regionArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.RegionHost"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $RegionHostPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
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

$regionServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", $RegionHostPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
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

$regionInputArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.RegionHost"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", ($RegionHostPort + 1),
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--brain-id", $brainId,
    "--region", 0,
    "--neuron-start", 0,
    "--neuron-count", 1,
    "--shard-index", 0,
    "--router-address", $brainAddress,
    "--router-id", $RouterId,
    "--tick-address", $hiveAddress,
    "--tick-id", "HiveMind",
    "--nbn-sha256", $artifact.nbn_sha256,
    "--nbn-size", $artifact.nbn_size,
    "--artifact-root", $artifactRoot
)

$regionInputServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", ($RegionHostPort + 1),
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--brain-id", $brainId,
    "--region", 0,
    "--neuron-start", 0,
    "--neuron-count", 1,
    "--shard-index", 0,
    "--router-address", $brainAddress,
    "--router-id", $RouterId,
    "--tick-address", $hiveAddress,
    "--tick-id", "HiveMind",
    "--nbn-sha256", $artifact.nbn_sha256,
    "--nbn-size", $artifact.nbn_size,
    "--artifact-root", $artifactRoot
)

$regionOutputArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.RegionHost"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", ($RegionHostPort + 2),
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--brain-id", $brainId,
    "--region", 31,
    "--neuron-start", 0,
    "--neuron-count", 1,
    "--shard-index", 0,
    "--router-address", $brainAddress,
    "--router-id", $RouterId,
    "--tick-address", $hiveAddress,
    "--tick-id", "HiveMind",
    "--output-address", $ioAddress,
    "--output-id", $ioOutputId,
    "--nbn-sha256", $artifact.nbn_sha256,
    "--nbn-size", $artifact.nbn_size,
    "--artifact-root", $artifactRoot
)

$regionOutputServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", ($RegionHostPort + 2),
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--brain-id", $brainId,
    "--region", 31,
    "--neuron-start", 0,
    "--neuron-count", 1,
    "--shard-index", 0,
    "--router-address", $brainAddress,
    "--router-id", $RouterId,
    "--tick-address", $hiveAddress,
    "--tick-id", "HiveMind",
    "--output-address", $ioAddress,
    "--output-id", $ioOutputId,
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
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--hivemind-address", $hiveAddress,
    "--hivemind-name", "HiveMind"
)

$ioServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", $IoPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
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
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--enable-debug",
    "--enable-viz"
)

$obsServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", $ObsPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
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

$settingsProc = if (Test-Path $settingsExe) {
    Start-Process -FilePath $settingsExe -ArgumentList @("--db", $settingsDbPath, "--bind-host", $BindHost, "--port", $SettingsPort) -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $settingsLog -RedirectStandardError $settingsErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $settingsArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $settingsLog -RedirectStandardError $settingsErr
}

$hiveProc = if (Test-Path $hiveExe) {
    Start-Process -FilePath $hiveExe -ArgumentList $hiveServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $hiveLog -RedirectStandardError $hiveErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $hiveArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $hiveLog -RedirectStandardError $hiveErr
}
Start-Sleep -Seconds 1
$brainProc = if (Test-Path $demoExe) {
    Start-Process -FilePath $demoExe -ArgumentList $brainServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $brainLog -RedirectStandardError $brainErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $brainArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $brainLog -RedirectStandardError $brainErr
}
Start-Sleep -Seconds 1
$regionProc = if (Test-Path $regionExe) {
    Start-Process -FilePath $regionExe -ArgumentList $regionServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionLog -RedirectStandardError $regionErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $regionArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionLog -RedirectStandardError $regionErr
}
Start-Sleep -Seconds 1
$regionInputProc = if (Test-Path $regionExe) {
    Start-Process -FilePath $regionExe -ArgumentList $regionInputServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionInputLog -RedirectStandardError $regionInputErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $regionInputArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionInputLog -RedirectStandardError $regionInputErr
}
Start-Sleep -Seconds 1
$regionOutputProc = if (Test-Path $regionExe) {
    Start-Process -FilePath $regionExe -ArgumentList $regionOutputServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionOutputLog -RedirectStandardError $regionOutputErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $regionOutputArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $regionOutputLog -RedirectStandardError $regionOutputErr
}
Start-Sleep -Seconds 1
$ioProc = if (Test-Path $ioExe) {
    Start-Process -FilePath $ioExe -ArgumentList $ioServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $ioLog -RedirectStandardError $ioErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $ioArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $ioLog -RedirectStandardError $ioErr
}
Start-Sleep -Seconds 1
$obsProc = if (Test-Path $obsExe) {
    Start-Process -FilePath $obsExe -ArgumentList $obsServiceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $obsLog -RedirectStandardError $obsErr
} else {
    Start-Process -FilePath "dotnet" -ArgumentList $obsArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $obsLog -RedirectStandardError $obsErr
}

Write-Host "HiveMind: $hiveAddress (pid $($hiveProc.Id))"
Write-Host "BrainHost: $brainAddress (pid $($brainProc.Id))"
Write-Host "RegionHost: $regionAddress (pid $($regionProc.Id))"
Write-Host "RegionHost Input: ${BindHost}:$($RegionHostPort + 1) (pid $($regionInputProc.Id))"
Write-Host "RegionHost Output: ${BindHost}:$($RegionHostPort + 2) (pid $($regionOutputProc.Id))"
Write-Host "IO Gateway: $ioAddress (pid $($ioProc.Id))"
Write-Host "Observability: $obsAddress (pid $($obsProc.Id))"
Write-Host "SettingsMonitor: $settingsAddress (pid $($settingsProc.Id))"
Write-Host "Settings DB: $settingsDbPath"
Write-Host "Logs: $logRoot"

if ($PidFile) {
    $processes = @(
        @{ pid = $settingsProc.Id; name = $settingsProc.ProcessName; startTicksUtc = $settingsProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $hiveProc.Id; name = $hiveProc.ProcessName; startTicksUtc = $hiveProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $brainProc.Id; name = $brainProc.ProcessName; startTicksUtc = $brainProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $regionProc.Id; name = $regionProc.ProcessName; startTicksUtc = $regionProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $regionInputProc.Id; name = $regionInputProc.ProcessName; startTicksUtc = $regionInputProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $regionOutputProc.Id; name = $regionOutputProc.ProcessName; startTicksUtc = $regionOutputProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $ioProc.Id; name = $ioProc.ProcessName; startTicksUtc = $ioProc.StartTime.ToUniversalTime().Ticks },
        @{ pid = $obsProc.Id; name = $obsProc.ProcessName; startTicksUtc = $obsProc.StartTime.ToUniversalTime().Ticks }
    )

    $payload = @{ processes = $processes }
    $payload | ConvertTo-Json | Set-Content -Path $PidFile
}

$deadline = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $deadline) {
    $hiveReady = (Test-Path $hiveLog) -and ((Get-Item $hiveLog).Length -gt 0)
    $brainReady = (Test-Path $brainLog) -and ((Get-Item $brainLog).Length -gt 0)
    $regionReady = (Test-Path $regionLog) -and ((Get-Item $regionLog).Length -gt 0)
    $regionInputReady = (Test-Path $regionInputLog) -and ((Get-Item $regionInputLog).Length -gt 0)
    $regionOutputReady = (Test-Path $regionOutputLog) -and ((Get-Item $regionOutputLog).Length -gt 0)
    $ioReady = (Test-Path $ioLog) -and ((Get-Item $ioLog).Length -gt 0)
    $obsReady = (Test-Path $obsLog) -and ((Get-Item $obsLog).Length -gt 0)

    if ($hiveReady -and $brainReady -and $regionReady -and $regionInputReady -and $regionOutputReady -and $ioReady -and $obsReady) {
        break
    }

    Start-Sleep -Milliseconds 250
}

if ($RunEnergyPlasticityScenario) {
    $probabilistic = if ($ScenarioAbsolutePlasticity) { "false" } else { "true" }
    $scenarioArgs = @(
        "io-scenario",
        "--io-address", $ioAddress,
        "--io-id", "io-gateway",
        "--brain-id", $brainId,
        "--credit", $ScenarioCredit,
        "--rate", $ScenarioRate,
        "--cost-enabled", "true",
        "--energy-enabled", "true",
        "--plasticity-enabled", "true",
        "--plasticity-rate", $ScenarioPlasticityRate,
        "--probabilistic", $probabilistic,
        "--json"
    )

    $scenarioOutput = if (Test-Path $demoExe) {
        & $demoExe @scenarioArgs
    } else {
        & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- @scenarioArgs
    }

    $scenarioOutput | Set-Content -Path $scenarioLog -Encoding UTF8
    $scenarioJson = $scenarioOutput | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
    if ($scenarioJson) {
        Write-Host "Energy/plasticity scenario completed."
        Write-Host "Scenario JSON: $scenarioJson"
    } else {
        Write-Warning "Energy/plasticity scenario did not emit JSON. See $scenarioLog."
    }
}

Write-Host "Press Enter to stop the demo."

try {
    [void](Read-Host)
}
finally {
    foreach ($proc in @($obsProc, $ioProc, $regionOutputProc, $regionInputProc, $regionProc, $brainProc, $hiveProc, $settingsProc)) {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force
        }
    }

    if ($PidFile -and (Test-Path $PidFile)) {
        Remove-Item -Path $PidFile -Force
    }
}
