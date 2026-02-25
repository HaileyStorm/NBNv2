param(
    [string]$DemoRoot = (Join-Path $PSScriptRoot "local-demo"),
    [string]$BindHost = "127.0.0.1",
    [int]$HiveMindPort = 12020,
    [int]$BrainHostPort = 12011,
    [int]$RegionHostPort = 12040,
    [int]$WorkerPort = 11940,
    [int]$WorkerCount = 3,
    [int]$IoPort = 12050,
    [int]$ReproPort = 12070,
    [int]$ObsPort = 12060,
    [int]$SettingsPort = 12010,
    [string]$PidFile = "",
    [switch]$RunEnergyPlasticityScenario = $true,
    [switch]$RunReproScenario = $true,
    [switch]$RunReproSuite = $true,
    [long]$ScenarioCredit = 500,
    [long]$ScenarioRate = 3,
    [double]$ScenarioPlasticityRate = 0.05,
    [switch]$ScenarioAbsolutePlasticity,
    [UInt64]$ReproSeed = 12345,
    [ValidateSet("default", "never", "always")]
    [string]$ReproSpawnPolicy = "never",
    [ValidateSet("base", "live")]
    [string]$ReproStrengthSource = "base"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$runRoot = Join-Path $DemoRoot (Get-Date -Format "yyyyMMdd_HHmmss")
$artifactRoot = Join-Path $runRoot "artifacts"
$logRoot = Join-Path $runRoot "logs"
$settingsDbPath = Join-Path $DemoRoot "settingsmonitor.db"
$settingsDbWal = "$settingsDbPath-wal"
$settingsDbShm = "$settingsDbPath-shm"

if (-not $PSBoundParameters.ContainsKey("WorkerPort") -and $PSBoundParameters.ContainsKey("RegionHostPort")) {
    # Keep Workbench launch compatibility while moving to worker-node-first topology.
    $WorkerPort = [Math]::Max(1, $RegionHostPort - 100)
}

if ($WorkerCount -lt 1) {
    throw "WorkerCount must be at least 1."
}

foreach ($path in @($settingsDbPath, $settingsDbWal, $settingsDbShm)) {
    if (Test-Path $path) {
        Remove-Item -Path $path -Force -ErrorAction SilentlyContinue
    }
}

New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null

$hiveAddress = "${BindHost}:${HiveMindPort}"
$ioAddress = "${BindHost}:${IoPort}"
$reproAddress = "${BindHost}:${ReproPort}"
$obsAddress = "${BindHost}:${ObsPort}"
$settingsAddress = "${BindHost}:${SettingsPort}"

Write-Host "Demo root: $runRoot"
Write-Host "Worker-node-first topology enabled."
if ($PSBoundParameters.ContainsKey("BrainHostPort") -or $PSBoundParameters.ContainsKey("RegionHostPort")) {
    Write-Host "Legacy BrainHost/RegionHost parameters are ignored for worker placement startup."
}

Get-CimInstance Win32_Process -Filter "Name='dotnet.exe'" |
    Where-Object { $_.CommandLine -match 'Nbn.Runtime.HiveMind|Nbn.Runtime.WorkerNode|Nbn.Runtime.IO|Nbn.Runtime.Reproduction|Nbn.Runtime.Observability|Nbn.Runtime.SettingsMonitor|Nbn.Tools.DemoHost|Nbn.Runtime.RegionHost' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.SettingsMonitor\Nbn.Runtime.SettingsMonitor.csproj") -c Release --disable-build-servers | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.WorkerNode\Nbn.Runtime.WorkerNode.csproj") -c Release --disable-build-servers | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.HiveMind\Nbn.Runtime.HiveMind.csproj") -c Release --disable-build-servers | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.IO\Nbn.Runtime.IO.csproj") -c Release --disable-build-servers | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.Reproduction\Nbn.Runtime.Reproduction.csproj") -c Release --disable-build-servers | Out-Null
& dotnet build (Join-Path $repoRoot "src\Nbn.Runtime.Observability\Nbn.Runtime.Observability.csproj") -c Release --disable-build-servers | Out-Null
& dotnet build (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost\Nbn.Tools.DemoHost.csproj") -c Release --disable-build-servers | Out-Null

function Get-ExePath([string]$projectFolder, [string]$exeName) {
    return Join-Path $repoRoot (Join-Path $projectFolder ("bin\Release\net8.0\" + $exeName + ".exe"))
}

function Start-ManagedProcess(
    [string]$exePath,
    [string[]]$serviceArgs,
    [string[]]$dotnetArgs,
    [string]$stdoutPath,
    [string]$stderrPath
) {
    if (Test-Path $exePath) {
        return Start-Process -FilePath $exePath -ArgumentList $serviceArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    }

    return Start-Process -FilePath "dotnet" -ArgumentList $dotnetArgs -WorkingDirectory $repoRoot -NoNewWindow -PassThru -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
}

$demoExe = Get-ExePath "tools\Nbn.Tools.DemoHost" "Nbn.Tools.DemoHost"
$settingsExe = Get-ExePath "src\Nbn.Runtime.SettingsMonitor" "Nbn.Runtime.SettingsMonitor"
$workerExe = Get-ExePath "src\Nbn.Runtime.WorkerNode" "Nbn.Runtime.WorkerNode"
$hiveExe = Get-ExePath "src\Nbn.Runtime.HiveMind" "Nbn.Runtime.HiveMind"
$ioExe = Get-ExePath "src\Nbn.Runtime.IO" "Nbn.Runtime.IO"
$reproExe = Get-ExePath "src\Nbn.Runtime.Reproduction" "Nbn.Runtime.Reproduction"
$obsExe = Get-ExePath "src\Nbn.Runtime.Observability" "Nbn.Runtime.Observability"

$artifactJson = if (Test-Path $demoExe) {
    & $demoExe init-artifacts --artifact-root "$artifactRoot" --json
} else {
    & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- init-artifacts --artifact-root "$artifactRoot" --json
}
$artifactLine = $artifactJson | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
if (-not $artifactLine) {
    throw "DemoHost did not return artifact JSON output."
}

$artifact = $artifactLine | ConvertFrom-Json

$settingsLog = Join-Path $logRoot "settingsmonitor.log"
$settingsErr = Join-Path $logRoot "settingsmonitor.err.log"
$hiveLog = Join-Path $logRoot "hivemind.log"
$hiveErr = Join-Path $logRoot "hivemind.err.log"
$ioLog = Join-Path $logRoot "io.log"
$ioErr = Join-Path $logRoot "io.err.log"
$reproLog = Join-Path $logRoot "reproduction.log"
$reproErr = Join-Path $logRoot "reproduction.err.log"
$obsLog = Join-Path $logRoot "observability.log"
$obsErr = Join-Path $logRoot "observability.err.log"
$spawnLog = Join-Path $logRoot "spawn.log"
$scenarioLog = Join-Path $logRoot "energy-plasticity-scenario.log"
$reproScenarioLog = Join-Path $logRoot "repro-scenario.log"
$reproSuiteLog = Join-Path $logRoot "repro-suite.log"

$settingsDotnetArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.SettingsMonitor"),
    "-c", "Release",
    "--no-build",
    "--",
    "--db", $settingsDbPath,
    "--bind-host", $BindHost,
    "--port", $SettingsPort
)
$settingsServiceArgs = @(
    "--db", $settingsDbPath,
    "--bind-host", $BindHost,
    "--port", $SettingsPort
)

$workerEntries = @()
for ($index = 0; $index -lt $WorkerCount; $index++) {
    $workerNumber = $index + 1
    $port = $WorkerPort + $index
    $logicalName = "nbn.worker.$workerNumber"
    $rootName = "worker-node-$workerNumber"
    $stdoutPath = Join-Path $logRoot ("worker-$workerNumber.log")
    $stderrPath = Join-Path $logRoot ("worker-$workerNumber.err.log")

    $serviceArgs = @(
        "--bind-host", $BindHost,
        "--port", $port,
        "--logical-name", $logicalName,
        "--root-name", $rootName,
        "--settings-host", $BindHost,
        "--settings-port", $SettingsPort,
        "--settings-name", "SettingsMonitor",
        "--service-roles", "all",
        "--cpu-pct", 100,
        "--ram-pct", 100,
        "--storage-pct", 100,
        "--gpu-pct", 100
    )

    $dotnetArgs = @(
        "run",
        "--project", (Join-Path $repoRoot "src\Nbn.Runtime.WorkerNode"),
        "-c", "Release",
        "--no-build",
        "--"
    ) + $serviceArgs

    $workerEntries += [pscustomobject]@{
        Number = $workerNumber
        Port = $port
        LogicalName = $logicalName
        RootName = $rootName
        StdOut = $stdoutPath
        StdErr = $stderrPath
        ServiceArgs = $serviceArgs
        DotnetArgs = $dotnetArgs
        Process = $null
    }
}

$hiveDotnetArgs = @(
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

$ioDotnetArgs = @(
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
    "--hivemind-name", "HiveMind",
    "--repro-address", $reproAddress,
    "--repro-name", "ReproductionManager"
)
$ioServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", $IoPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--hivemind-address", $hiveAddress,
    "--hivemind-name", "HiveMind",
    "--repro-address", $reproAddress,
    "--repro-name", "ReproductionManager"
)

$reproDotnetArgs = @(
    "run",
    "--project", (Join-Path $repoRoot "src\Nbn.Runtime.Reproduction"),
    "-c", "Release",
    "--no-build",
    "--",
    "--bind-host", $BindHost,
    "--port", $ReproPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--io-address", $ioAddress,
    "--io-name", "io-gateway"
)
$reproServiceArgs = @(
    "--bind-host", $BindHost,
    "--port", $ReproPort,
    "--settings-host", $BindHost,
    "--settings-port", $SettingsPort,
    "--settings-name", "SettingsMonitor",
    "--io-address", $ioAddress,
    "--io-name", "io-gateway"
)

$obsDotnetArgs = @(
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

$previousArtifactRoot = $env:NBN_ARTIFACT_ROOT
$env:NBN_ARTIFACT_ROOT = $artifactRoot

$settingsProc = $null
$hiveProc = $null
$ioProc = $null
$reproProc = $null
$obsProc = $null
$brainId = ""

try {
    $settingsProc = Start-ManagedProcess -exePath $settingsExe -serviceArgs $settingsServiceArgs -dotnetArgs $settingsDotnetArgs -stdoutPath $settingsLog -stderrPath $settingsErr
    Start-Sleep -Seconds 1

    foreach ($worker in $workerEntries) {
        $worker.Process = Start-ManagedProcess -exePath $workerExe -serviceArgs $worker.ServiceArgs -dotnetArgs $worker.DotnetArgs -stdoutPath $worker.StdOut -stderrPath $worker.StdErr
        Start-Sleep -Milliseconds 400
    }

    $hiveProc = Start-ManagedProcess -exePath $hiveExe -serviceArgs $hiveServiceArgs -dotnetArgs $hiveDotnetArgs -stdoutPath $hiveLog -stderrPath $hiveErr
    Start-Sleep -Seconds 1

    $ioProc = Start-ManagedProcess -exePath $ioExe -serviceArgs $ioServiceArgs -dotnetArgs $ioDotnetArgs -stdoutPath $ioLog -stderrPath $ioErr
    Start-Sleep -Seconds 1

    $reproProc = Start-ManagedProcess -exePath $reproExe -serviceArgs $reproServiceArgs -dotnetArgs $reproDotnetArgs -stdoutPath $reproLog -stderrPath $reproErr
    Start-Sleep -Seconds 1

    $obsProc = Start-ManagedProcess -exePath $obsExe -serviceArgs $obsServiceArgs -dotnetArgs $obsDotnetArgs -stdoutPath $obsLog -stderrPath $obsErr

    Write-Host "SettingsMonitor: $settingsAddress (pid $($settingsProc.Id))"
    foreach ($worker in $workerEntries) {
        Write-Host ("WorkerNode {0}: {1}:{2} (pid {3}, root {4})" -f $worker.Number, $BindHost, $worker.Port, $worker.Process.Id, $worker.RootName)
    }
    Write-Host "HiveMind: $hiveAddress (pid $($hiveProc.Id))"
    Write-Host "IO Gateway: $ioAddress (pid $($ioProc.Id))"
    Write-Host "Reproduction: $reproAddress (pid $($reproProc.Id))"
    Write-Host "Observability: $obsAddress (pid $($obsProc.Id))"
    Write-Host "Settings DB: $settingsDbPath"
    Write-Host "Logs: $logRoot"

    if ($PidFile) {
        $processes = @(
            @{ pid = $settingsProc.Id; name = $settingsProc.ProcessName; startTicksUtc = $settingsProc.StartTime.ToUniversalTime().Ticks },
            @{ pid = $hiveProc.Id; name = $hiveProc.ProcessName; startTicksUtc = $hiveProc.StartTime.ToUniversalTime().Ticks },
            @{ pid = $ioProc.Id; name = $ioProc.ProcessName; startTicksUtc = $ioProc.StartTime.ToUniversalTime().Ticks },
            @{ pid = $reproProc.Id; name = $reproProc.ProcessName; startTicksUtc = $reproProc.StartTime.ToUniversalTime().Ticks },
            @{ pid = $obsProc.Id; name = $obsProc.ProcessName; startTicksUtc = $obsProc.StartTime.ToUniversalTime().Ticks }
        )

        foreach ($worker in $workerEntries) {
            $processes += @{
                pid = $worker.Process.Id
                name = $worker.Process.ProcessName
                startTicksUtc = $worker.Process.StartTime.ToUniversalTime().Ticks
            }
        }

        $payload = @{ processes = $processes }
        $payload | ConvertTo-Json | Set-Content -Path $PidFile
    }

    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $settingsReady = (Test-Path $settingsLog) -and ((Get-Item $settingsLog).Length -gt 0)
        $workersReady = $true
        foreach ($worker in $workerEntries) {
            $workerReady = (Test-Path $worker.StdOut) -and ((Get-Item $worker.StdOut).Length -gt 0)
            if (-not $workerReady) {
                $workersReady = $false
                break
            }
        }
        $hiveReady = (Test-Path $hiveLog) -and ((Get-Item $hiveLog).Length -gt 0)
        $ioReady = (Test-Path $ioLog) -and ((Get-Item $ioLog).Length -gt 0)
        $reproReady = (Test-Path $reproLog) -and ((Get-Item $reproLog).Length -gt 0)
        $obsReady = (Test-Path $obsLog) -and ((Get-Item $obsLog).Length -gt 0)

        if ($settingsReady -and $workersReady -and $hiveReady -and $ioReady -and $reproReady -and $obsReady) {
            break
        }

        Start-Sleep -Milliseconds 250
    }

    Start-Sleep -Seconds 2

    $spawnArgs = @(
        "spawn-brain",
        "--io-address", $ioAddress,
        "--io-id", "io-gateway",
        "--port", ($ReproPort + 4),
        "--nbn-sha256", $artifact.nbn_sha256,
        "--nbn-size", $artifact.nbn_size,
        "--store-uri", $artifactRoot,
        "--timeout-seconds", 70,
        "--wait-seconds", 30,
        "--json"
    )

    $spawnOutput = if (Test-Path $demoExe) {
        & $demoExe @spawnArgs
    } else {
        & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- @spawnArgs
    }
    $spawnOutput | Set-Content -Path $spawnLog -Encoding UTF8
    $spawnJson = $spawnOutput | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
    if (-not $spawnJson) {
        throw "Spawn command did not emit JSON output. See $spawnLog."
    }

    $spawnPayload = $spawnJson | ConvertFrom-Json
    $brainId = [string]$spawnPayload.spawn_ack.brain_id
    if ([string]::IsNullOrWhiteSpace($brainId)) {
        $reason = [string]$spawnPayload.failure_reason_code
        $message = [string]$spawnPayload.failure_message
        throw "Spawn failed: $reason $message"
    }

    if ($spawnPayload.registration_status -ne "registered") {
        Write-Warning "Spawned brain $brainId but registration status is '$($spawnPayload.registration_status)'. Continuing; see $spawnLog if follow-on scenarios report brain_not_found."
    }

    Write-Host "Spawned brain: $brainId"
    Write-Host "Spawn JSON: $spawnJson"

    if ($RunEnergyPlasticityScenario) {
        $probabilistic = if ($ScenarioAbsolutePlasticity) { "false" } else { "true" }
        $scenarioArgs = @(
            "io-scenario",
            "--io-address", $ioAddress,
            "--io-id", "io-gateway",
            "--port", ($ReproPort + 1),
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

    if ($RunReproScenario) {
        $reproScenarioArgs = @(
            "repro-scenario",
            "--io-address", $ioAddress,
            "--io-id", "io-gateway",
            "--port", ($ReproPort + 2),
            "--parent-a-sha256", $artifact.nbn_sha256,
            "--parent-a-size", $artifact.nbn_size,
            "--parent-b-sha256", $artifact.nbn_sha256,
            "--parent-b-size", $artifact.nbn_size,
            "--store-uri", $artifactRoot,
            "--seed", $ReproSeed,
            "--spawn-policy", $ReproSpawnPolicy,
            "--strength-source", $ReproStrengthSource,
            "--json"
        )

        $reproOutput = if (Test-Path $demoExe) {
            & $demoExe @reproScenarioArgs
        } else {
            & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- @reproScenarioArgs
        }

        $reproOutput | Set-Content -Path $reproScenarioLog -Encoding UTF8
        $reproJson = $reproOutput | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
        if ($reproJson) {
            Write-Host "Repro scenario completed."
            Write-Host "Repro JSON: $reproJson"
        } else {
            Write-Warning "Repro scenario did not emit JSON. See $reproScenarioLog."
        }
    }

    if ($RunReproSuite) {
        $reproSuiteArgs = @(
            "repro-suite",
            "--io-address", $ioAddress,
            "--io-id", "io-gateway",
            "--port", ($ReproPort + 3),
            "--parent-a-sha256", $artifact.nbn_sha256,
            "--parent-a-size", $artifact.nbn_size,
            "--store-uri", $artifactRoot,
            "--seed", $ReproSeed,
            "--json"
        )

        $reproSuiteOutput = if (Test-Path $demoExe) {
            & $demoExe @reproSuiteArgs
        } else {
            & dotnet run --project (Join-Path $repoRoot "tools\Nbn.Tools.DemoHost") -c Release --no-build -- @reproSuiteArgs
        }

        $reproSuiteOutput | Set-Content -Path $reproSuiteLog -Encoding UTF8
        $reproSuiteJson = $reproSuiteOutput | Where-Object { $_ -match '^{.*}$' } | Select-Object -Last 1
        if ($reproSuiteJson) {
            $suitePayload = $reproSuiteJson | ConvertFrom-Json
            Write-Host "Repro suite completed."
            Write-Host ("Repro suite summary: {0}/{1} passed (all_passed={2})" -f $suitePayload.passed_cases, $suitePayload.total_cases, $suitePayload.all_passed)
            Write-Host "Repro suite JSON: $reproSuiteJson"
        } else {
            Write-Warning "Repro suite did not emit JSON. See $reproSuiteLog."
        }
    }

    Write-Host "Press Enter to stop the demo."
    [void](Read-Host)
}
finally {
    foreach ($proc in @($obsProc, $reproProc, $ioProc, $hiveProc, $settingsProc)) {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force
        }
    }

    foreach ($worker in $workerEntries) {
        if ($worker.Process -and -not $worker.Process.HasExited) {
            Stop-Process -Id $worker.Process.Id -Force
        }
    }

    if ($null -eq $previousArtifactRoot) {
        Remove-Item Env:\NBN_ARTIFACT_ROOT -ErrorAction SilentlyContinue
    } else {
        $env:NBN_ARTIFACT_ROOT = $previousArtifactRoot
    }

    if ($PidFile -and (Test-Path $PidFile)) {
        Remove-Item -Path $PidFile -Force
    }
}
