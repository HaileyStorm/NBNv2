param(
    [string[]]$RuntimeIdentifiers = @("win-x64", "linux-x64"),
    [string]$Configuration = "Release",
    [string]$OutputRoot = "artifacts/dist/worker-node",
    [switch]$FrameworkDependent
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\\..")
$projectPath = Join-Path $repoRoot "src\\Nbn.Runtime.WorkerNode\\Nbn.Runtime.WorkerNode.csproj"
$publishSelfContained = if ($FrameworkDependent) { "false" } else { "true" }

if (-not (Test-Path $projectPath))
{
    throw "WorkerNode project not found: $projectPath"
}

Write-Host "Publishing Nbn.Runtime.WorkerNode"
Write-Host "  configuration: $Configuration"
Write-Host "  self-contained: $publishSelfContained"
Write-Host "  rids: $($RuntimeIdentifiers -join ', ')"

foreach ($rid in $RuntimeIdentifiers)
{
    if ([string]::IsNullOrWhiteSpace($rid))
    {
        continue
    }

    $outputDir = Join-Path $repoRoot (Join-Path $OutputRoot $rid.Trim())
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

    Write-Host "Publishing RID $rid to $outputDir ..."
    dotnet publish $projectPath `
        -c $Configuration `
        --disable-build-servers `
        -r $rid `
        --self-contained $publishSelfContained `
        /p:PublishSingleFile=true `
        /p:PublishTrimmed=false `
        /p:IncludeNativeLibrariesForSelfExtract=true `
        /p:DebugType=None `
        /p:DebugSymbols=false `
        -o $outputDir

    if ($LASTEXITCODE -ne 0)
    {
        throw "dotnet publish failed for RID '$rid' with exit code $LASTEXITCODE."
    }
}

Write-Host "WorkerNode publish complete."
Write-Host "Artifacts root: $(Join-Path $repoRoot $OutputRoot)"
