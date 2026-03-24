param(
    [string]$SourceDir = "",
    [string]$DestinationDir = "",
    [switch]$Zip
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($SourceDir)) {
    $SourceDir = Join-Path $env:LOCALAPPDATA "Nbn.Workbench\logs"
}

if ([string]::IsNullOrWhiteSpace($DestinationDir)) {
    $DestinationDir = Join-Path $HOME "Downloads\NBNLogs"
}

if (-not (Test-Path -LiteralPath $SourceDir)) {
    throw "Workbench log directory not found: $SourceDir"
}

if (Test-Path -LiteralPath $DestinationDir) {
    Remove-Item -LiteralPath $DestinationDir -Recurse -Force
}

New-Item -ItemType Directory -Path $DestinationDir -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $SourceDir "*") -Destination $DestinationDir -Recurse -Force

if ($Zip) {
    $zipPath = "$DestinationDir.zip"
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }

    Compress-Archive -Path (Join-Path $DestinationDir "*") -DestinationPath $zipPath -Force
    Write-Host "Exported Workbench logs to $DestinationDir and $zipPath"
}
else {
    Write-Host "Exported Workbench logs to $DestinationDir"
}
