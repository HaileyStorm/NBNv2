param(
    [switch]$Confirm,
    [string]$PushRemote = "origin"
)

$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptRoot "..\..")
Set-Location $repoRoot

$status = git status --porcelain
if (-not [string]::IsNullOrWhiteSpace($status))
{
    throw "Release requires a clean git worktree."
}

git fetch --tags --force | Out-Null
$version = (python tools/dist/build-release.py print-version | Select-Object -Last 1).Trim()
$tag = "v$version"

Write-Host "Planned release tag: $tag"
powershell -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check
dotnet build NBNv2.sln -c Release --disable-build-servers
dotnet test NBNv2.sln -c Release --disable-build-servers

if (-not $Confirm)
{
    Write-Host "Validation succeeded. Re-run with -Confirm to create and push $tag."
    exit 0
}

git rev-parse -q --verify "refs/tags/$tag" | Out-Null
if ($LASTEXITCODE -eq 0)
{
    throw "Tag already exists: $tag"
}

git tag -a $tag -m "NBN $version"
git push $PushRemote $tag
Write-Host "Pushed release tag $tag to $PushRemote."
