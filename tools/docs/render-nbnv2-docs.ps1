param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$renderer = Join-Path $scriptDir "render_nbnv2_docs.py"

if (-not (Test-Path -LiteralPath $renderer)) {
    throw "Renderer not found: $renderer"
}

$cmd = $null
$args = @()

$python = Get-Command python -ErrorAction SilentlyContinue
if ($python) {
    $cmd = $python.Source
    $args = @($renderer)
}
else {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if (-not $py) {
        throw "Python runtime not found. Install python or py launcher."
    }

    $cmd = $py.Source
    $args = @("-3", $renderer)
}

if ($Check) {
    $args += "--check"
}

& $cmd @args
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
