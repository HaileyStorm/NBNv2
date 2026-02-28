param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$renderer = Join-Path $scriptDir "render_nbnv2_docs.py"

if (-not (Test-Path -LiteralPath $renderer)) {
    throw "Renderer not found: $renderer"
}

function Resolve-PythonCommand {
    $pythonCandidates = @("python", "python3")
    foreach ($candidate in $pythonCandidates) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if (-not $command) {
            continue
        }

        try {
            & $command.Source -c "import sys" *> $null
            if ($LASTEXITCODE -eq 0) {
                return @{
                    Command = $command.Source
                    Args = @()
                }
            }
        }
        catch {
            # Try the next candidate.
        }
    }

    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        try {
            & $py.Source -3 -c "import sys" *> $null
            if ($LASTEXITCODE -eq 0) {
                return @{
                    Command = $py.Source
                    Args = @("-3")
                }
            }
        }
        catch {
            # Fall through to final error.
        }
    }

    throw "Python runtime not found. Install python/python3 or py launcher."
}

$runtime = Resolve-PythonCommand
$cmd = $runtime.Command
$args = @()
$args += $runtime.Args
$args += $renderer

if ($Check) {
    $args += "--check"
}

& $cmd @args
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
