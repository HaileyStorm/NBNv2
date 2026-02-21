param()

$ErrorActionPreference = 'Stop'

function Test-CodexDirect {
    try {
        & python -c "import subprocess,sys; p=subprocess.run(['codex','--version']); sys.exit(p.returncode)" | Out-Null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Get-CodexWhich {
    try {
        return (& python -c "import shutil; print(shutil.which('codex') or '')").Trim()
    }
    catch {
        return ""
    }
}

function Write-FileContent {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Content
    )

    $directory = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($directory) -and -not (Test-Path $directory)) {
        New-Item -ItemType Directory -Path $directory | Out-Null
    }

    Set-Content -Path $Path -Value $Content -Encoding UTF8
}

$codexCmd = Get-Command codex.cmd -ErrorAction SilentlyContinue
if ($null -eq $codexCmd) {
    throw "codex.cmd was not found on PATH. Install Codex CLI first."
}

$targetDir = Split-Path -Parent $codexCmd.Path
$targetExe = Join-Path $targetDir "codex.exe"

if (Test-CodexDirect) {
    Write-Output "codex is already directly invokable. No shim install needed."
    Write-Output "Resolved (python which): $(Get-CodexWhich)"
    exit 0
}

$buildRoot = Join-Path $env:TEMP "nbnv2-codex-shim-build"
if (Test-Path $buildRoot) {
    Remove-Item -Path $buildRoot -Recurse -Force
}

New-Item -ItemType Directory -Path $buildRoot | Out-Null

$projectPath = Join-Path $buildRoot "CodexShim.csproj"
$programPath = Join-Path $buildRoot "Program.cs"

$project = @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <AssemblyName>codex</AssemblyName>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
</Project>
"@

$program = @"
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

static class Program
{
    public static int Main(string[] args)
    {
        var pathValue = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
        var codexCmd = pathValue
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(dir => Path.Combine(dir, "codex.cmd"))
            .FirstOrDefault(File.Exists);

        if (string.IsNullOrWhiteSpace(codexCmd))
        {
            Console.Error.WriteLine("codex shim error: unable to locate codex.cmd on PATH.");
            return 127;
        }

        try
        {
            var start = new ProcessStartInfo
            {
                FileName = codexCmd,
                UseShellExecute = false,
                WorkingDirectory = Environment.CurrentDirectory
            };

            foreach (var arg in args)
            {
                start.ArgumentList.Add(arg);
            }

            using var process = Process.Start(start);
            if (process is null)
            {
                Console.Error.WriteLine("codex shim error: failed to launch codex.cmd.");
                return 127;
            }

            process.WaitForExit();
            return process.ExitCode;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"codex shim error: {ex.Message}");
            return 127;
        }
    }
}
"@

Write-FileContent -Path $projectPath -Content $project
Write-FileContent -Path $programPath -Content $program

dotnet publish $projectPath -c Release --disable-build-servers -r win-x64 --self-contained false /p:PublishSingleFile=true /p:PublishTrimmed=false | Out-Null

$publishedExe = Join-Path $buildRoot "bin\Release\net8.0\win-x64\publish\codex.exe"
if (-not (Test-Path $publishedExe)) {
    throw "Shim publish did not produce codex.exe at expected path: $publishedExe"
}

Copy-Item -Path $publishedExe -Destination $targetExe -Force

if (-not (Test-CodexDirect)) {
    throw "Installed codex.exe, but direct invocation still failed."
}

Write-Output "Installed codex.exe shim at: $targetExe"
Write-Output "Resolved (python which): $(Get-CodexWhich)"
