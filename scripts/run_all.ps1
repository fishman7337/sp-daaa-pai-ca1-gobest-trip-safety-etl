param(
    [string] $EnvFile = "config/.env",
    [switch] $SkipCounts,
    [switch] $DryRun
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
$SourcePath = Join-Path $ProjectRoot "src"
if ($env:PYTHONPATH) {
    $env:PYTHONPATH = "$SourcePath;$env:PYTHONPATH"
} else {
    $env:PYTHONPATH = $SourcePath
}

if (Test-Path -LiteralPath $EnvFile) {
    $env:PAI_ENV_FILE = $EnvFile
}

$Arguments = @("-m", "pai_pipeline", "run", "all")
if ($SkipCounts) {
    $Arguments += "--skip-counts"
}
if ($DryRun) {
    $Arguments += "--dry-run"
}

python @Arguments
