param(
    [string]$NodePath = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe",
    [string]$NpmPath = ''
)
$ErrorActionPreference = 'Stop'
if (!(Test-Path -LiteralPath $NodePath)) { throw 'Node.js 24 is required. Supply -NodePath.' }
Push-Location $PSScriptRoot
$taskPreviousPath = $env:PATH
try {
    $env:PATH = (Split-Path -Parent $NodePath) + ';' + $env:PATH
    if (Test-Path -LiteralPath (Join-Path $PSScriptRoot 'node_modules\archiver\package.json')) {
        & $NodePath --input-type=module -e "import archiver from 'archiver'; import { DatabaseSync } from 'node:sqlite'; new DatabaseSync(':memory:').close();"
        if ($LASTEXITCODE -eq 0) { Write-Output 'Excel runtime is available.'; return }
    }
    if (!$NpmPath) {
        $taskNpmCommand = Get-Command npm.cmd -ErrorAction SilentlyContinue
        if ($taskNpmCommand) { $NpmPath = $taskNpmCommand.Source }
    }
    if (!$NpmPath -or !(Test-Path -LiteralPath $NpmPath)) {
        throw 'npm.cmd is required for a fresh install. Install Node.js 24 with npm, or supply -NpmPath.'
    }
    & $NpmPath ci --ignore-scripts --no-audit --no-fund
    if ($LASTEXITCODE -ne 0) { throw 'Excel dependency installation failed.' }
    & $NodePath --input-type=module -e "import archiver from 'archiver'; import { DatabaseSync } from 'node:sqlite'; new DatabaseSync(':memory:').close();"
    if ($LASTEXITCODE -ne 0) { throw 'Excel runtime validation failed.' }
} finally { $env:PATH = $taskPreviousPath; Pop-Location }
