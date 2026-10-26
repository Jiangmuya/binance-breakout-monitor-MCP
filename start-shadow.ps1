param(
    [Parameter(Mandatory=$true)][string]$SourceDirectory,
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [string]$PythonPath = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe",
    [string]$NodePath = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
)
$ErrorActionPreference = 'Stop'
$taskSource = (Resolve-Path -LiteralPath $SourceDirectory).Path
$taskOutput = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $taskOutput | Out-Null
if (Test-Path -LiteralPath (Join-Path $taskOutput 'STOP')) { throw 'Remove the STOP request before an intentional restart; the continuous validation period will restart.' }
$taskArguments = @('-u', ('"' + (Join-Path $PSScriptRoot 'tools\run_shadow.py') + '"'),
    '--source-db', ('"' + (Join-Path $taskSource 'runtime\monitor.sqlite3') + '"'),
    '--source-config', ('"' + (Join-Path $PSScriptRoot 'config.json') + '"'),
    '--output', ('"' + $taskOutput + '"'), '--node', ('"' + $NodePath + '"'), '--hours', '0')
$taskOldEncoding = $env:PYTHONUTF8
try {
    $env:PYTHONUTF8 = '1'
    $taskProcess = Start-Process -FilePath $PythonPath -ArgumentList $taskArguments -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $taskOutput 'stdout.log') -RedirectStandardError (Join-Path $taskOutput 'stderr.log')
    Write-Output ("Shadow validation started: PID {0}, output {1}" -f $taskProcess.Id,$taskOutput)
} finally { $env:PYTHONUTF8 = $taskOldEncoding }
