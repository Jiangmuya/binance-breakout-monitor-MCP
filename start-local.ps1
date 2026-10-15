param(
    [ValidateSet('init','bootstrap','run','export','status','stop')][string]$Command = 'run',
    [int]$Seconds = 0,
    [string]$PythonPath = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe",
    [string]$NodePath = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
)
$ErrorActionPreference = 'Stop'
if (!(Test-Path -LiteralPath $PythonPath)) { throw '找不到 Python，请通过 -PythonPath 指定 Python 3.12。' }
if (!(Test-Path -LiteralPath $NodePath)) { throw '找不到 Node，请通过 -NodePath 指定 Node 24。' }
$env:MONITOR_NODE = $NodePath
$env:PYTHONUTF8 = '1'
$taskDependencyPath = Join-Path $PSScriptRoot 'runtime\python-deps'
if (Test-Path -LiteralPath $taskDependencyPath) { $env:PYTHONPATH = $taskDependencyPath }
Push-Location $PSScriptRoot
try {
    & $PythonPath -m monitor $Command --config config.json --seconds $Seconds
    if ($LASTEXITCODE -ne 0) { throw "监控命令退出，代码 $LASTEXITCODE。请查看 runtime 中的日志。" }
} finally { Pop-Location }
