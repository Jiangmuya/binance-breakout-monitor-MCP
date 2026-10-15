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
    Write-Host ("[{0}] 币安监控：正在执行 {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Command)
    Write-Host ("程序目录：{0}" -f $PSScriptRoot)
    if ($Command -eq 'run') {
        Write-Host '正在启动并恢复历史数据，请保持窗口打开；初始化完成后会显示运行状态。'
        Write-Host '未触发信号时仍会持续监控。控制台状态每60秒显示一次，不影响Excel收盘刷新时间。'
    }
    & $PythonPath -u -m monitor $Command --config config.json --seconds $Seconds
    if ($LASTEXITCODE -ne 0) { throw "监控命令退出，代码 $LASTEXITCODE。请查看 runtime 中的日志。" }
} finally { Pop-Location }
