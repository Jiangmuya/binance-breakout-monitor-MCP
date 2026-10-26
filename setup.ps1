param([string]$PythonPath = "$env:USERPROFILE\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe")
$ErrorActionPreference = 'Stop'
if (!(Test-Path -LiteralPath $PythonPath)) { throw '请指定 Python 3.12 路径。' }
$taskTempPath = Join-Path $PSScriptRoot 'runtime\setup-tmp'
$taskDepsPath = Join-Path $PSScriptRoot 'runtime\python-deps'
New-Item -ItemType Directory -Force -Path $taskTempPath,$taskDepsPath | Out-Null
$taskOldTemp = $env:TEMP
$taskOldTmp = $env:TMP
try {
    $env:TEMP=$taskTempPath; $env:TMP=$taskTempPath
    & $PythonPath -m pip install --disable-pip-version-check --target $taskDepsPath -r (Join-Path $PSScriptRoot 'requirements.lock')
    if ($LASTEXITCODE -ne 0) { throw '依赖安装未成功。' }
} finally { $env:TEMP=$taskOldTemp; $env:TMP=$taskOldTmp }
& (Join-Path $PSScriptRoot 'setup-export.ps1')
Write-Output 'Python 和流式 Excel 依赖已检查完成。'
