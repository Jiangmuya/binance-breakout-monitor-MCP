"""Compatibility checks for the Windows PowerShell 5.1 launchers."""
import codecs
import json
import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ('start-local.ps1', 'setup.ps1')

class PowerShellCompatibilityTests(unittest.TestCase):
    def test_non_ascii_scripts_have_utf8_bom(self):
        for name in SCRIPTS:
            data = (ROOT / name).read_bytes()
            with self.subTest(script=name):
                data.decode('utf-8-sig')
                self.assertTrue(data.isascii() or data.startswith(codecs.BOM_UTF8),
                                'Windows PowerShell requires a BOM for non-ASCII UTF-8 scripts')

    @unittest.skipUnless(os.name == 'nt', 'Windows PowerShell is only available on Windows')
    def test_windows_powershell_parser(self):
        executable = Path(os.environ['SystemRoot']) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
        command = """
$result = @()
foreach ($name in @('start-local.ps1', 'setup.ps1')) {
    $tokens = $null
    $parseErrors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $env:MONITOR_TEST_ROOT $name), [ref]$tokens, [ref]$parseErrors)
    $result += @{script=$name; errors=@($parseErrors).Count}
}
@{version=$PSVersionTable.PSVersion.ToString(); scripts=$result} | ConvertTo-Json -Compress
"""
        environment = dict(os.environ, MONITOR_TEST_ROOT=str(ROOT))
        result = subprocess.run([str(executable), '-NoProfile', '-NonInteractive', '-Command', command],
                                env=environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report['version'].startswith('5.1.'))
        self.assertEqual([row['errors'] for row in report['scripts']], [0, 0])

if __name__ == '__main__':
    unittest.main()
