"""Inspect special/ordinary trigger labels in a real offline XLSX export."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile
from monitor.report_parameters import parameter_rows
from types import SimpleNamespace
ROOT = Path(__file__).resolve().parents[1]
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

class SpecialReportingTests(unittest.TestCase):
    def test_parameters_keep_special_rules_separate_from_normal_and_history(self):
        config = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
        from monitor.core import rules_for_interval
        rows = parameter_rows(SimpleNamespace(version='v4-test'), config, '15m', 0, 0,
                              {}, [], rules_for_interval(config, '15m'), 0, 86400000,
                              0, [], 'CANDLE_CLOSE')
        values = {r['key']: r['value'] for r in rows}
        self.assertIn('量比>= 12', values['特别条件'])
        self.assertIn('实体扩张倍数>= 6', values['特别条件'])
        self.assertIn('不要求200根', values['特别条件'])
        self.assertIn('<=41%', values['形态与突破'])
        self.assertIn('>=59%', values['形态与突破'])
        self.assertIn('07:45', values['采集窗口'])
        self.assertIn('V>V15_max整行红字', values['放量与红字'])
        self.assertIn('原放量红字判定保留', values['放量与红字'])
        self.assertIn('蓝字优先于红字', values['放量与红字'])

    def test_actual_workbook_trigger_labels_tail_columns_and_cached_rows(self):
        bundled = Path.home() / '.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe'
        node = os.environ.get('EXCEL_NODE') or shutil.which('node') or str(bundled)
        if not Path(node).is_file() or not (ROOT / 'node_modules/archiver').is_dir():
            self.skipTest('Node and locked archiver dependencies are required')
        stamp = 1792627200000
        base = dict(symbol='TESTUSDT', contract_type='PERPETUAL', interval='1h',
                    t=stamp-3600000, T=stamp-1, close_boundary=stamp, discovered=stamp,
                    comparison_window=15, V_mean=100, V_max=1400, v=1200,
                    strategy_version='v4-test', trigger=True, volume_highlight=False)
        paths = [('STANDARD', '普通条件'), ('SPECIAL', '特别条件'),
                 ('BOTH', '普通条件＋特别条件')]
        rows = [dict(base, signal_id=path, trigger_path=path) for path, label in paths]
        rows.append(dict(base, signal_id='legacy', strategy_version='v3-legacy'))
        overview = [dict(base, trigger=False, trigger_path='NONE')]
        book = dict(filename='special.xlsx', interval='1h', generated=stamp,
                    comparison_window=15, refresh_boundary=stamp,
                    coverage=dict(completed=1, expected=1), triggers=rows,
                    overview=overview, previous=[], status=[], recent_anomalies=[],
                    parameters=[dict(key='说明', value='offline fixture')])
        original = copy.deepcopy(book)
        with tempfile.TemporaryDirectory(prefix='special-report-') as directory:
            target = Path(directory)
            spec = dict(format_version=4, books=[book], format_cache=str(target / 'cache.sqlite3'))
            source = target / 'input.json'
            source.write_text(json.dumps(spec, ensure_ascii=False), encoding='utf-8')
            command = [node, '--max-old-space-size=256', str(ROOT / 'excel_builder.mjs'),
                       str(source), str(target / 'out')]
            for attempt in range(2):
                result = subprocess.run(command, cwd=ROOT, capture_output=True,
                                        text=True, encoding='utf-8', timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr)
                metrics = json.loads((target / 'out/metrics.json').read_text(encoding='utf-8'))
                self.assertEqual(metrics['status'], 'SUCCESS')
                if attempt:
                    self.assertGreater(metrics['cache_hits'], 0)
                    self.assertEqual(metrics['cache_misses'], 0)
                with zipfile.ZipFile(target / 'out/special.xlsx') as archive:
                    sheet = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
                    self.assertIsNone(sheet.find('s:autoFilter', NS))
                    headers = sheet.findall('s:sheetData/s:row[@r="6"]/s:c', NS)
                    labels = [''.join(c.itertext()) for c in headers]
                    self.assertEqual(labels[-2:], ['signal_id', '策略版本'])
                    self.assertEqual(labels.count('触发依据'), 1)
                    table = ET.fromstring(archive.read('xl/tables/table1.xml'))
                    self.assertEqual([c.attrib['name'] for c in table.findall('s:tableColumns/s:tableColumn', NS)], labels)
                    index = labels.index('触发依据')
                    data_rows = sheet.findall('s:sheetData/s:row', NS)
                    actual = [''.join(list(row)[index].itertext()) for row in data_rows if int(row.attrib['r']) >= 7]
                    self.assertEqual(actual, [label for path, label in paths] + ['普通条件'])
                    overview_sheet = ET.fromstring(archive.read('xl/worksheets/sheet2.xml'))
                    overview_labels = [''.join(c.itertext()) for c in overview_sheet.findall('s:sheetData/s:row[@r="6"]/s:c', NS)]
                    overview_cells = overview_sheet.findall('s:sheetData/s:row[@r="7"]/s:c', NS)
                    self.assertEqual(''.join(overview_cells[overview_labels.index('触发依据')].itertext()), '未触发')
        self.assertEqual(book, original)

if __name__ == '__main__':
    unittest.main()
