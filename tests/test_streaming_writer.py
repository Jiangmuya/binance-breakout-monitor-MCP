"""Exercise the real streaming writer and inspect its OOXML, offline only."""
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


NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
ROOT = Path(__file__).resolve().parents[1]


class StreamingWriterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bundled = Path.home() / '.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe'
        cls.node = os.environ.get('EXCEL_NODE') or shutil.which('node') or str(bundled)
        if not Path(cls.node).is_file() or not (ROOT / 'node_modules/archiver').is_dir():
            raise unittest.SkipTest('Node and npm ci are required for real writer tests')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='stream-writer-')
        self.target = Path(self.temp.name)
        self.stamp = 1792627200000
        self.book = dict(filename='fixture.xlsx', interval='1h', generated=self.stamp,
                         comparison_window=15, report_period_start=self.stamp,
                         report_period_end=self.stamp + 86400000,
                         refresh_boundary=self.stamp, refresh_reason='CANDLE_CLOSE',
                         coverage=dict(completed=523, expected=524, missing=['MISSINGUSDT']))
        base = dict(symbol='TESTUSDT', contract_type='PERPETUAL', interval='1h',
                    t=self.stamp-3600000, T=self.stamp-1, close_boundary=self.stamp,
                    discovered=self.stamp, strategy_version='v3-fixture',
                    comparison_window=15, V_mean=100, V_max=300, v=500)
        cases = [dict(signal_id='old20', comparison_window=20, volume_highlight=True),
                 dict(signal_id='old15'),
                 dict(signal_id='saved-false', volume_highlight=False),
                 dict(signal_id='saved-four', v=400, volume_highlight=True,
                      volume_highlight_multiple=4)]
        self.book['triggers'] = [dict(base, **row) for row in cases]
        self.book['overview'] = [dict(symbol='TESTUSDT', health='OK', readiness='READY')]
        self.book['previous'] = []
        self.book['status'] = [dict(symbol='TESTUSDT', code='OK', time=self.stamp)]
        self.book['parameters'] = [dict(key='source', value='offline fixture')]
        self.book['recent_anomalies'] = [dict(first_time=self.stamp-1000, last_time=self.stamp,
                                              occurrence_count=7, symbol='TESTUSDT',
                                              code='REST_RETRY', detail='retry')]
        self.spec = dict(format_version=4, books=[self.book],
                         format_cache=str(self.target / 'format.sqlite3'))

    def tearDown(self):
        self.temp.cleanup()

    def run_writer(self, spec=None, raw=None):
        source = self.target / 'snapshot.json'
        source.write_text(raw if raw is not None else json.dumps(spec or self.spec), encoding='utf-8')
        result = subprocess.run([self.node, '--max-old-space-size=256',
                                 str(ROOT / 'excel_builder.mjs'), str(source),
                                 str(self.target / 'xlsx')], cwd=ROOT,
                                capture_output=True, text=True, encoding='utf-8', timeout=30)
        metrics = json.loads((self.target / 'xlsx/metrics.json').read_text(encoding='utf-8'))
        return result, metrics

    def test_style_legacy_decisions_anomalies_and_cache_append(self):
        result, metrics = self.run_writer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(metrics['status'], 'SUCCESS')
        with zipfile.ZipFile(self.target / 'xlsx/fixture.xlsx') as archive:
            self.assertIsNone(archive.testzip())
            workbook = ET.fromstring(archive.read('xl/workbook.xml'))
            self.assertEqual([s.attrib['name'] for s in workbook.findall('s:sheets/s:sheet', NS)],
                             ['触发记录', '监控概览', '历史K线对照', '数据状态与异常', '参数与说明', '近期异常'])
            self.assertEqual(workbook.find('s:bookViews/s:workbookView', NS).attrib['activeTab'], '0')
            styles = ET.fromstring(archive.read('xl/styles.xml'))
            fonts = styles.findall('s:fonts/s:font', NS)
            formats = styles.findall('s:cellXfs/s:xf', NS)
            borders = styles.findall('s:borders/s:border', NS)
            for sheet_number in range(1, 7):
                sheet = ET.fromstring(archive.read(f'xl/worksheets/sheet{sheet_number}.xml'))
                # Excel rejects a worksheet AutoFilter overlapping a structured
                # table. Keep filtering owned by the table, including empty data.
                self.assertIsNone(sheet.find('s:autoFilter', NS))
                table_parts = sheet.findall('s:tableParts/s:tablePart', NS)
                self.assertEqual(len(table_parts), 1)
                relation_id = table_parts[0].attrib[
                    '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']
                relations = ET.fromstring(archive.read(
                    f'xl/worksheets/_rels/sheet{sheet_number}.xml.rels'))
                matching = [r for r in relations if r.attrib['Id'] == relation_id]
                self.assertEqual(len(matching), 1)
                relation = matching[0]
                self.assertEqual(relation.attrib['Type'],
                    'http://schemas.openxmlformats.org/officeDocument/2006/relationships/table')
                self.assertEqual(relation.attrib['Target'], f'../tables/table{sheet_number}.xml')
                table = ET.fromstring(archive.read(f'xl/tables/table{sheet_number}.xml'))
                table_filter = table.find('s:autoFilter', NS)
                self.assertIsNotNone(table_filter)
                self.assertEqual(table_filter.attrib['ref'], table.attrib['ref'])
                headers = sheet.findall('s:sheetData/s:row[@r="6"]/s:c', NS)
                columns = table.findall('s:tableColumns/s:tableColumn', NS)
                self.assertEqual([''.join(c.itertext()) for c in headers],
                                 [c.attrib['name'] for c in columns])
                pane = sheet.find('s:sheetViews/s:sheetView/s:pane', NS)
                self.assertEqual(pane.attrib['ySplit'], '6')
                self.assertEqual(pane.attrib.get('xSplit', '0'), '0')
                for row in sheet.findall('s:sheetData/s:row', NS):
                    if int(row.attrib['r']) < 6:
                        continue
                    for cell in row:
                        style = formats[int(cell.attrib['s'])]
                        border = borders[int(style.attrib['borderId'])]
                        for edge in ('left', 'right', 'top', 'bottom'):
                            self.assertEqual(border.find(f's:{edge}', NS).attrib['style'], 'thin')
                        if sheet_number == 1 and int(row.attrib['r']) >= 7:
                            font = fonts[int(style.attrib['fontId'])]
                            red = font.find('s:color', NS).attrib['rgb'] == 'FFFF0000'
                            self.assertEqual(red, int(row.attrib['r']) in (8, 10))
                if sheet_number == 1:
                    text = ''.join(sheet.find('s:sheetData/s:row[@r="4"]', NS).itertext())
                    self.assertIn('523/524', text)
                    self.assertIn('不完整', text)
                if sheet_number == 6:
                    cells = sheet.findall('s:sheetData/s:row[@r="7"]/s:c', NS)
                    self.assertEqual(cells[2].find('s:v', NS).text, '7')
                    date1 = float(cells[0].find('s:v', NS).text)
                    date2 = float(cells[1].find('s:v', NS).text)
                    self.assertAlmostEqual((date2-date1)*86400000, 1000, delta=.001)
        previous_count = metrics['cache_misses']
        appended = copy.deepcopy(self.book['triggers'][-1])
        appended['signal_id'] = 'appended'
        self.book['triggers'].append(appended)
        result, metrics = self.run_writer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(metrics['books'][0]['signal_count'], 5)
        self.assertEqual(metrics['cache_hits'], previous_count)
        self.assertEqual(metrics['cache_misses'], 1)

    def test_memory_budget_failure_is_recorded_without_output(self):
        self.spec['budget'] = dict(rss_bytes=1)
        result, metrics = self.run_writer()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(metrics['status'], 'FAILED')
        self.assertIn('MEMORY_BUDGET_EXCEEDED', metrics['error']['message'])
        self.assertGreater(metrics['peak_rss_bytes'], 0)
        self.assertFalse((self.target / 'xlsx/fixture.xlsx').exists())

    def test_zero_target_or_insufficient_count_is_not_labeled_complete(self):
        for completed, expected in [(0, 0), (523, 524)]:
            with self.subTest(completed=completed, expected=expected):
                self.book['coverage'] = dict(completed=completed, expected=expected, missing=[])
                result, metrics = self.run_writer()
                self.assertEqual(result.returncode, 0, result.stderr)
                with zipfile.ZipFile(self.target / 'xlsx/fixture.xlsx') as archive:
                    sheet = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
                    text = ''.join(sheet.find('s:sheetData/s:row[@r="4"]', NS).itertext())
                    self.assertIn('不完整', text)

    def test_ndjson_count_mismatch_removes_partial_workbook(self):
        source = self.target / 'rows.ndjson'
        source.write_text(json.dumps(self.book['triggers'][0]) + '\n', encoding='utf-8')
        self.book['triggers'] = dict(ndjson=str(source), count=2)
        result, metrics = self.run_writer()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('ROW_COUNT_MISMATCH', metrics['error']['message'])
        self.assertFalse((self.target / 'xlsx/fixture.xlsx').exists())

    def test_missing_ndjson_is_recorded_without_hanging(self):
        self.book['triggers'] = dict(ndjson=str(self.target / 'absent.ndjson'), count=1)
        result, metrics = self.run_writer()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(metrics['status'], 'FAILED')
        self.assertIn('ENOENT', metrics['error']['message'])
        self.assertFalse((self.target / 'xlsx/fixture.xlsx').exists())

    def test_invalid_envelope_is_recorded(self):
        result, metrics = self.run_writer(raw='{invalid')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(metrics['status'], 'FAILED')
        self.assertEqual(metrics['error']['name'], 'SyntaxError')

    def test_output_open_failure_is_recorded_without_hanging(self):
        (self.target / 'xlsx/fixture.xlsx').mkdir(parents=True)
        result, metrics = self.run_writer()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(metrics['status'], 'FAILED')


if __name__ == '__main__':
    unittest.main()
