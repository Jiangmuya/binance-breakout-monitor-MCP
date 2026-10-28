"""Real XLSX regressions for saved special-signal blue priority and cache reuse."""
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

ROOT=Path(__file__).resolve().parents[1]
NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
BLUE='FF0000FF'
RED='FFFF0000'
NORMAL='FF22334A'


class SpecialBlueWriterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        bundled=Path.home()/'.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe'
        cls.node=os.environ.get('EXCEL_NODE') or shutil.which('node') or str(bundled)
        if not Path(cls.node).is_file() or not (ROOT/'node_modules/archiver').is_dir():
            raise unittest.SkipTest('Node and locked archiver dependencies are required')

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='special-blue-')
        self.target=Path(self.temp.name)
        self.stamp=1793145600000

    def tearDown(self):
        self.temp.cleanup()

    def books(self):
        cases=[
            (dict(signal_id='special-below-max',trigger_path='SPECIAL',special_trigger=True,
                  v=1200,V_max=1400,volume_highlight=False),BLUE),
            (dict(signal_id='special-also-red',trigger_path='SPECIAL',special_trigger=True,
                  v=2000,V_max=1400,volume_highlight=True),BLUE),
            (dict(signal_id='both-also-red',trigger_path='BOTH',special_trigger=True,
                  v=2000,V_max=1400,volume_highlight=True),BLUE),
            (dict(signal_id='standard-red',trigger_path='STANDARD',special_trigger=False,
                  v=500,V_max=300,volume_highlight=True),RED),
            (dict(signal_id='standard-normal',trigger_path='STANDARD',special_trigger=False,
                  v=300,V_max=200,volume_highlight=False),NORMAL),
            (dict(signal_id='legacy-high-ratios-red',strategy_version='v3-legacy',
                  v=2000,V_max=1400,volume_ratio=20,relative_body_ratio=10,
                  volume_highlight=True),RED),
            (dict(signal_id='legacy-high-ratios-normal',strategy_version='v3-legacy',
                  v=2000,V_max=1400,volume_ratio=20,relative_body_ratio=10,
                  volume_highlight=False),NORMAL),
        ]
        expected=[color for _,color in cases]
        books=[]
        for interval,step in (('15m',900000),('1h',3600000),('4h',14400000),('1d',86400000)):
            base=dict(symbol='TESTUSDT',contract_type='PERPETUAL',interval=interval,
                      t=self.stamp-step,T=self.stamp-1,close_boundary=self.stamp,
                      discovered=self.stamp,comparison_window=15,V_mean=100,
                      strategy_version='v4-test',trigger=True,health='OK',readiness='READY')
            rows=[dict(base,**row) for row,_ in cases]
            overview=copy.deepcopy(rows)
            overview.append(dict(base,signal_id=None,trigger=False,trigger_path='NONE',
                                 special_trigger=False,v=500,V_max=300,volume_highlight=True))
            books.append(dict(filename=f'blue-{interval}.xlsx',interval=interval,
                generated=self.stamp,comparison_window=15,refresh_boundary=self.stamp,
                coverage=dict(completed=len(overview),expected=len(overview)),
                triggers=rows,overview=overview,previous=[],status=[],recent_anomalies=[],
                parameters=[dict(key='Fixture',value='offline color regression')]))
        return books,expected

    def run_writer(self,books):
        spec=dict(format_version=4,books=books,format_cache=str(self.target/'format.sqlite3'))
        source=self.target/'input.json'
        source.write_text(json.dumps(spec,ensure_ascii=False),encoding='utf-8')
        result=subprocess.run([self.node,'--max-old-space-size=256',str(ROOT/'excel_builder.mjs'),
            str(source),str(self.target/'out')],cwd=ROOT,capture_output=True,text=True,
            encoding='utf-8',timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        metrics=json.loads((self.target/'out/metrics.json').read_text(encoding='utf-8'))
        self.assertEqual(metrics['status'],'SUCCESS')
        return metrics

    def assert_book_colors(self,filename,expected):
        with zipfile.ZipFile(self.target/'out'/filename) as archive:
            self.assertIsNone(archive.testzip())
            styles=ET.fromstring(archive.read('xl/styles.xml'))
            fonts=styles.findall('s:fonts/s:font',NS)
            formats=styles.findall('s:cellXfs/s:xf',NS)
            borders=styles.findall('s:borders/s:border',NS)
            self.assertEqual(int(styles.find('s:fonts',NS).attrib['count']),len(fonts))
            workbook=ET.fromstring(archive.read('xl/workbook.xml'))
            self.assertEqual(workbook.find('s:bookViews/s:workbookView',NS).attrib['activeTab'],'0')
            for number,colors in ((1,expected),(2,expected+[RED])):
                sheet=ET.fromstring(archive.read(f'xl/worksheets/sheet{number}.xml'))
                headers=sheet.findall('s:sheetData/s:row[@r="6"]/s:c',NS)
                labels=[''.join(cell.itertext()) for cell in headers]
                if number==1:self.assertEqual(labels[-2:],['signal_id','策略版本'])
                self.assertIsNone(sheet.find('s:autoFilter',NS))
                table=ET.fromstring(archive.read(f'xl/tables/table{number}.xml'))
                self.assertEqual(table.find('s:autoFilter',NS).attrib['ref'],table.attrib['ref'])
                self.assertEqual([c.attrib['name'] for c in table.findall('s:tableColumns/s:tableColumn',NS)],labels)
                pane=sheet.find('s:sheetViews/s:sheetView/s:pane',NS)
                self.assertEqual(pane.attrib['ySplit'],'6')
                self.assertEqual(pane.attrib.get('xSplit','0'),'0')
                rows=[r for r in sheet.findall('s:sheetData/s:row',NS) if int(r.attrib['r'])>=7]
                self.assertEqual(len(rows),len(colors))
                for row,color in zip(rows,colors):
                    cells=row.findall('s:c',NS)
                    self.assertEqual(len(cells),len(headers))
                    for cell in cells:
                        style=formats[int(cell.attrib['s'])]
                        font=fonts[int(style.attrib['fontId'])]
                        self.assertEqual(font.find('s:color',NS).attrib['rgb'],color,
                                         f'{filename} sheet{number} {cell.attrib["r"]}')
                        border=borders[int(style.attrib['borderId'])]
                        for edge in ('left','right','top','bottom'):
                            self.assertEqual(border.find(f's:{edge}',NS).attrib['style'],'thin')

    def test_all_periods_blue_over_red_whole_rows_and_legacy_not_reclassified(self):
        books,expected=self.books();original=copy.deepcopy(books)
        self.run_writer(books)
        for book in books:
            with self.subTest(interval=book['interval']):
                self.assert_book_colors(book['filename'],expected)
        self.assertEqual(books,original)

    def test_cached_second_export_preserves_all_font_choices(self):
        books,expected=self.books()
        first=self.run_writer(books)
        self.assertGreater(first['cache_misses'],0)
        second=self.run_writer(books)
        self.assertGreater(second['cache_hits'],0)
        self.assertEqual(second['cache_misses'],0)
        for book in books:self.assert_book_colors(book['filename'],expected)


if __name__=='__main__':unittest.main()
