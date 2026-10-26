"""A newly listed symbol must not create an impossible prelisting close batch."""
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from monitor.core import Store, PERIODS
from monitor.export import close_coverage, snapshot, export_all
from monitor.close_applicability import close_applicable
from test_monitor import CONFIG, BASE, bars, info
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_shadow import ShadowRunner

class CloseApplicabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.config=dict(CONFIG,data_dir=self.temp.name,excel_dir=str(self.root/'excel'))
        self.store=Store(self.root/'monitor.sqlite3',self.config)

    def tearDown(self):
        self.store.close();self.temp.cleanup()

    def seed(self,interval='1d'):
        data=bars(2,interval);close=data[-1][6]+1
        self.store.refresh({'symbols':[info('OLDUSDT',onboardDate=BASE-1),info('NEWUSDT',onboardDate=close+1)]})
        self.store.ingest('OLDUSDT',interval,data,'HISTORICAL',close+1000)
        return close

    def book(self,interval,close):
        with patch('monitor.export.now_ms',return_value=close+1000):
            return snapshot(self.store,self.config,[interval],'RECOVERY_COMPLETE',close)[0]

    def test_listing_at_or_after_close_is_not_applicable_unknown_is_conservative(self):
        close=BASE+86400000
        for value in (close,close+1):
            self.assertFalse(close_applicable({'onboardDate':value},close))
        for value in (None,True,False,0,-1,'invalid',str(close),float('nan'),float('inf'),close-1):
            with self.subTest(value=value):self.assertTrue(close_applicable({'onboardDate':value},close))
        self.assertTrue(close_applicable({},close))

    def test_all_intervals_keep_all_symbols_and_explain_prelisting_exclusion(self):
        for interval in PERIODS:
            with self.subTest(interval=interval):
                close=self.seed(interval)
                coverage=close_coverage(self.store,interval,close)
                self.assertEqual((coverage['expected'],coverage['completed'],coverage['total_symbols']),(1,1,2))
                self.assertEqual(coverage['missing'],[])
                self.assertEqual(coverage['not_applicable'],['NEWUSDT'])
                book=self.book(interval,close)
                self.assertEqual(book['coverage'],coverage)
                self.assertTrue(book['complete'])
                self.assertEqual({r['symbol'] for r in book['overview']},{'OLDUSDT','NEWUSDT'})
                statuses={r['symbol']:r for r in book['status']}
                self.assertEqual(statuses['NEWUSDT']['code'],'NOT_APPLICABLE')
                self.assertIn(str(close),statuses['NEWUSDT']['detail'])
                parameters={row['key']:str(row['value']) for row in book['parameters']}
                self.assertTrue(any('1/1' in value for value in parameters.values()))
                self.assertEqual(self.store.universe().keys(),{'OLDUSDT','NEWUSDT'})

    def test_first_postlisting_close_still_requires_real_processed_candle(self):
        close=self.seed();next_close=close+PERIODS['1d']
        coverage=close_coverage(self.store,'1d',next_close)
        self.assertEqual(coverage['expected'],2)
        self.assertIn('NEWUSDT',coverage['missing'])
        self.assertEqual(coverage['not_applicable'],[])
        self.assertFalse(self.book('1d',next_close)['complete'])

    def test_listing_change_invalidates_cached_state_rows_at_same_close(self):
        close=self.seed();before=self.book('1d',close)
        self.assertTrue(before['complete'])
        self.store.refresh({'symbols':[info('OLDUSDT',onboardDate=BASE-1),info('NEWUSDT',onboardDate=close-1)]})
        after=self.book('1d',close)
        self.assertFalse(after['complete'])
        self.assertEqual(after['coverage']['missing'],['NEWUSDT'])
        self.assertEqual(next(r for r in after['status'] if r['symbol']=='NEWUSDT')['code'],'CLOSE_NOT_PROCESSED')

    def test_zero_applicable_symbols_does_not_report_complete(self):
        close=BASE+PERIODS['1d']
        self.store.refresh({'symbols':[info('NEWUSDT',onboardDate=close)]})
        book=self.book('1d',close)
        self.assertEqual(book['coverage']['expected'],0)
        self.assertEqual(book['coverage']['total_symbols'],1)
        self.assertFalse(book['complete'])
        self.assertEqual(len(book['overview']),1)

    def test_successful_publication_clears_the_impossible_old_pending_batch(self):
        close=self.seed();runner=ShadowRunner(self.root/'unused',self.store,self.config)
        runner.pending={'1d':{close}}
        def fake_builder(command,**kwargs):
            spec=json.loads(Path(command[-2]).read_text(encoding='utf-8'))
            for book in spec['books']:(Path(command[-1])/book['filename']).write_bytes(b'fixture')
            return SimpleNamespace(returncode=0,stderr='')
        before=self.store.version
        with patch('monitor.export.now_ms',return_value=close+1000),patch('monitor.export.run_builder',side_effect=fake_builder):
            export_all(self.store,self.config,intervals=['1d'],refresh_reason='RECOVERY_COMPLETE',refresh_boundary=close)
        runner.published('1d',close)
        self.assertEqual(runner.pending['1d'],set())
        self.assertEqual(self.store.version,before)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM signals').fetchone()[0],0)

if __name__=='__main__':unittest.main()
