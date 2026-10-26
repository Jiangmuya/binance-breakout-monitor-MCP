"""Exact saved evaluations and current health are independent report facts."""
import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from monitor.core import Store,dumps,PERIODS
from monitor.export import close_coverage,snapshot
from test_monitor import CONFIG,BASE,info,bars

class CloseProcessingProofTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.config=dict(CONFIG,data_dir=self.tmp.name,excel_dir=self.tmp.name)
        self.store=Store(self.root/'monitor.sqlite3',self.config)
        self.store.refresh({'symbols':[info()]})
        data=bars(23,'15m');self.close=data[20][6]+1;self.target=data[20][0]
        self.store.ingest('TESTUSDT','15m',data[:21],'HISTORICAL',self.close+1000)
        self.original=self.store.db.execute('SELECT payload FROM evaluations WHERE t=?',(self.target,)).fetchone()[0]

    def tearDown(self):self.store.close();self.tmp.cleanup()

    def book(self):
        with patch('monitor.export.now_ms',return_value=self.close+1000):
            return snapshot(self.store,self.config,['15m'],'RECOVERY_COMPLETE',self.close)[0]

    def test_saved_close_remains_processed_while_current_warning_is_visible(self):
        self.store.mark('TESTUSDT','15m','DATA_ERROR','later input or connection failure')
        before=self.store.state('TESTUSDT','15m')
        c=close_coverage(self.store,'15m',self.close);b=self.book()
        self.assertEqual(c,b['coverage']);self.assertEqual(c['completed'],1)
        self.assertEqual(c['current_warnings'],['TESTUSDT']);self.assertTrue(b['complete'])
        self.assertEqual(b['status'][0]['code'],'CURRENT_STATE_WARNING')
        self.assertIn('DATA_ERROR',b['status'][0]['detail'])
        self.assertEqual(b['overview'][0]['health'],'DATA_ERROR')
        self.assertFalse(b['overview'][0]['stale'])
        self.assertEqual(self.store.state('TESTUSDT','15m'),before)

    def test_exact_old_close_survives_newer_current_state_error(self):
        self.store.ingest('TESTUSDT','15m',bars(23,'15m')[21:],'REALTIME',self.close+2*PERIODS['15m']+1000)
        self.store.mark('TESTUSDT','15m','DATA_ERROR','newer state error')
        b=self.book();self.assertTrue(b['complete']);self.assertEqual(b['overview'][0]['close_boundary'],self.close)
        self.assertEqual(b['overview'][0]['health'],'DATA_ERROR')

    def test_corrupt_or_mismatched_saved_rows_do_not_prove_processing(self):
        self.store.mark('TESTUSDT','15m','DATA_ERROR','unresolved')
        original=json.loads(self.original)
        wrong=[]
        for key,value in [('strategy_version','wrong'),('symbol','OTHERUSDT'),('interval','1h')]:
            x=copy.deepcopy(original);x[key]=value;wrong.append(dumps(x))
        x=copy.deepcopy(original);x['bar']['t']+=1;wrong.append(dumps(x))
        x=copy.deepcopy(original);x['bar']['T']+=1;wrong.append(dumps(x))
        wrong+=['not-json','null','{}']
        for payload in wrong:
            with self.subTest(payload=payload[:30]):
                self.store.db.execute('UPDATE evaluations SET payload=? WHERE t=?',(payload,self.target));self.store.db.commit()
                c=close_coverage(self.store,'15m',self.close)
                self.assertEqual(c['completed'],0);self.assertEqual(c['missing'],['TESTUSDT'])
                self.assertFalse(self.book()['complete'])

    def test_removed_proof_invalidates_cached_rows_without_clearing_warning(self):
        self.store.mark('TESTUSDT','15m','DATA_ERROR','unresolved')
        self.assertTrue(self.book()['complete'])
        self.store.db.execute('DELETE FROM evaluations WHERE t=?',(self.target,));self.store.db.commit()
        b=self.book();self.assertFalse(b['complete']);self.assertTrue(b['overview'][0]['stale'])
        self.assertEqual(b['status'][0]['code'],'CLOSE_NOT_PROCESSED')

    def test_legacy_fallback_requires_exact_latest_candle_and_healthy_state(self):
        self.store.db.execute('DELETE FROM evaluations');self.store.db.commit()
        self.assertEqual(close_coverage(self.store,'15m',self.close)['completed'],1)
        self.store.mark('TESTUSDT','15m','DATA_ERROR','unresolved')
        self.assertEqual(close_coverage(self.store,'15m',self.close)['completed'],0)
        self.assertEqual(close_coverage(self.store,'15m',self.close+PERIODS['15m'])['completed'],0)

if __name__=='__main__':unittest.main()
