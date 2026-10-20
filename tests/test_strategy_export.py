"""Export compatibility for preserved v1 history and the current 15-bar strategy."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from monitor.core import PERIODS, Store, dumps
from monitor.export import close_coverage, flatten, partitions, previous_rows, snapshot
BASE=1704067200000
CONFIG=json.loads((Path(__file__).resolve().parents[1]/'config.json').read_text(encoding='utf-8'))

def info(symbol='TESTUSDT'):
    return dict(symbol=symbol,quoteAsset='USDT',marginAsset='USDT',status='TRADING',contractType='PERPETUAL')


def payload(window=15, volume=500, maximum=200, mean=100, interval='1h', symbol='TESTUSDT'):
    step=PERIODS[interval]
    previous=[dict(t=BASE+i*step,T=BASE+(i+1)*step-1,o=10,h=12,l=9,c=11,v=100,q=1100)
              for i in range(window)]
    bar=dict(t=BASE+window*step,T=BASE+(window+1)*step-1,o=11,h=15,l=8,c=14,v=volume,q=volume*14)
    m=dict(checks=[True,False,True,True,True,True],trigger=False,ema_bullish=False,
           body_share=.75,upper_wick_share=.25,thresholds={'volume_multiple':3 if window==15 else 2},
           **{f'V{window}_mean':mean,f'V{window}_median':mean,f'V{window}_max':maximum,
              f'B{window}_mean':.1,f'G{window}_mean':.1,f'G{window}_median':.1,
              f'G{window}_max':.2,f'H{window}':12,f'abs_body{window}_mean':1,
              f'abs_body{window}_median':1,f'abs_body{window}_max':2})
    p=dict(bar=bar,metrics=m,symbol=symbol,interval=interval,contract_type='PERPETUAL',
           ema=[12,13,10],readiness='READY',seed=BASE,count=1500,strategy_version=f'v{window}',
           discovered=bar['T']+101,received=bar['T']+100,processed=bar['T']+101,
           signal_id=f'{window}-{symbol}',event_type='REALTIME',origin='REALTIME',
           **{f'previous{window}':previous,f'previous{window}_before_close':10})
    if window==15:
        p['comparison_window']=15
        m['comparison_window']=15
    return p


class StrategyExportTests(unittest.TestCase):
    def test_new_and_old_values_keep_their_original_window(self):
        for window in (15,20):
            p=payload(window)
            original=copy.deepcopy(p)
            row=flatten(p)
            self.assertEqual(row['comparison_window'],window)
            self.assertEqual(row['V_mean'],100)
            self.assertEqual(row['V_max'],200)
            self.assertEqual(row['H'],12)
            self.assertEqual(row['body_share'],.75)
            self.assertEqual(row['check2'],False)
            self.assertEqual(p,original)
            previous=previous_rows(p)
            self.assertEqual(len(previous),window)
            self.assertEqual([r['index'] for r in previous],list(range(1,window+1)))
            self.assertTrue(all(r['comparison_window']==window for r in previous))
            self.assertAlmostEqual(previous[0]['close_return'],.1)
            self.assertTrue(all(r['t']<p['bar']['t'] for r in previous))

    def test_red_font_threshold_is_independent_of_signal_and_strict_max(self):
        for volume,maximum,mean,expected in [
            (500,200,100,True),(499.999,200,100,False),
            (500,500,100,False),(501,500,100,True),
            (1,0,0,True),(0,0,0,False),
        ]:
            with self.subTest(volume=volume,maximum=maximum,mean=mean):
                row=flatten(payload(volume=volume,maximum=maximum,mean=mean))
                self.assertFalse(row['trigger'])
                self.assertEqual(row['volume_highlight'],expected)
        self.assertFalse(flatten(payload(20,volume=10000))['volume_highlight'])

    def test_all_four_intervals_export_their_own_previous_fifteen(self):
        for interval,step in PERIODS.items():
            p=payload(interval=interval)
            rows=previous_rows(p)
            self.assertEqual(len(rows),15)
            self.assertTrue(all(r['interval']==interval for r in rows))
            self.assertEqual(p['bar']['t']-rows[0]['t'],15*step)
            self.assertEqual(p['bar']['t']-rows[-1]['t'],step)

    def test_mixed_partitions_follow_actual_comparison_row_counts(self):
        signals=[payload(20),payload(15,symbol='SECONDUSDT'),payload(15,symbol='THIRDUSDT')]
        groups=partitions(signals,40)
        self.assertEqual([len(g[2]) for g in groups],[2,1])
        self.assertEqual([sum(len(previous_rows(p)) for p in g[2]) for g in groups],[35,15])
        self.assertEqual([p['signal_id'] for g in groups for p in g[2]],[p['signal_id'] for p in signals])
        # Missing comparison data must not let the trigger table exceed its cap.
        no_previous=[dict(payload(),previous15=[],signal_id=str(i)) for i in range(41)]
        self.assertEqual([len(g[2]) for g in partitions(no_previous,20)],[20,20,1])

    def test_snapshot_retains_v1_signals_and_excludes_stale_delivery_universe(self):
        with tempfile.TemporaryDirectory() as directory:
            config=dict(CONFIG,data_dir=directory,excel_dir=directory)
            store=Store(Path(directory)/'db.sqlite3',config)
            try:
                store.refresh({'symbols':[info()]})
                delivery=dict(info('DELIVERYUSDT'),contractType='CURRENT_QUARTER')
                with store.lock,store.db:
                    store.db.execute('INSERT INTO universe VALUES(?,?,?,?,?)',
                                     ('DELIVERYUSDT',1,dumps(delivery),'',BASE))
                    for window in (20,15):
                        p=payload(window,symbol=f'HISTORY{window}USDT')
                        if window==20: p['contract_type']='CURRENT_QUARTER'
                        store.db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?,?)',
                            (p['signal_id'],p['strategy_version'],p['symbol'],p['interval'],
                             p['bar']['t'],p['event_type'],dumps(p)))
                self.assertEqual(close_coverage(store,'1h',BASE+PERIODS['1h'])['expected'],1)
                books=snapshot(store,config,intervals=['1h'])
                self.assertEqual(len(books),1)
                book=books[0]
                self.assertEqual(book['comparison_window'],15)
                self.assertEqual([r['symbol'] for r in book['overview']],['TESTUSDT'])
                self.assertEqual({r['comparison_window'] for r in book['triggers']},{15,20})
                self.assertEqual(len(book['previous']),35)
                self.assertIn('CURRENT_QUARTER',{r['contract_type'] for r in book['triggers']})
                old=next(r for r in book['triggers'] if r['comparison_window']==20)
                self.assertEqual(json.loads(old['thresholds'])['volume_multiple'],2)
                self.assertFalse(old['volume_highlight'])
                self.assertEqual(book['coverage']['expected'],1)
            finally:
                store.close()


if __name__=='__main__':
    unittest.main()
