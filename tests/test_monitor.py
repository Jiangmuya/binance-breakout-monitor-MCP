import asyncio
import copy
import datetime as dt
import json
import math
import os
import tempfile
import time
import unittest
from decimal import Decimal, getcontext
from pathlib import Path
from unittest.mock import patch
from monitor.core import Store, PERIODS, DEFAULT_RULES, eligible, parse_bar, metrics, version, now_ms
from monitor.export import snapshot, partitions, safe_replace, export_all
from monitor.notifications import dispatch_once
from monitor.transport import PublicClient, ALLOWED, WeightBudget

ROOT=Path(__file__).resolve().parents[1]
CONFIG=json.loads((ROOT/'config.json').read_text(encoding='utf-8'))
BASE=1704067200000

def info(symbol='TESTUSDT',**kw):
    return dict(symbol=symbol,quoteAsset='USDT',marginAsset='USDT',status='TRADING',contractType='PERPETUAL',**kw)

def bars(count=200,interval='1h',start=BASE):
    step=PERIODS[interval];out=[]
    for n in range(count):
        o=100+n*.1;c=o+.05
        out.append([start+n*step,o,c+.02,o-.01,c,100,start+(n+1)*step-1,100*c])
    return out

def signal_bar(previous,interval='1h',volume=1000):
    step=PERIODS[interval];t=previous[0]+step;o=previous[4];c=o*1.04
    return [t,o,c+.01,o-.01,c,volume,t+step-1,volume*c]

class CoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'test.sqlite3'
        self.config=dict(CONFIG,data_dir=self.tmp.name,excel_dir=self.tmp.name)
        self.store=Store(self.path,self.config);self.store.refresh({'symbols':[info()]})
    def tearDown(self): self.store.close();self.tmp.cleanup()
    def seed(self,n=200,interval='1h'):
        rows=bars(n,interval)
        self.store.ingest('TESTUSDT',interval,rows,'HISTORICAL',now_ms());return rows
    def fire(self,origin='REALTIME',age=10):
        rows=self.seed();b=signal_bar(rows[-1]);received=b[6]+1+age*1000
        with patch('monitor.core.now_ms',return_value=received):
            return self.store.ingest('TESTUSDT','1h',[b],origin,received),b

    def test_universe_filter_and_dynamic_remove(self):
        self.assertTrue(eligible(info('ABCUSDT')))
        delivery=info('ABCUSDT_261225');delivery['contractType']='CURRENT_QUARTER'
        self.assertFalse(eligible(delivery))
        bad=info();bad['marginAsset']='BTC';self.assertFalse(eligible(bad))
        added,removed=self.store.refresh({'symbols':[info('NEWUSDT')]})
        self.assertEqual(added,{'NEWUSDT'});self.assertEqual(removed,{'TESTUSDT'})
        self.assertEqual(set(self.store.universe()),{'NEWUSDT'})

    def test_empty_refresh_keeps_universe(self):
        self.store.refresh({'symbols':[]});self.assertIn('TESTUSDT',self.store.universe())
        self.assertTrue(self.store.get_meta('universe_stale'))

    def test_periods_independent(self):
        for n,i in enumerate(PERIODS): self.seed(200+n,i)
        self.assertEqual([self.store.state('TESTUSDT',i)['count'] for i in PERIODS],[200,201,202,203])

    def test_window_excludes_target(self):
        events,b=self.fire();self.assertEqual(len(events),1)
        self.assertEqual(len(events[0]['previous15']),15)
        self.assertEqual(events[0]['metrics']['V15_mean'],100)
        self.assertTrue(all(x['t']<b[0] for x in events[0]['previous15']))

    def test_unclosed_cannot_ingest(self):
        b=bars(1)[0]
        with self.assertRaisesRegex(ValueError,'UNCLOSED'): self.store.ingest('TESTUSDT','1h',[b],received=b[6]-1)
        self.assertIsNone(self.store.state('TESTUSDT','1h'))

    def test_ema_against_decimal_reference(self):
        getcontext().prec=45;rows=self.seed(1500)
        actual=self.store.state('TESTUSDT','1h')['ema']
        for n,value in zip((12,50,200),actual):
            alpha=Decimal(2)/Decimal(n+1);ref=Decimal(str(rows[0][4]))
            for b in rows[1:]: ref += alpha*(Decimal(str(b[4]))-ref)
            self.assertAlmostEqual(value,float(ref),places=10)

    def test_strict_ema_equality(self):
        w=[parse_bar(r,'1h',now_ms()) for r in bars(15)]
        b=parse_bar(signal_bar(bars(15)[-1]),'1h',now_ms())
        for index in range(3):
            ema=[101,101,100];ema[index]=b['c']
            self.assertFalse(metrics(b,w,ema,[100,100,99],DEFAULT_RULES)['checks'][1])

    def test_volume_boundary_and_max(self):
        w=[parse_bar(r,'1h',now_ms()) for r in bars(15)]
        b=parse_bar(signal_bar(bars(15)[-1],volume=300),'1h',now_ms())
        self.assertTrue(metrics(b,w,[102,101,100],[101,100,99],DEFAULT_RULES)['checks'][2])
        b['v']=299.999;self.assertFalse(metrics(b,w,[102,101,100],[101,100,99],DEFAULT_RULES)['checks'][2])
        w[0]['v']=1000;b['v']=1000
        self.assertFalse(metrics(b,w,[102,101,100],[101,100,99],DEFAULT_RULES)['checks'][2])

    def test_shape_boundary(self):
        w=[parse_bar(r,'1h',now_ms()) for r in bars(15)]
        b=dict(t=0,T=0,o=100.,c=107.,h=110.,l=80.,v=1000.,q=100.)
        m=metrics(b,w,[102,101,100],[101,100,99],DEFAULT_RULES)
        self.assertEqual(m['body_share'],.7);self.assertEqual(m['upper_wick_share'],.3)
        self.assertTrue(m['checks'][5]);b['h']+=.0001
        self.assertFalse(metrics(b,w,[102,101,100],[101,100,99],DEFAULT_RULES)['checks'][5])

    def test_readiness_199_200_1500(self):
        self.seed(199);self.assertEqual(self.store.state('TESTUSDT','1h')['readiness'],'NOT_READY')
        self.seed(200);self.assertEqual(self.store.state('TESTUSDT','1h')['readiness'],'SHORT_WARMUP')
        self.seed(1500);self.assertEqual(self.store.state('TESTUSDT','1h')['readiness'],'READY')

    def test_zero_denominator(self):
        w=[parse_bar(r,'1h',now_ms()) for r in bars(15)]
        for b in w: b['v']=0;b['c']=b['o']
        m=metrics(w[-1],w,[1,1,1],[1,1,1],DEFAULT_RULES)
        self.assertIsNone(m['volume_ratio']);self.assertFalse(m['trigger']);self.assertFalse(m['available'])

    def test_invalid_ohlc_and_nonfinite(self):
        b=bars(1)[0];b[2]=b[3]-1
        with self.assertRaisesRegex(ValueError,'OHLC'): parse_bar(b,'1h',now_ms())
        b=bars(1)[0];b[1]=math.nan
        with self.assertRaisesRegex(ValueError,'NONFINITE'): parse_bar(b,'1h',now_ms())

    def test_gap_atomic_state(self):
        self.seed();old=self.store.state('TESTUSDT','1h')
        with self.assertRaisesRegex(ValueError,'GAP'): self.store.ingest('TESTUSDT','1h',[bars(202)[-1]])
        self.assertEqual(self.store.state('TESTUSDT','1h'),old)

    def test_duplicate_and_out_of_order(self):
        self.seed();self.store.ingest('TESTUSDT','1h',list(reversed(bars(200))))
        self.assertEqual(self.store.state('TESTUSDT','1h')['count'],200)
        b=bars(200)[-1];b[5]+=1
        with self.assertRaisesRegex(ValueError,'CONFLICTING'):self.store.ingest('TESTUSDT','1h',[b])

    def test_daily_boundary_beijing(self):
        b=parse_bar(bars(1,'1d')[0],'1d',now_ms())
        local=dt.datetime.fromtimestamp((b['T']+1)/1000,dt.timezone(dt.timedelta(hours=8)))
        self.assertEqual((local.hour,local.minute),(8,0))

    def test_restart_signal_idempotency(self):
        events,b=self.fire();seed=self.store.state('TESTUSDT','1h')['seed']
        self.store.close();self.store=Store(self.path,self.config)
        again=self.store.ingest('TESTUSDT','1h',[b],'RECOVERED',b[6]+20000)
        self.assertEqual(again,[]);self.assertEqual(self.store.db.execute('SELECT count(*) FROM signals').fetchone()[0],1)
        self.assertEqual(self.store.state('TESTUSDT','1h')['seed'],seed)

    def test_historical_no_outbox(self):
        events,_=self.fire('HISTORICAL');self.assertEqual(events[0]['event_type'],'HISTORICAL')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0],0)

    def test_expiry_strict_boundary(self):
        events,_=self.fire('RECOVERED',120);self.assertEqual(events[0]['event_type'],'RECOVERED')
        self.store.refresh({'symbols':[info(),info('EXPIREDUSDT')]})
        rows=bars(200);self.store.ingest('EXPIREDUSDT','1h',rows,'HISTORICAL')
        b=signal_bar(rows[-1]);p=self.store.ingest('EXPIREDUSDT','1h',[b],'RECOVERED',b[6]+121001)[0]
        self.assertEqual(p['event_type'],'EXPIRED');self.assertEqual(p['origin'],'RECOVERED')

    def test_config_version_changes(self):
        other=dict(self.config,volume_multiple=2.1)
        self.assertNotEqual(version(other),version(self.config))

    def test_no_cooldown_distinct_bar(self):
        events,b=self.fire();b2=signal_bar(b,volume=20000)
        b2[4]=b2[1]*1.08;b2[2]=b2[4]+.01
        with patch('monitor.core.now_ms',return_value=b2[6]+10000):
            second=self.store.ingest('TESTUSDT','1h',[b2],'REALTIME',b2[6]+10000)
        self.assertEqual(len(second),1);self.assertNotEqual(events[0]['signal_id'],second[0]['signal_id'])

    def test_export_four_periods_and_unready(self):
        self.seed();books=snapshot(self.store,self.config)
        self.assertEqual(len(books),4);self.assertEqual({b['interval'] for b in books},set(PERIODS))
        self.assertTrue(all(len(b['overview'])==1 for b in books))
        self.assertEqual(books[0]['overview'][0]['readiness'],'NOT_READY')

    def test_partition_no_loss(self):
        events,_=self.fire();items=[dict(events[0],signal_id=str(n)) for n in range(13)]
        chunks=partitions(items,100)
        self.assertEqual([len(c[2]) for c in chunks],[6,6,1])
        self.assertEqual(len({p['signal_id'] for c in chunks for p in c[2]}),13)

    def test_locked_excel_preserves_old(self):
        target=Path(self.tmp.name)/'book.xlsx';temp=Path(self.tmp.name)/'temp.xlsx'
        target.write_bytes(b'old');temp.write_bytes(b'new');real_replace=os.replace
        def replace(a,b):
            if Path(b)==target:raise PermissionError('locked')
            real_replace(a,b)
        with patch('monitor.export.os.replace',side_effect=replace): actual,pending=safe_replace(temp,target)
        self.assertTrue(pending);self.assertEqual(target.read_bytes(),b'old');self.assertEqual(Path(actual).read_bytes(),b'new')

    def test_export_interruption_keeps_previous_file(self):
        from types import SimpleNamespace
        target=Path(self.tmp.name)/'币安USDT合约监控_15分钟.xlsx';target.write_bytes(b'old snapshot')
        with patch('monitor.export.subprocess.run',return_value=SimpleNamespace(returncode=1,stderr='interrupted')):
            with self.assertRaisesRegex(RuntimeError,'导出失败'):export_all(self.store,self.config)
        self.assertEqual(target.read_bytes(),b'old snapshot')

    def test_notification_failure_isolated(self):
        self.fire()
        self.store.db.execute('UPDATE outbox SET next_attempt=0');self.store.db.commit()
        class Failure:
            name='failure'
            async def send(self,*args):raise ConnectionError('private credential must not leak')
        asyncio.run(dispatch_once(self.store,self.config,None,[Failure()]))
        row=self.store.db.execute('SELECT status,error FROM outbox').fetchone()
        self.assertEqual(tuple(row),('RETRY','ConnectionError'))
        self.seed(205,'4h');self.assertEqual(self.store.state('TESTUSDT','4h')['count'],205)

    def test_interface_allowlist(self):
        self.assertEqual(ALLOWED,{'/fapi/v1/klines','/fapi/v1/time','/fapi/v1/exchangeInfo'})
        client=PublicClient(self.config)
        with self.assertRaisesRegex(ValueError,'allowlist'):asyncio.run(client.get('/fapi/v1/order'))

    def test_removal_prevents_signals(self):
        rows=self.seed();self.store.refresh({'symbols':[info('NEWUSDT')]})
        b=signal_bar(rows[-1]);events=self.store.ingest('TESTUSDT','1h',[b],'REALTIME',b[6]+1000)
        self.assertEqual(events,[])

if __name__=='__main__':unittest.main()
