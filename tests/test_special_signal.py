"""Independent special-route boundaries, safety gates and durable reminders."""
import copy
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from monitor.core import (PERIODS, Store, metrics, parse_bar, rules_for_interval,
                          validate_volume_rules)
from test_monitor import BASE, CONFIG, info


def history(interval='1h', count=15):
    step=PERIODS[interval]
    return [[BASE+n*step,128.,130.,127.,130.,100.,BASE+(n+1)*step-1,13000.]
            for n in range(count)]


def target(previous, interval='1h', volume=1200., close=116.):
    start=previous[0]+PERIODS[interval]
    return [start,128.,145.,110.,close,volume,start+PERIODS[interval]-1,volume*close]


class SpecialMetricsTests(unittest.TestCase):
    def result(self, interval='1h', volume=1200., close=116., raw=None, window=None, rules=None):
        raw=raw or history(interval)
        w=window if window is not None else [parse_bar(b,interval,b[6]+1) for b in raw]
        bar=target(raw[-1],interval,volume,close)
        return metrics(parse_bar(bar,interval,bar[6]+1),w,[140.,141.,142.],
                       [140.,141.,142.],rules or rules_for_interval(CONFIG,interval))

    def test_exact_12_and_6_bearish_no_ema_breakout_or_shape_all_periods(self):
        for interval in PERIODS:
            with self.subTest(interval=interval):
                m=self.result(interval)
                self.assertEqual(m['volume_ratio'],12.)
                self.assertEqual(m['relative_body_ratio'],6.)
                self.assertFalse(m['checks'][0])
                self.assertFalse(m['checks'][1])
                self.assertFalse(m['checks'][4])
                self.assertFalse(m['checks'][5])
                self.assertFalse(m['standard_trigger'])
                self.assertTrue(m['special_trigger'])
                self.assertTrue(m['trigger'])
                self.assertEqual(m['trigger_path'],'SPECIAL')

    def test_both_special_thresholds_required_inclusive_and_above(self):
        for volume,close,expected in ((1199.999,116.,False),(1200.,116.000001,False),
                                     (1200.,116.,True),(1300.,115.,True)):
            with self.subTest(volume=volume,close=close):
                self.assertEqual(self.result(volume=volume,close=close)['trigger'],expected)

    def test_special_does_not_require_volume_max_and_does_not_change_red_rule(self):
        rows=history();rows[0][5]=1500.
        for b in rows[1:]:b[5]=0.
        m=self.result(raw=rows)
        self.assertEqual(m['V15_mean'],100.)
        self.assertEqual(m['V15_max'],1500.)
        self.assertFalse(m['checks'][2])
        self.assertTrue(m['special_trigger'])
        self.assertFalse(m['volume_highlight'])

    def test_missing_window_zero_mean_and_doji_never_special(self):
        for size in (0,14,16):
            window=[parse_bar(b,'1h',b[6]+1) for b in history(count=size)]
            self.assertFalse(self.result(window=window)['trigger'])
        for field in ('volume','body'):
            rows=history()
            for b in rows:
                if field=='volume':b[5]=0.
                else:b[4]=b[1]
            self.assertFalse(self.result(raw=rows)['special_trigger'])
        self.assertFalse(self.result(close=128.)['special_trigger'])

    def test_standard_and_special_are_or_routes(self):
        rows=history();w=[parse_bar(b,'1h',b[6]+1) for b in rows]
        for volume,path in ((400.,'STANDARD'),(1200.,'BOTH')):
            bar=target(rows[-1],volume=volume,close=140.);bar[2]=140.
            m=metrics(parse_bar(bar,'1h',bar[6]+1),w,[100.,100.,100.],
                      [100.,100.,100.],rules_for_interval(CONFIG,'1h'))
            self.assertEqual(m['trigger_path'],path)
            self.assertTrue(m['trigger'])

    def test_body_59_upper_41_inclusive_boundary_all_periods(self):
        for interval in PERIODS:
            rows=history(interval);w=[parse_bar(b,interval,b[6]+1) for b in rows]
            rule=rules_for_interval(CONFIG,interval)
            self.assertEqual(rule['max_upper_wick_share'],.41)
            self.assertEqual(rule['min_body_share'],.59)
            for close,high,expected in ((159.,200.,True),(159.,200.00001,False),
                    (158.99999,200.,False),(160.,200.,True)):
                with self.subTest(interval=interval,close=close,high=high):
                    bar=dict(t=BASE,T=BASE+PERIODS[interval]-1,o=100.,c=close,h=high,l=90.,v=400.,q=1.)
                    m=metrics(bar,w,[100.]*3,[100.]*3,rule)
                    self.assertEqual(m['checks'][5],expected)
                    self.assertEqual(m['standard_trigger'],expected)
                    self.assertFalse(m['special_trigger'])
                    self.assertEqual(m['trigger'],expected)

    def test_special_configuration_rejects_nonfinite_or_nonpositive(self):
        for key in ('special_volume_multiple','special_body_multiple'):
            for value in (True,0,-1,float('nan'),float('inf'),'12'):
                with self.subTest(key=key,value=value),self.assertRaises(ValueError):
                    validate_volume_rules(dict(CONFIG,**{key:value}))


class SpecialPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.config=dict(CONFIG,data_dir=self.temp.name,excel_dir=self.temp.name)
        self.store=Store(Path(self.temp.name)/'monitor.sqlite3',self.config)
        self.store.refresh({'symbols':[info()]})

    def tearDown(self):
        self.store.close();self.temp.cleanup()

    def ingest_target(self, interval='1h', count=15, origin='REALTIME', age=1000, volume=1200., close=116.):
        rows=history(interval,count)
        self.store.ingest('TESTUSDT',interval,rows,'HISTORICAL',rows[-1][6]+1)
        bar=target(rows[-1],interval,volume,close);stamp=bar[6]+1+age
        with patch('monitor.core.now_ms',return_value=stamp):
            events=self.store.ingest('TESTUSDT',interval,[bar],origin,stamp)
        return events,bar,stamp

    def test_special_can_remind_at_sixteenth_bar_all_periods(self):
        for interval in PERIODS:
            events,bar,stamp=self.ingest_target(interval)
            self.assertEqual(len(events),1,interval)
            signal=events[0]
            self.assertEqual(signal['count'],16)
            self.assertEqual(signal['readiness'],'NOT_READY')
            self.assertEqual(signal['metrics']['trigger_path'],'SPECIAL')
            self.assertEqual(len(signal['previous15']),15)
            self.assertEqual(signal['event_type'],'REALTIME')
            self.assertEqual(self.store.db.execute('SELECT status FROM outbox WHERE id=?',
                (signal['signal_id'],)).fetchone()[0],'PENDING')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM signals').fetchone()[0],4)

    def test_fourteen_previous_bars_not_enough(self):
        events,_,_=self.ingest_target(count=14)
        self.assertEqual(events,[])

    def test_standard_still_requires_200_bars(self):
        rows=history();self.store.ingest('TESTUSDT','1h',rows,'HISTORICAL',rows[-1][6]+1)
        bar=target(rows[-1],volume=400.,close=140.);bar[2]=140.;stamp=bar[6]+1000
        with patch('monitor.core.now_ms',return_value=stamp):
            events=self.store.ingest('TESTUSDT','1h',[bar],'REALTIME',stamp)
        self.assertEqual(events,[])
        self.assertTrue(all(self.store.state('TESTUSDT','1h')['latest']['metrics']['checks']))

    def test_both_routes_and_replayed_bar_produce_only_one_reminder(self):
        rows=history(count=200);self.store.ingest('TESTUSDT','1h',rows,'HISTORICAL',rows[-1][6]+1)
        bar=target(rows[-1],close=140.);bar[2]=140.;stamp=bar[6]+1000
        with patch('monitor.core.now_ms',return_value=stamp):
            events=self.store.ingest('TESTUSDT','1h',[bar],'REALTIME',stamp)
            again=self.store.ingest('TESTUSDT','1h',[bar],'RECOVERED',stamp)
        self.assertEqual(len(events),1);self.assertEqual(again,[])
        self.assertEqual(events[0]['metrics']['trigger_path'],'BOTH')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0],1)

    def test_historical_and_expired_special_signals_keep_notification_policy(self):
        for interval,origin,age,event_type in (('15m','HISTORICAL',1000,'HISTORICAL'),
                ('1h','RECOVERED',120000,'RECOVERED'),('4h','REALTIME',120001,'EXPIRED')):
            events,_,_=self.ingest_target(interval,origin=origin,age=age)
            self.assertEqual(events[0]['event_type'],event_type)
            expected=int(event_type=='RECOVERED')
            self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox WHERE id=?',
                (events[0]['signal_id'],)).fetchone()[0],expected)

    def test_inactive_special_signal_and_unclosed_input_rejected(self):
        rows=history();self.store.ingest('TESTUSDT','1h',rows,'HISTORICAL',rows[-1][6]+1)
        bar=target(rows[-1])
        with self.assertRaisesRegex(ValueError,'UNCLOSED'):
            self.store.ingest('TESTUSDT','1h',[bar],'REALTIME',bar[6]-1)
        self.store.refresh({'symbols':[info('OTHERUSDT')]})
        stamp=bar[6]+1000
        with patch('monitor.core.now_ms',return_value=stamp):
            self.assertEqual(self.store.ingest('TESTUSDT','1h',[bar],'REALTIME',stamp),[])


if __name__=='__main__':unittest.main()
