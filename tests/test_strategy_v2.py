"""Regressions for the five requested rules; all persistence uses temporary files."""
import copy
import json
import statistics
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from monitor.core import DEFAULT_RULES, PERIODS, Store, dumps, eligible, metrics, parse_bar, version
from monitor.service import health_snapshot

ROOT = Path(__file__).resolve().parents[1]
CONFIG = json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))
BASE = 1704067200000


def market(symbol='TESTUSDT', contract_type='PERPETUAL', **overrides):
    return dict(symbol=symbol, quoteAsset='USDT', marginAsset='USDT',
                status='TRADING', contractType=contract_type) | overrides


def history(count=200, interval='1h'):
    step = PERIODS[interval]
    return [[BASE+n*step, 100+n*.1, 100.07+n*.1, 99.99+n*.1,
             100.05+n*.1, 100., BASE+(n+1)*step-1, 10000.]
            for n in range(count)]


def candidate(previous, interval='1h', volume=1000.):
    start = previous[0] + PERIODS[interval]
    opened = previous[4]
    closed = opened * 1.04
    return [start, opened, closed+.01, opened-.01, closed, volume,
            start+PERIODS[interval]-1, volume*closed]


def parsed(rows, interval='1h'):
    return [parse_bar(row, interval, row[6]+1) for row in rows]


class RuleV2Tests(unittest.TestCase):
    def setUp(self):
        self.window = parsed(history(15))
        self.bar = dict(t=BASE, T=BASE+PERIODS['1h']-1,
                        o=100., h=112., l=99., c=110., v=300., q=33000.)

    def calculate(self, bar=None, ema=None, previous_ema=None, window=None, rules=None):
        return metrics(self.bar if bar is None else bar,
                       self.window if window is None else window,
                       [105., 106., 104.] if ema is None else ema,
                       [106., 107., 103.] if previous_ema is None else previous_ema,
                       DEFAULT_RULES if rules is None else rules)

    def test_default_thresholds_and_new_formula_version(self):
        self.assertEqual(DEFAULT_RULES, dict(volume_multiple=3., body_multiple=2.,
                                            min_body_share=.6, max_upper_wick_share=.3))
        self.assertEqual(CONFIG['volume_multiple'], 3.)
        self.assertTrue(version(CONFIG).startswith('v2-'))

    def test_nonbullish_ema_and_falling_ema12_still_trigger(self):
        result = self.calculate()
        self.assertFalse(result['ema_bullish'])
        self.assertTrue(result['checks'][1])
        self.assertTrue(result['trigger'])
        self.assertEqual(result['comparison_window'], 15)

    def test_equal_emas_allowed_but_close_must_exceed_each_ema(self):
        self.assertTrue(self.calculate(ema=[105., 105., 105.])['checks'][1])
        for index in range(3):
            for level in (self.bar['c'], self.bar['c']+.001):
                with self.subTest(index=index, level=level):
                    ema = [105., 105., 105.]
                    ema[index] = level
                    self.assertFalse(self.calculate(ema=ema)['checks'][1])

    def test_lower_wick_cannot_change_shape_or_trigger(self):
        short_wick = self.calculate()
        deep_wick = self.calculate(bar=dict(self.bar, l=1.))
        self.assertEqual(short_wick, deep_wick)
        self.assertAlmostEqual(short_wick['body_share'], 10/12)
        self.assertAlmostEqual(short_wick['upper_wick_share'], 2/12)
        self.assertTrue(short_wick['trigger'])

    def test_shape_thresholds_remain_inclusive(self):
        boundary = dict(self.bar, c=107., h=110., l=1.)
        result = self.calculate(bar=boundary)
        self.assertEqual(result['body_share'], .7)
        self.assertEqual(result['upper_wick_share'], .3)
        self.assertTrue(result['checks'][5])
        self.assertFalse(self.calculate(bar=dict(boundary, h=110.000001))['checks'][5])
        self.assertFalse(self.calculate(bar=boundary,
                                       rules=dict(DEFAULT_RULES, min_body_share=.700001))['checks'][5])
        self.assertFalse(self.calculate(bar=boundary,
                                       rules=dict(DEFAULT_RULES, max_upper_wick_share=.299999))['checks'][5])

    def test_lower_wick_only_is_zero_denominator(self):
        result = self.calculate(bar=dict(self.bar, o=100., c=100., h=100., l=1.))
        self.assertIsNone(result['body_share'])
        self.assertIsNone(result['upper_wick_share'])
        self.assertFalse(result['available'])
        self.assertFalse(result['trigger'])
        self.assertEqual(result['reason'], 'ZERO_DENOMINATOR')

    def test_three_and_five_times_volume_boundaries(self):
        for volume, triggered, highlighted in (
                (299.999, False, False), (300., True, False),
                (499.999, True, False), (500., True, True)):
            with self.subTest(volume=volume):
                result = self.calculate(bar=dict(self.bar, v=volume))
                self.assertEqual(result['checks'][2], triggered)
                self.assertEqual(result['trigger'], triggered)
                self.assertEqual(result['volume_highlight'], highlighted)

    def test_volume_maximum_is_strict_for_trigger_and_highlight(self):
        window = copy.deepcopy(self.window)
        window[0]['v'] = 1000.
        equal = self.calculate(bar=dict(self.bar, v=1000.), window=window)
        self.assertGreater(1000., 5*equal['V15_mean'])
        self.assertFalse(equal['checks'][2])
        self.assertFalse(equal['volume_highlight'])
        greater = self.calculate(bar=dict(self.bar, v=1000.001), window=window)
        self.assertTrue(greater['checks'][2])
        self.assertTrue(greater['volume_highlight'])

    def test_highlight_does_not_require_other_signal_conditions(self):
        result = self.calculate(bar=dict(self.bar, v=500.), ema=[111., 106., 104.])
        self.assertFalse(result['trigger'])
        self.assertTrue(result['volume_highlight'])

    def test_only_fifteen_completed_comparison_bars_are_accepted(self):
        for size in (0, 14, 16, 20):
            with self.subTest(size=size):
                result = self.calculate(window=parsed(history(size)))
                self.assertFalse(result['trigger'])
                self.assertEqual(result['reason'], 'WINDOW_NOT_READY')
        self.assertTrue(self.calculate()['trigger'])

    def test_universe_requires_usdt_trading_perpetual(self):
        self.assertTrue(eligible(market()))
        for value in ('CURRENT_QUARTER', 'NEXT_QUARTER', 'PERPETUAL_DELIVERING', ''):
            with self.subTest(contract_type=value):
                self.assertFalse(eligible(market(contract_type=value)))
        for key, value in (('quoteAsset', 'USDC'), ('marginAsset', 'BTC'),
                           ('status', 'SETTLING')):
            with self.subTest(field=key):
                self.assertFalse(eligible(market(**{key: value})))


class PersistenceV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'test.sqlite3'
        self.config = dict(CONFIG, data_dir=self.temp.name, excel_dir=self.temp.name)
        self.store = Store(self.path, self.config)
        self.store.refresh({'symbols': [market()]})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_each_period_excludes_the_sixteenth_bar_and_target(self):
        for interval in PERIODS:
            with self.subTest(interval=interval):
                rows = history(interval=interval)
                # The sixteenth preceding bar has extreme volume, return and high.
                rows[-16][1:6] = [1., 1000000., .5, 10000., 1000000.]
                self.store.ingest('TESTUSDT', interval, rows, 'HISTORICAL',
                                  rows[-1][6]+1)
                target = candidate(rows[-1], interval, volume=300.)
                at = target[6]+10
                with patch('monitor.core.now_ms', return_value=at):
                    self.store.ingest('TESTUSDT', interval, [target], 'REALTIME', at)
                payload = self.store.state('TESTUSDT', interval)['latest']
                result = payload['metrics']
                expected = rows[-15:]
                returns = [r[4]/r[1]-1 for r in expected]
                self.assertEqual(payload['comparison_window'], 15)
                self.assertEqual(result['V15_mean'], 100.)
                self.assertEqual(result['V15_max'], 100.)
                self.assertEqual(result['H15'], max(r[2] for r in expected))
                self.assertEqual(result['G15_max'], max(returns))
                self.assertAlmostEqual(result['B15_mean'],
                                       statistics.fmean(map(abs, returns)))
                self.assertEqual(result['volume_rank'], 1)
                self.assertTrue(result['checks'][2])

    def test_legacy_twenty_bar_state_resumes_without_reseeding_or_rewriting_history(self):
        rows = history()
        self.store.ingest('TESTUSDT', '1h', rows, 'HISTORICAL', rows[-1][6]+1)
        state = self.store.state('TESTUSDT', '1h')
        original_ema = list(state['ema'])
        original_seed = state['seed']
        old_window = parsed(rows[-20:])
        state['window'] = old_window
        state['before_window_close'] = rows[-21][4]
        legacy_signal = dict(state['latest'], strategy_version='v1-legacy-fixture',
                             signal_id='legacy-signal', previous20=old_window,
                             previous20_before_close=rows[-21][4], event_type='HISTORICAL')
        legacy_signal.pop('comparison_window', None)
        legacy_signal['metrics'] = {'V20_mean': 100., 'checks': [True]*6, 'trigger': True}
        legacy_json = dumps(legacy_signal)
        with self.store.db:
            self.store.db.execute('UPDATE states SET state=? WHERE symbol=? AND interval=?',
                                  (dumps(state), 'TESTUSDT', '1h'))
            self.store.db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?,?)',
                                  ('legacy-signal', 'v1-legacy-fixture', 'TESTUSDT',
                                   '1h', rows[-1][0], 'HISTORICAL', legacy_json))
        self.store.close()
        self.store = Store(self.path, self.config)
        target = candidate(rows[-1])
        at = target[6]+10
        with patch('monitor.core.now_ms', return_value=at):
            signals = self.store.ingest('TESTUSDT', '1h', [target], 'REALTIME', at)
        self.assertEqual(len(signals), 1)
        signal = signals[0]
        self.assertEqual(signal['previous15'], old_window[-15:])
        self.assertEqual(signal['previous15_before_close'], rows[-16][4])
        self.assertNotIn('previous20', signal)
        self.assertEqual(signal['previous_ema'], original_ema)
        new_state = self.store.state('TESTUSDT', '1h')
        self.assertEqual(new_state['count'], 201)
        self.assertEqual(new_state['seed'], original_seed)
        self.assertEqual(len(new_state['window']), 15)
        self.assertEqual(new_state['before_window_close'], rows[-15][4])
        for length, old, actual in zip((12, 50, 200), original_ema, new_state['ema']):
            expected = (2/(length+1))*target[4] + (1-2/(length+1))*old
            self.assertAlmostEqual(actual, expected, places=12)
        saved = self.store.db.execute('SELECT payload FROM signals WHERE id=?',
                                      ('legacy-signal',)).fetchone()[0]
        self.assertEqual(saved, legacy_json)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM signals').fetchone()[0], 2)

    def test_stale_delivery_universe_entry_cannot_trigger_before_refresh(self):
        rows = history()
        self.store.ingest('TESTUSDT', '1h', rows, 'HISTORICAL', rows[-1][6]+1)
        # Simulate a delivery contract persisted as active by the old version.
        with self.store.db:
            self.store.db.execute('UPDATE universe SET info=? WHERE symbol=?',
                                  (dumps(market(contract_type='CURRENT_QUARTER')), 'TESTUSDT'))
        self.assertNotIn('TESTUSDT', self.store.universe())
        target = candidate(rows[-1])
        at = target[6]+10
        with patch('monitor.core.now_ms', return_value=at):
            signals = self.store.ingest('TESTUSDT', '1h', [target], 'REALTIME', at)
        self.assertEqual(signals, [])
        self.assertFalse(self.store.state('TESTUSDT', '1h')['latest']['metrics']['trigger'])

    def test_health_counts_exclude_legacy_active_delivery_states(self):
        rows = history()
        self.store.ingest('TESTUSDT', '1h', rows, 'HISTORICAL', rows[-1][6]+1)
        delivery = market('DELIVERYUSDT_261225', contract_type='CURRENT_QUARTER')
        with self.store.db:
            self.store.db.execute('INSERT INTO universe VALUES(?,?,?,?,?)',
                                  (delivery['symbol'], 1, dumps(delivery), '', BASE))
            for interval in PERIODS:
                self.store.db.execute('INSERT INTO states VALUES(?,?,?)',
                                      (delivery['symbol'], interval,
                                       dumps(dict(readiness='READY', health='OK',
                                                  count=1500, last=BASE))))
        self.assertEqual(set(self.store.universe()), {'TESTUSDT'})
        report = health_snapshot(self.store)
        self.assertEqual(report['universe'], 1)
        for interval, counts in report['periods'].items():
            with self.subTest(interval=interval):
                self.assertEqual(counts['expected'], 1)
                self.assertEqual(counts['READY'], 0)
                self.assertEqual(counts['SHORT_WARMUP'], int(interval == '1h'))
                self.assertEqual(counts['MISSING'], int(interval != '1h'))
                self.assertGreaterEqual(counts['MISSING'], 0)


if __name__ == '__main__':
    unittest.main()
