"""Period-specific volume boundaries and compatibility of saved strategy records."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from monitor.core import (PERIODS, Store, dumps, metrics, parse_bar,
                          rules_for_interval, stored_volume_highlight,
                          validate_volume_rules, version)
from test_monitor import CONFIG, bars, info, signal_bar


class PeriodVolumeTests(unittest.TestCase):
    def result(self, interval, volume, window=None, config=None):
        rows = bars(15, interval)
        parsed = [parse_bar(row, interval, row[6]+1) for row in rows]
        target = signal_bar(rows[-1], interval, volume)
        bar = parse_bar(target, interval, target[6]+1)
        return metrics(bar, parsed if window is None else window,
                       [101., 102., 100.], [110., 110., 110.],
                       rules_for_interval(CONFIG if config is None else config, interval))

    def test_all_period_trigger_and_red_boundaries_are_inclusive(self):
        for interval, trigger, red in (('15m', 400., 500.), ('1h', 250., 400.),
                                        ('4h', 250., 400.), ('1d', 250., 400.)):
            for volume, triggered, highlighted in ((trigger-.001, False, False),
                    (trigger, True, trigger == red), (red-.001, True, False),
                    (red, True, True)):
                with self.subTest(interval=interval, volume=volume):
                    result = self.result(interval, volume)
                    self.assertEqual(result['checks'][2], triggered)
                    self.assertEqual(result['trigger'], triggered)
                    self.assertEqual(result['volume_highlight'], highlighted)
                    self.assertEqual(result['volume_threshold'], trigger)
                    self.assertEqual(result['volume_highlight_threshold'], red)
                    self.assertEqual(result['thresholds']['volume_multiple'], trigger/100.)
                    self.assertEqual(result['volume_highlight_multiple'], red/100.)
                    self.assertEqual(result['comparison_window'], 15)
                    self.assertEqual(result['checks'][:2]+result['checks'][3:], [True]*5)

    def test_exceeding_prior_max_is_strict_for_each_period(self):
        for interval in PERIODS:
            rows = bars(15, interval)
            window = [parse_bar(row, interval, row[6]+1) for row in rows]
            window[0]['v'] = 1000.
            equal = self.result(interval, 1000., window)
            self.assertGreater(1000., equal['volume_highlight_threshold'])
            self.assertFalse(equal['checks'][2], interval)
            self.assertFalse(equal['volume_highlight'], interval)
            above = self.result(interval, 1000.001, window)
            self.assertTrue(above['checks'][2], interval)
            self.assertTrue(above['volume_highlight'], interval)

    def test_period_override_does_not_change_any_other_period(self):
        changed = copy.deepcopy(CONFIG)
        changed['volume_multiple_by_interval']['1h'] = 3.
        changed['volume_highlight_multiple_by_interval']['1h'] = 4.5
        for interval in PERIODS:
            original = self.result(interval, 275.)
            adjusted = self.result(interval, 275., config=changed)
            if interval == '1h':
                self.assertTrue(original['trigger'])
                self.assertFalse(adjusted['trigger'])
            else:
                self.assertEqual(original, adjusted)
        self.assertNotEqual(version(CONFIG), version(changed))

    def test_invalid_period_threshold_policy_is_rejected(self):
        for key in ('volume_multiple_by_interval', 'volume_highlight_multiple_by_interval'):
            for invalid in ({'15m': 4.}, {**CONFIG[key], '5m': 4.}, [], None):
                candidate = copy.deepcopy(CONFIG)
                candidate[key] = invalid
                with self.subTest(key=key, invalid=invalid), self.assertRaises(ValueError):
                    validate_volume_rules(candidate)
            for invalid in (True, 0., -1., float('nan'), float('inf'), '4'):
                candidate = copy.deepcopy(CONFIG)
                candidate[key]['1h'] = invalid
                with self.subTest(key=key, invalid=invalid), self.assertRaises(ValueError):
                    validate_volume_rules(candidate)
        inverted = copy.deepcopy(CONFIG)
        inverted['volume_highlight_multiple_by_interval']['15m'] = 3.
        with self.assertRaises(ValueError):
            validate_volume_rules(inverted)

    def test_store_uses_actual_interval_and_persists_effective_threshold(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary)/'test.sqlite3', CONFIG)
            try:
                store.refresh({'symbols': [info()]})
                for interval in PERIODS:
                    rows = bars(200, interval)
                    store.ingest('TESTUSDT', interval, rows, 'HISTORICAL', rows[-1][6]+1)
                    prior = store.state('TESTUSDT', interval)
                    target = signal_bar(rows[-1], interval, 300.)
                    stamp = target[6]+1000
                    with patch('monitor.core.now_ms', return_value=stamp):
                        events = store.ingest('TESTUSDT', interval, [target], 'REALTIME', stamp)
                    state = store.state('TESTUSDT', interval)
                    payload = state['latest']
                    self.assertEqual(bool(events), interval != '15m')
                    self.assertEqual(payload['metrics']['thresholds'], rules_for_interval(CONFIG, interval))
                    self.assertTrue(payload['strategy_version'].startswith('v3-'))
                    self.assertEqual(payload['previous_ema'], prior['ema'])
                    self.assertEqual(state['seed'], prior['seed'])
                    self.assertEqual(state['count'], 201)
                    self.assertEqual(len(state['window']), 15)
                    for length, previous, actual in zip((12, 50, 200), prior['ema'], state['ema']):
                        expected = (2/(length+1))*target[4] + (1-2/(length+1))*previous
                        self.assertAlmostEqual(actual, expected, places=12)
            finally:
                store.close()

    def test_v2_saved_payload_and_existing_ema_survive_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'test.sqlite3'
            store = Store(path, CONFIG)
            try:
                store.refresh({'symbols': [info()]})
                rows = bars(200)
                store.ingest('TESTUSDT', '1h', rows, 'HISTORICAL', rows[-1][6]+1)
                previous = store.state('TESTUSDT', '1h')
                legacy = copy.deepcopy(previous['latest'])
                legacy.update(strategy_version='v2-fixture', signal_id='v2-signal', event_type='HISTORICAL')
                legacy['metrics']['thresholds']['volume_multiple'] = 3.
                legacy['metrics']['thresholds'].pop('volume_highlight_multiple', None)
                legacy['metrics']['volume_highlight'] = False
                for key in ('volume_highlight_multiple', 'volume_threshold', 'volume_highlight_threshold'):
                    legacy['metrics'].pop(key, None)
                saved = dumps(legacy)
                with store.db:
                    store.db.execute('INSERT INTO evaluations VALUES(?,?,?,?,?)',
                        ('v2-fixture', 'TESTUSDT', '1h', rows[-1][0], saved))
                    store.db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?,?)',
                        ('v2-signal', 'v2-fixture', 'TESTUSDT', '1h', rows[-1][0], 'HISTORICAL', saved))
                store.close()
                store = Store(path, CONFIG)
                # Replayed already processed bars must not evaluate under the new version.
                self.assertEqual(store.ingest('TESTUSDT', '1h', [rows[-1]], 'RECOVERED', rows[-1][6]+1), [])
                target = signal_bar(rows[-1], volume=250.)
                stamp = target[6]+1000
                with patch('monitor.core.now_ms', return_value=stamp):
                    events = store.ingest('TESTUSDT', '1h', [target], 'REALTIME', stamp)
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0]['previous_ema'], previous['ema'])
                for table in ('evaluations', 'signals'):
                    actual = store.db.execute(f'SELECT payload FROM {table} WHERE version=?', ('v2-fixture',)).fetchone()[0]
                    self.assertEqual(actual, saved)
                self.assertEqual(events[0]['metrics']['thresholds']['volume_multiple'], 2.5)
                self.assertFalse(stored_volume_highlight(json.loads(saved)))
            finally:
                store.close()

    def test_legacy_and_new_highlight_use_their_saved_rules(self):
        payload = {'bar': {'v': 450.}, 'interval': '1h', 'strategy_version': 'v2-test',
                   'metrics': {'V15_mean': 100., 'V15_max': 200.}}
        self.assertFalse(stored_volume_highlight(payload))
        new = copy.deepcopy(payload)
        new['strategy_version'] = 'v3-test'
        new['metrics']['volume_highlight_multiple'] = 4.
        self.assertTrue(stored_volume_highlight(new))
        new['metrics']['volume_highlight'] = False
        self.assertFalse(stored_volume_highlight(new))
        legacy = copy.deepcopy(new)
        legacy['comparison_window'] = 20
        legacy['metrics']['volume_highlight'] = True
        self.assertFalse(stored_volume_highlight(legacy))


if __name__ == '__main__':
    unittest.main()
