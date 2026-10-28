import copy
import unittest

from monitor.core import DEFAULT_RULES, metrics, now_ms, parse_bar
from monitor.notifications import alert
from test_monitor import bars, signal_bar


class NotificationRulesTests(unittest.TestCase):
    def payload(self):
        raw = bars(15)
        target = parse_bar(signal_bar(raw[-1]), '1h', now_ms())
        window = [parse_bar(row, '1h', now_ms()) for row in raw]
        ema = [101., 102., 100.]
        computed = metrics(target, window, ema, [110., 110., 110.], DEFAULT_RULES)
        self.assertTrue(computed['trigger'])
        self.assertFalse(computed['ema_bullish'])
        return dict(bar=target, metrics=computed, ema=ema, signal_id='new-signal',
                    strategy_version='v2-test', symbol='TESTUSDT',
                    contract_type='PERPETUAL', interval='1h', comparison_window=15,
                    discovered=target['T']+1001, event_type='REALTIME', origin='REALTIME',
                    readiness='SHORT_WARMUP', latency_ms=1000)

    def test_new_alert_accepts_non_bullish_ema_and_uses_15_bar_statistics(self):
        payload = self.payload()
        message = alert(payload)
        self.assertEqual(message['comparison_window'], 15)
        self.assertEqual(message['V15_mean'], payload['metrics']['V15_mean'])
        self.assertEqual(message['V15_max'], payload['metrics']['V15_max'])
        self.assertEqual(message['previous_max_return'], payload['metrics']['G15_max'])
        self.assertNotIn('V20_mean', message)
        self.assertEqual(message['EMA条件'], '收盘价 > EMA12、EMA50、EMA200：满足')

    def test_special_bearish_alert_does_not_claim_ema_passed(self):
        payload = self.payload()
        raw = bars(15)
        for index, row in enumerate(raw):
            row[1:6] = [128., 140., 127., 129., 1400. if index == 0 else 1.]
        window = [parse_bar(row, '1h', now_ms()) for row in raw]
        target = dict(payload['bar'], o=128., c=120., h=200., l=100., v=1200., q=144000.)
        payload.update(bar=target, ema=[125., 126., 127.], readiness='NOT_READY')
        payload['metrics'] = metrics(target, window, payload['ema'], payload['ema'], DEFAULT_RULES)
        computed = payload['metrics']
        self.assertEqual(computed['trigger_path'], 'SPECIAL')
        self.assertFalse(computed['checks'][0])
        self.assertFalse(computed['checks'][1])
        self.assertFalse(computed['checks'][2])
        self.assertFalse(computed['checks'][5])
        original = copy.deepcopy(payload)
        message = alert(payload)
        self.assertEqual(message['trigger_path'], 'SPECIAL')
        self.assertEqual(message['触发依据'], '特别条件')
        self.assertIn('未满足', message['EMA条件'])
        self.assertIn('特别条件不要求', message['EMA条件'])
        self.assertEqual(message['readiness'], 'NOT_READY')
        self.assertEqual(message['checks'], computed['checks'])
        self.assertLess(message['body'], 0)
        self.assertEqual(payload, original)

    def test_both_paths_and_legacy_alert_basis_are_explicit(self):
        payload = self.payload()
        payload['metrics'].update(standard_trigger=True, special_trigger=True, trigger_path='BOTH')
        self.assertEqual(alert(payload)['触发依据'], '普通条件＋特别条件')
        for key in ('standard_trigger', 'special_trigger', 'trigger_path'):
            payload['metrics'].pop(key, None)
        original = copy.deepcopy(payload)
        message = alert(payload)
        self.assertEqual(message['trigger_path'], 'STANDARD')
        self.assertEqual(message['触发依据'], '普通条件')
        self.assertEqual(payload, original)

    def test_pending_legacy_alert_keeps_20_bar_statistics(self):
        payload = copy.deepcopy(self.payload())
        payload.pop('comparison_window')
        payload['strategy_version'] = 'v1-test'
        payload['metrics'] = {
            key.replace('15', '20'): value for key, value in payload['metrics'].items()
            if key not in ('comparison_window', 'volume_highlight')
        }
        message = alert(payload)
        self.assertEqual(message['comparison_window'], 20)
        self.assertEqual(message['V20_mean'], payload['metrics']['V20_mean'])
        self.assertEqual(message['V20_max'], payload['metrics']['V20_max'])
        self.assertNotIn('V15_mean', message)
        self.assertIn('EMA12上升', message['EMA条件'])


if __name__ == '__main__':
    unittest.main()
