"""Calendar report rotation against temporary stores/files; no Node or network."""
import copy
import datetime as dt
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from monitor.archive import ARCHIVE_DIR, report_period, report_timestamp
from monitor.core import PERIODS, Store
from monitor.export import LABELS, export_all, snapshot
from test_monitor import CONFIG, bars, info, signal_bar


BEIJING = dt.timezone(dt.timedelta(hours=8))


def stamp(text):
    return int(dt.datetime.fromisoformat(text).replace(tzinfo=BEIJING).timestamp() * 1000)


class ReportCalendarTests(unittest.TestCase):
    def test_daily_weekly_biweekly_and_monthly_boundaries(self):
        cases = [
            ('15m', '2026-10-21 08:00', '2026-10-22 08:00'),
            ('1h', '2026-10-19 08:00', '2026-10-26 08:00'),
            ('4h', '2026-10-19 08:00', '2026-11-02 08:00'),
            ('1d', '2026-10-01 08:00', '2026-11-01 08:00'),
        ]
        for interval, begin, finish in cases:
            with self.subTest(interval=interval):
                start, end = stamp(begin), stamp(finish)
                self.assertEqual(report_period(interval, start), (start, end))
                self.assertEqual(report_period(interval, end - 1), (start, end))
                self.assertEqual(report_period(interval, start - 1)[1], start)
                self.assertEqual(report_period(interval, end)[0], end)

    def test_biweekly_anchor_before_and_after_november_second(self):
        expected = [
            ('2026-10-19 07:59:59.999', '2026-10-05 08:00', '2026-10-19 08:00'),
            ('2026-11-02 07:59:59.999', '2026-10-19 08:00', '2026-11-02 08:00'),
            ('2026-11-02 08:00', '2026-11-02 08:00', '2026-11-16 08:00'),
        ]
        for at, begin, finish in expected:
            self.assertEqual(report_period('4h', stamp(at)), (stamp(begin), stamp(finish)))

    def test_month_rollover_handles_year_and_leap_day(self):
        for at, begin, finish in [
            ('2026-12-31 23:00', '2026-12-01 08:00', '2027-01-01 08:00'),
            ('2027-01-01 07:59:59.999', '2026-12-01 08:00', '2027-01-01 08:00'),
            ('2027-01-01 08:00', '2027-01-01 08:00', '2027-02-01 08:00'),
            ('2028-02-29 12:00', '2028-02-01 08:00', '2028-03-01 08:00'),
        ]:
            self.assertEqual(report_period('1d', stamp(at)), (stamp(begin), stamp(finish)))

    def test_timestamp_uses_discovery_with_legacy_fallbacks(self):
        self.assertEqual(report_timestamp({'discovered': 1000, 'processed': 2000, 'bar': {'T': 2999}}), 1000)
        self.assertEqual(report_timestamp({'processed': 2000, 'bar': {'T': 2999}}), 2000)
        self.assertEqual(report_timestamp({'bar': {'T': 2999}}), 3000)


class ReportPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = dict(CONFIG, data_dir=str(self.root / 'data'), excel_dir=str(self.root / 'excel'))
        Path(self.config['excel_dir']).mkdir()
        self.store = Store(self.root / 'data' / 'monitor.sqlite3', self.config)
        self.symbols = []
        self.built_batches = []

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def main_path(self, interval='15m'):
        return Path(self.config['excel_dir']) / f'币安USDT合约监控_{LABELS[interval]}.xlsx'

    def history_files(self):
        return sorted((Path(self.config['excel_dir']) / ARCHIVE_DIR).glob('*.xlsx'))

    def fake_builder(self, command, **kwargs):
        spec = json.loads(Path(command[-2]).read_text(encoding='utf-8'))
        self.built_batches.append(copy.deepcopy(spec['books']))
        for book in spec['books']:
            target = Path(command[-1]) / book['filename']
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(book, ensure_ascii=False, sort_keys=True), encoding='utf-8')
        return SimpleNamespace(returncode=0, stderr='')

    def publish(self, at, interval='15m', reason='CANDLE_CLOSE'):
        with patch('monitor.export.now_ms', return_value=at), \
             patch('monitor.export.subprocess.run', side_effect=self.fake_builder):
            return export_all(self.store, self.config, intervals=[interval], refresh_reason=reason,
                              refresh_boundary=at // PERIODS[interval] * PERIODS[interval])

    def add_signal(self, discovered, interval='15m', candle_close=None, origin='REALTIME'):
        symbol = f'TEST{len(self.symbols)}USDT'
        self.symbols.append(symbol)
        self.store.refresh({'symbols': [info(s) for s in self.symbols]})
        step = PERIODS[interval]
        close = candle_close if candle_close is not None else discovered // step * step
        rows = bars(200, interval, start=close - 201 * step)
        with patch('monitor.core.now_ms', return_value=discovered):
            self.store.ingest(symbol, interval, rows, 'HISTORICAL', discovered)
            events = self.store.ingest(symbol, interval, [signal_bar(rows[-1], interval)], origin, discovered)
        self.assertEqual(len(events), 1)
        return events[0]

    def read_current(self, interval='15m'):
        return json.loads(self.main_path(interval).read_text(encoding='utf-8'))

    def test_discovery_at_eight_enters_new_period_even_for_old_candle(self):
        boundary = stamp('2026-10-22 08:00')
        before = self.add_signal(boundary - 1)
        exact = self.add_signal(boundary, candle_close=boundary)
        recovered = self.add_signal(boundary + 1000, candle_close=boundary - PERIODS['15m'], origin='RECOVERED')
        with patch('monitor.export.now_ms', return_value=boundary + 2000):
            books = snapshot(self.store, self.config, ['15m'])
        current = [book for book in books if not book.get('archive')]
        records = [r for book in current for r in book['triggers']]
        self.assertEqual({r['signal_id'] for r in records}, {exact['signal_id'], recovered['signal_id']})
        self.assertNotIn(before['signal_id'], {r['signal_id'] for r in records})
        self.assertTrue(all(book['report_period_start'] == boundary for book in current))
        restored = next(r for r in records if r['signal_id'] == recovered['signal_id'])
        self.assertEqual(restored['close_boundary'], boundary - PERIODS['15m'])
        self.assertEqual(restored['event_type'], 'EXPIRED')

    def test_snapshot_period_is_now_not_stale_refresh_boundary(self):
        boundary = stamp('2026-10-22 08:00')
        self.add_signal(boundary + 1000)
        with patch('monitor.export.now_ms', return_value=boundary + 2000):
            books = snapshot(self.store, self.config, ['15m'], 'CANDLE_CLOSE', boundary - PERIODS['15m'])
        self.assertEqual(books[0]['report_period_start'], boundary)
        self.assertEqual(len(books[0]['triggers']), 1)

    def test_first_publish_preserves_legacy_bytes_and_other_intervals(self):
        at = stamp('2026-10-21 12:00')
        previous_bytes = b'previous cumulative workbook, preserve exactly'
        self.main_path().write_bytes(previous_bytes)
        other = self.main_path('1h')
        other.write_bytes(b'other interval must stay untouched')
        self.add_signal(at + 1000)
        outputs = self.publish(at + 2000)
        self.assertEqual(outputs, [str(self.main_path())])
        self.assertIn(previous_bytes, [p.read_bytes() for p in self.history_files()])
        self.assertEqual(other.read_bytes(), b'other interval must stay untouched')
        state = self.store.get_meta('excel_archive_15m')
        self.assertEqual(state['first_start'], stamp('2026-10-21 08:00'))
        self.assertEqual(state['current_start'], state['first_start'])

    def test_first_migration_does_not_repartition_old_signals(self):
        at = stamp('2026-10-21 12:00')
        old = self.add_signal(stamp('2026-10-20 12:00'))
        self.main_path().write_bytes(b'legacy contains old signals')
        self.publish(at)
        generated = [b for batch in self.built_batches for b in batch]
        self.assertFalse(any(b.get('archive') for b in generated))
        self.assertNotIn(old['signal_id'], {r['signal_id'] for b in generated for r in b['triggers']})
        self.assertIn(b'legacy contains old signals', [p.read_bytes() for p in self.history_files()])

    def test_next_period_contains_only_new_records_and_archive_has_previous(self):
        first = stamp('2026-10-21 12:00')
        previous = self.add_signal(first + 1000)
        self.publish(first + 2000)
        second = stamp('2026-10-22 08:00')
        current = self.add_signal(second + 1000)
        self.publish(second + 2000)
        book = self.read_current()
        self.assertEqual([r['signal_id'] for r in book['triggers']], [current['signal_id']])
        archived_records = [r for b in self.built_batches[-1] if b.get('archive') for r in b['triggers']]
        # An unchanged closed period may be moved byte-for-byte instead of rebuilt.
        if not archived_records:
            archived_records = [r for path in self.history_files()
                                for r in json.loads(path.read_text(encoding='utf-8'))['triggers']]
        self.assertIn(previous['signal_id'], {r['signal_id'] for r in archived_records})
        self.assertEqual(book['report_period_start'], second)
        self.assertEqual(book['report_period_end'], stamp('2026-10-23 08:00'))

    def test_empty_period_still_rotates_and_keeps_previous_workbook(self):
        self.publish(stamp('2026-10-21 12:00'))
        previous = self.main_path().read_bytes()
        self.publish(stamp('2026-10-22 08:00'))
        self.assertEqual(self.read_current()['triggers'], [])
        self.assertEqual(self.read_current()['report_period_start'], stamp('2026-10-22 08:00'))
        self.assertIn(previous, [p.read_bytes() for p in self.history_files()])

    def test_restart_same_period_does_not_duplicate_archives(self):
        first = stamp('2026-10-21 12:00')
        self.add_signal(first + 1000)
        self.publish(first + 2000)
        second = stamp('2026-10-22 08:00')
        self.publish(second + 2000)
        existing = {str(p): p.read_bytes() for p in self.history_files()}
        self.store.close()
        self.store = Store(self.root / 'data' / 'monitor.sqlite3', self.config)
        self.publish(second + 3000, reason='STARTUP')
        self.assertEqual({str(p): p.read_bytes() for p in self.history_files()}, existing)
        self.assertFalse(any(b.get('archive') for b in self.built_batches[-1]))

    def test_skipped_periods_preserve_records_and_create_current_report(self):
        first = stamp('2026-10-21 12:00')
        initial = self.add_signal(first + 1000)
        self.publish(first + 2000)
        skipped = self.add_signal(stamp('2026-10-23 10:00') + 1000)
        resumed = stamp('2026-10-25 12:00')
        current = self.add_signal(resumed + 1000)
        self.publish(resumed + 2000, reason='STARTUP')
        self.assertEqual([r['signal_id'] for r in self.read_current()['triggers']], [current['signal_id']])
        all_archived = [r for path in self.history_files()
                        for r in json.loads(path.read_text(encoding='utf-8'))['triggers']]
        self.assertTrue({initial['signal_id'], skipped['signal_id']} <= {r['signal_id'] for r in all_archived})
        self.assertEqual(self.store.get_meta('excel_archive_15m')['current_start'], stamp('2026-10-25 08:00'))

    def test_changed_notification_state_refreshes_closed_period_once(self):
        first = stamp('2026-10-21 12:00')
        event = self.add_signal(first + 1000)
        self.publish(first + 2000)
        second = stamp('2026-10-22 08:00')
        self.publish(second + 2000)
        key = str(stamp('2026-10-21 08:00'))
        before = self.store.get_meta('excel_archive_15m')['archived'][key]['signature']
        with self.store.lock, self.store.db:
            self.store.db.execute('UPDATE outbox SET status=?,sent=?,attempts=? WHERE id=?',
                                  ('SENT', first + 3000, 2, event['signal_id']))
        self.publish(second + 3000)
        after = self.store.get_meta('excel_archive_15m')['archived'][key]['signature']
        self.assertNotEqual(before, after)
        records = [r for b in self.built_batches[-1] if b.get('archive') for r in b['triggers']]
        row = next(r for r in records if r['signal_id'] == event['signal_id'])
        self.assertEqual((row['notification_status'], row['sent'], row['attempts']), ('SENT', first + 3000, 2))
        self.publish(second + 4000)
        self.assertFalse(any(b.get('archive') for b in self.built_batches[-1]))

    def test_failed_build_cannot_move_old_workbook_or_advance_state(self):
        first = stamp('2026-10-21 12:00')
        self.publish(first)
        before_file = self.main_path().read_bytes()
        before_state = self.store.get_meta('excel_archive_15m')
        before_manifest = (Path(self.config['excel_dir']) / '导出清单.json').read_bytes()
        with patch('monitor.export.now_ms', return_value=stamp('2026-10-22 08:00')), \
             patch('monitor.export.subprocess.run', return_value=SimpleNamespace(returncode=1, stderr='builder failed')):
            with self.assertRaises(RuntimeError):
                export_all(self.store, self.config, intervals=['15m'])
        self.assertEqual(self.main_path().read_bytes(), before_file)
        self.assertEqual(self.store.get_meta('excel_archive_15m'), before_state)
        self.assertEqual((Path(self.config['excel_dir']) / '导出清单.json').read_bytes(), before_manifest)
        self.assertEqual(self.history_files(), [])

    def test_locked_old_workbook_is_preserved_and_recovers_next_refresh(self):
        first = stamp('2026-10-21 12:00')
        old_event = self.add_signal(first + 1000)
        self.publish(first + 2000)
        old_bytes = self.main_path().read_bytes()
        second = stamp('2026-10-22 08:00')
        new_event = self.add_signal(second + 1000)
        original_replace = os.replace
        locked_path = self.main_path()

        def replace(source, target):
            if Path(source) == locked_path or Path(target) == locked_path:
                raise PermissionError('simulated Excel sharing lock')
            return original_replace(source, target)

        with patch('os.replace', side_effect=replace):
            outputs = self.publish(second + 2000)
        self.assertEqual(self.main_path().read_bytes(), old_bytes)
        self.assertTrue(outputs)
        current_books = [json.loads(Path(p).read_text(encoding='utf-8')) for p in outputs]
        self.assertIn(new_event['signal_id'], {r['signal_id'] for b in current_books for r in b['triggers']})
        self.assertTrue(self.store.get_meta('last_export')['by_interval']['15m']['pending_replace'])
        self.publish(second + 3000)
        self.assertEqual([r['signal_id'] for r in self.read_current()['triggers']], [new_event['signal_id']])
        self.assertFalse(self.store.get_meta('last_export')['by_interval']['15m']['pending_replace'])
        archived_records = [r for path in self.history_files()
                            for r in json.loads(path.read_text(encoding='utf-8'))['triggers']]
        self.assertIn(old_event['signal_id'], {r['signal_id'] for r in archived_records})

    def test_current_period_capacity_split_preserves_every_signal_and_comparison(self):
        self.config['excel_max_data_rows'] = 30
        at = stamp('2026-10-21 12:00')
        expected = {self.add_signal(at + 1000 + n)['signal_id'] for n in range(5)}
        outputs = self.publish(at + 2000)
        current = [b for b in self.built_batches[-1] if not b.get('archive')]
        self.assertGreater(len(current), 1)
        self.assertEqual(len(outputs), len(current))
        self.assertEqual({r['signal_id'] for b in current for r in b['triggers']}, expected)
        previous = [r for b in current for r in b['previous']]
        self.assertEqual(len(previous), 5 * 15)
        self.assertTrue(all(len(b['previous']) <= 30 for b in current))
        self.assertTrue(all(b['report_period_start'] == stamp('2026-10-21 08:00') for b in current))
        self.publish(stamp('2026-10-22 08:00') + 1000)
        in_root = list(Path(self.config['excel_dir']).glob('*.xlsx'))
        self.assertEqual(in_root, [self.main_path()])

    def test_status_overflow_uses_current_period_when_old_report_is_rebuilt(self):
        first = stamp('2026-10-21 12:00')
        event = self.add_signal(first + 1000)
        self.publish(first + 2000)
        self.store.db.execute('UPDATE outbox SET status=? WHERE id=?', ('SENT', event['signal_id']))
        self.store.db.commit()
        self.config['excel_max_data_rows'] = 20
        for n in range(22):
            self.store.anomaly('', '15m', 'TEST', str(n))
        second = stamp('2026-10-22 08:00')
        self.publish(second + 2000)
        books = self.built_batches[-1]
        self.assertTrue(any(b['archive'] for b in books))
        overflow = [b for b in books if '异常归档' in b['filename']]
        self.assertTrue(overflow)
        for book in overflow:
            parameters = {p['key']: p['value'] for p in book['parameters']}
            self.assertEqual(book['report_period_start'], second)
            self.assertTrue(parameters['记录归属期 北京'].startswith('20261022_0800'))


if __name__ == '__main__':
    unittest.main()
