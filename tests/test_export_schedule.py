"""Close-aligned exports, coverage, and per-period publication without Node/network."""
import asyncio
import datetime as dt
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from monitor.core import PERIODS, Store, now_ms
from monitor.export import (
    CLOSE_WAIT_SECONDS, LABELS, close_coverage, export_all, export_loop,
    export_period_loop, next_boundary, snapshot, wait_for_close,
)
from test_monitor import BASE, CONFIG, bars, info, signal_bar


class FakeClock:
    def __init__(self, stamp):
        self.stamp = stamp
        self.pauses = []

    def time(self):
        return self.stamp

    async def pause(self, monitor, seconds):
        self.pauses.append(seconds)
        self.stamp += round(seconds * 1000)
        await asyncio.sleep(0)


class ScheduleTests(unittest.IsolatedAsyncioTestCase):
    def monitor(self, stamp):
        clock = FakeClock(stamp)
        anomalies = []
        monitor = SimpleNamespace(
            client=clock, stop=asyncio.Event(), config={},
            store=SimpleNamespace(anomaly=lambda *args: anomalies.append(args)),
        )
        return monitor, clock, anomalies

    def test_boundaries_use_binance_close_times_in_beijing(self):
        zone = dt.timezone(dt.timedelta(hours=8))
        current = dt.datetime(2026, 10, 15, 15, 29, 1, tzinfo=zone)
        stamp = int(current.timestamp() * 1000)
        expected = {
            '15m': (2026, 10, 15, 15, 30),
            '1h': (2026, 10, 15, 16, 0),
            '4h': (2026, 10, 15, 16, 0),
            '1d': (2026, 10, 16, 8, 0),
        }
        for interval, parts in expected.items():
            result = dt.datetime.fromtimestamp(next_boundary(stamp, interval) / 1000, zone)
            self.assertEqual((result.year, result.month, result.day, result.hour, result.minute), parts)
            self.assertEqual(next_boundary(int(result.timestamp() * 1000), interval),
                             int(result.timestamp() * 1000) + PERIODS[interval])

    async def test_startup_and_only_requested_close_no_minute_rewrites(self):
        monitor, clock, anomalies = self.monitor(BASE + 60_000)
        boundary = BASE + PERIODS['15m']
        calls = []

        def publish(*args, **kwargs):
            calls.append(dict(kwargs))

        async def pause(mon, seconds):
            await clock.pause(mon, seconds)
            if clock.time() >= boundary + 61_000:
                mon.stop.set()

        with patch('monitor.export.export_all', side_effect=publish), \
             patch('monitor.export.close_coverage', return_value={'expected': 2, 'completed': 2, 'missing': []}), \
             patch('monitor.export.pause_or_stop', side_effect=pause):
            await asyncio.wait_for(export_period_loop(monitor, '15m'), 5)
        self.assertEqual([c['refresh_reason'] for c in calls], ['STARTUP', 'CANDLE_CLOSE'])
        self.assertTrue(all(c['intervals'] == ['15m'] for c in calls))
        self.assertEqual(calls[1]['refresh_boundary'], boundary)
        self.assertEqual(anomalies, [])

    async def test_startup_crossing_close_does_not_drop_boundary(self):
        boundary = BASE + PERIODS['15m']
        monitor, clock, anomalies = self.monitor(boundary - 1000)
        calls = []
        event_loop = asyncio.get_running_loop()

        def publish(*args, **kwargs):
            calls.append(dict(kwargs))
            if kwargs['refresh_reason'] == 'STARTUP':
                clock.stamp = boundary + 1200
            else:
                event_loop.call_soon_threadsafe(monitor.stop.set)

        with patch('monitor.export.export_all', side_effect=publish), \
             patch('monitor.export.close_coverage', return_value={'expected': 1, 'completed': 1, 'missing': []}), \
             patch('monitor.export.pause_or_stop', side_effect=clock.pause):
            await asyncio.wait_for(export_period_loop(monitor, '15m'), 5)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]['refresh_boundary'], boundary)
        self.assertEqual(anomalies, [])

    async def test_all_four_workers_publish_startup_independently(self):
        monitor, clock, _ = self.monitor(BASE + 1000)
        calls = []
        event_loop = asyncio.get_running_loop()
        guard = threading.Lock()

        def publish(*args, **kwargs):
            with guard:
                calls.append(dict(kwargs))
                if len(calls) == 4:
                    event_loop.call_soon_threadsafe(monitor.stop.set)

        async def pause(mon, seconds):
            await asyncio.sleep(0)

        with patch('monitor.export.export_all', side_effect=publish), \
             patch('monitor.export.pause_or_stop', side_effect=pause):
            await asyncio.wait_for(export_loop(monitor), 5)
        self.assertEqual({tuple(c['intervals']) for c in calls}, {(i,) for i in PERIODS})
        self.assertTrue(all(c['refresh_reason'] == 'STARTUP' for c in calls))

    async def test_waits_for_committed_coverage_then_returns_immediately(self):
        boundary = BASE + PERIODS['15m']
        monitor, clock, _ = self.monitor(boundary)
        results = [
            {'expected': 2, 'completed': 1, 'missing': ['LATEUSDT']},
            {'expected': 2, 'completed': 1, 'missing': ['LATEUSDT']},
            {'expected': 2, 'completed': 2, 'missing': []},
        ]
        with patch('monitor.export.close_coverage', side_effect=results) as coverage, \
             patch('monitor.export.pause_or_stop', side_effect=clock.pause):
            await wait_for_close(monitor, '15m', boundary)
        self.assertEqual(coverage.call_count, 3)
        self.assertEqual(clock.time() - boundary, 1000)
        self.assertFalse(monitor.stop.is_set())

    async def test_missing_coverage_is_bounded_by_thirty_seconds(self):
        boundary = BASE + PERIODS['15m']
        monitor, clock, _ = self.monitor(boundary)
        with patch('monitor.export.close_coverage', return_value={'expected': 2, 'completed': 1, 'missing': ['LATEUSDT']}), \
             patch('monitor.export.pause_or_stop', side_effect=clock.pause), \
             patch('monitor.export.export_all') as publish:
            await wait_for_close(monitor, '15m', boundary)
        self.assertEqual(CLOSE_WAIT_SECONDS, 30)
        self.assertEqual(clock.time() - boundary, 30_000)
        publish.assert_not_called()

    async def test_shutdown_waits_for_inflight_export_without_extra_refresh(self):
        monitor, _, _ = self.monitor(BASE + 1000)
        event_loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()
        calls = []

        def publish(*args, **kwargs):
            calls.append(dict(kwargs))
            event_loop.call_soon_threadsafe(started.set)
            if not release.wait(5):
                raise TimeoutError('test export was not released')

        with patch('monitor.export.export_all', side_effect=publish):
            task = asyncio.create_task(export_period_loop(monitor, '15m'))
            try:
                await asyncio.wait_for(started.wait(), 2)
                monitor.stop.set()
                await asyncio.sleep(.01)
                self.assertFalse(task.done())
            finally:
                release.set()
                await asyncio.wait_for(task, 2)
        self.assertEqual([c['refresh_reason'] for c in calls], ['STARTUP'])


class SnapshotPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = dict(CONFIG, data_dir=self.temp.name, excel_dir=self.temp.name)
        self.store = Store(self.root / 'monitor.sqlite3', self.config)
        self.store.refresh({'symbols': [info()]})

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def fake_builder(self, command, **kwargs):
        spec = json.loads(Path(command[-2]).read_text(encoding='utf-8'))
        for book in spec['books']:
            (Path(command[-1]) / book['filename']).write_bytes(b'placeholder workbook')
        return SimpleNamespace(returncode=0, stderr='')

    def test_low_warmup_evaluation_counts_as_processed(self):
        rows = bars(10, '15m')
        self.store.ingest('TESTUSDT', '15m', rows, 'HISTORICAL', now_ms())
        self.assertEqual(self.store.state('TESTUSDT', '15m')['readiness'], 'NOT_READY')
        coverage = close_coverage(self.store, '15m', rows[-1][6] + 1)
        self.assertEqual(coverage, {'expected': 1, 'completed': 1, 'missing': [], 'total_symbols': 1, 'not_applicable': [], 'current_warnings': []})

    def test_snapshot_filters_period_and_marks_missing_close(self):
        self.store.refresh({'symbols': [info(), info('MISSINGUSDT'), info('ERRORUSDT')]})
        rows = bars(21, '15m')
        boundary = rows[-1][6] + 1
        for symbol in ('TESTUSDT', 'ERRORUSDT'):
            self.store.ingest(symbol, '15m', rows, 'HISTORICAL', now_ms())
        self.store.mark('ERRORUSDT', '15m', 'DATA_ERROR', 'test missing data')
        books = snapshot(self.store, self.config, ['15m'], 'CANDLE_CLOSE', boundary)
        self.assertEqual(len(books), 1)
        book = books[0]
        self.assertEqual(book['interval'], '15m')
        self.assertEqual(book['refresh_boundary'], boundary)
        self.assertEqual(book['coverage']['completed'], 2)
        self.assertEqual(set(book['coverage']['missing']), {'MISSINGUSDT'})
        rows_by_symbol = {r['symbol']: r for r in book['overview']}
        self.assertFalse(rows_by_symbol['TESTUSDT']['stale'])
        self.assertTrue(rows_by_symbol['MISSINGUSDT']['stale'])
        self.assertEqual(rows_by_symbol['ERRORUSDT']['health'], 'DATA_ERROR')
        self.assertIn('ERRORUSDT',book['coverage']['current_warnings'])
        missing_rows = [r for r in book['status'] if r['code'] == 'CLOSE_NOT_PROCESSED']
        self.assertEqual({r['symbol'] for r in missing_rows}, {'MISSINGUSDT'})
        self.assertTrue(all(str(boundary) in r['detail'] for r in missing_rows))
        self.assertEqual(close_coverage(self.store, '15m', boundary)['completed'], 2)

    def test_trigger_records_distinguish_detection_and_candle_close(self):
        rows = bars(200, '15m')
        self.store.ingest('TESTUSDT', '15m', rows, 'HISTORICAL', now_ms())
        target = signal_bar(rows[-1], '15m')
        boundary = target[6] + 1
        detected = boundary + 1500
        original_version = self.store.version
        with patch('monitor.core.now_ms', return_value=detected):
            event = self.store.ingest('TESTUSDT', '15m', [target], 'REALTIME', detected)[0]
        with patch('monitor.export.now_ms', return_value=detected):
            books = snapshot(self.store, self.config, ['15m'], 'CANDLE_CLOSE', boundary)
        trigger = books[0]['triggers'][0]
        self.assertEqual(trigger['discovered'], detected)
        self.assertEqual(trigger['close_boundary'], boundary)
        self.assertEqual(trigger['T'], boundary - 1)
        self.assertEqual(trigger['strategy_version'], original_version)
        self.assertEqual(trigger['signal_id'], event['signal_id'])
        self.assertEqual(self.store.version, original_version)

    def test_invalid_interval_selection_rejected(self):
        for invalid in ([], ['5m'], ['15m', '5m']):
            with self.assertRaisesRegex(ValueError, 'interval'):
                snapshot(self.store, self.config, invalid)

    def test_single_period_export_preserves_legacy_other_periods(self):
        files = []
        for interval in PERIODS:
            target = self.root / f'币安USDT合约监控_{LABELS[interval]}.xlsx'
            target.write_bytes(('old ' + interval).encode())
            files.append(str(target))
        self.store.meta('last_export', {'time': 123, 'files': files, 'pending_replace': False})
        with patch('monitor.export.run_builder', side_effect=self.fake_builder):
            outputs = export_all(self.store, self.config, intervals=['15m'],
                                 refresh_reason='CANDLE_CLOSE', refresh_boundary=BASE + PERIODS['15m'])
        state = self.store.get_meta('last_export')
        manifest = json.loads((self.root / '导出清单.json').read_text(encoding='utf-8'))
        self.assertEqual(len(outputs), 1)
        self.assertEqual(set(state['by_interval']), set(PERIODS))
        self.assertEqual(set(state['files']), set(files))
        self.assertEqual(manifest['by_interval'], state['by_interval'])
        for interval in ('1h', '4h', '1d'):
            target = self.root / f'币安USDT合约监控_{LABELS[interval]}.xlsx'
            self.assertEqual(target.read_bytes(), ('old ' + interval).encode())
            self.assertEqual(state['by_interval'][interval]['time'], 123)
        self.assertEqual(state['by_interval']['15m']['reason'], 'CANDLE_CLOSE')

    def test_pending_replacement_for_other_period_is_preserved(self):
        locked = str(self.root / '币安USDT合约监控_1小时_snapshot_123.xlsx')
        previous = {'time': 123, 'files': [locked], 'pending_replace': True,
                    'by_interval': {'1h': {'time': 123, 'files': [locked], 'pending_replace': True}}}
        self.store.meta('last_export', previous)
        with patch('monitor.export.run_builder', side_effect=self.fake_builder):
            export_all(self.store, self.config, intervals=['15m'])
        state = self.store.get_meta('last_export')
        self.assertTrue(state['pending_replace'])
        self.assertEqual(state['by_interval']['1h'], previous['by_interval']['1h'])
        self.assertIn(locked, state['files'])
        self.assertFalse(state['by_interval']['15m']['pending_replace'])

    def test_concurrent_period_exports_keep_all_files_and_separate_caches(self):
        active = 0
        peak_active = 0
        specs = []
        guard = threading.Lock()

        def builder(command, **kwargs):
            nonlocal active, peak_active
            with guard:
                specs.append(Path(command[-2]))
                active += 1
                peak_active = max(peak_active, active)
            try:
                threading.Event().wait(.02)
                return self.fake_builder(command, **kwargs)
            finally:
                with guard:
                    active -= 1

        with patch('monitor.export.run_builder', side_effect=builder):
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda i: export_all(self.store, self.config, intervals=[i]), PERIODS))
        state = self.store.get_meta('last_export')
        self.assertEqual(len(set(specs)), 4)
        self.assertEqual(peak_active, 1)
        self.assertEqual({p.parent.name for p in specs}, set(PERIODS))
        self.assertEqual(set(state['by_interval']), set(PERIODS))
        self.assertEqual(set(state['files']), {r[0] for r in results})
        self.assertTrue(all(Path(p).exists() for p in state['files']))

    def test_failed_period_does_not_replace_manifest_or_other_periods(self):
        previous = {'time': 123, 'files': ['retained.xlsx'], 'pending_replace': False}
        self.store.meta('last_export', previous)
        manifest = self.root / '导出清单.json'
        manifest.write_text(json.dumps(previous), encoding='utf-8')
        original = manifest.read_bytes()
        with patch('monitor.export.run_builder', return_value=SimpleNamespace(returncode=1, stderr='test failure')):
            with self.assertRaisesRegex(RuntimeError, '导出失败'):
                export_all(self.store, self.config, intervals=['15m'])
        self.assertEqual(self.store.get_meta('last_export'), previous)
        self.assertEqual(manifest.read_bytes(), original)


if __name__ == '__main__':
    unittest.main()
