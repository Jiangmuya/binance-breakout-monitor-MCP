"""Archive durability, bounded pages, late events and file move recovery."""
import datetime as dt
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from monitor.archive import ARCHIVE_DIR, DAY, report_period, resolved
from monitor import history_maintenance as maintenance


class TestStore:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE anomalies(id INTEGER PRIMARY KEY AUTOINCREMENT,time INTEGER,symbol TEXT,
                interval TEXT,code TEXT,detail TEXT);
            CREATE TABLE signals_export_metadata(interval TEXT,start INTEGER,revision INTEGER,
                exported_ms INTEGER,row_count INTEGER,manifest TEXT,PRIMARY KEY(interval,start));
            CREATE TABLE signals(id TEXT PRIMARY KEY,event_type TEXT);''')
        self.lock = threading.RLock()

    def get_meta(self, key, default=None):
        row = self.db.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def meta(self, key, value):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)',
                            (key, json.dumps(value, ensure_ascii=False)))


class HistoryMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = resolved(self.temp.name)
        self.store = TestStore(self.root / 'monitor.sqlite3')
        self.config = dict(excel_dir=str(self.root / 'excel'))
        self.history = Path(self.config['excel_dir']) / ARCHIVE_DIR
        self.history.mkdir(parents=True)
        self.now = int(dt.datetime(2027, 2, 1, tzinfo=dt.timezone.utc).timestamp()*1000)

    def tearDown(self):
        self.store.db.close()
        # Test-created root is checked before recursive cleanup; Windows child
        # names may exceed MAX_PATH in the dedicated regression below.
        self.assertEqual(resolved(self.temp.name), self.root)
        shutil.rmtree(maintenance._io(self.root))
        self.temp.cleanup()

    def anomalies(self, count, stamp=None):
        stamp = self.now if stamp is None else stamp
        with self.store.db:
            self.store.db.executemany('INSERT INTO anomalies(time,symbol,interval,code,detail) VALUES(?,?,?,?,?)',
                [(stamp, 'TESTUSDT', '15m', 'GAP', f'detail-{n}') for n in range(count)])

    def archived_rows(self):
        rows = []
        for path in maintenance._io(self.history / '异常历史').glob('*/*.jsonl.gz'):
            with gzip.open(path, 'rt', encoding='utf-8') as handle:
                rows.extend(json.loads(line) for line in handle)
        return rows

    def workbook(self, age, name=None, interval='15m'):
        end = self.now - age*DAY
        start = report_period(interval, end-1)[0]
        path = self.history / (name or f'period_{age}.xlsx')
        maintenance._io(path).write_bytes(f'workbook-age-{age}'.encode())
        manifest = dict(files=[str(path)], report_period_start=start, report_period_end=end)
        with self.store.db:
            self.store.db.execute('INSERT OR REPLACE INTO signals_export_metadata VALUES(?,?,?,?,?,?)',
                (interval, start, 1, self.now, 3, json.dumps(manifest)))
        state = self.store.get_meta('excel_archive_'+interval, dict(archived={}))
        state['archived'][str(start)] = dict(signature='hash', files=[str(path)])
        self.store.meta('excel_archive_'+interval, state)
        return path, start

    def test_backfill_is_paged_idempotent_and_keeps_all_database_rows(self):
        self.anomalies(1201)
        with self.store.db:
            self.store.db.execute("INSERT INTO signals VALUES('expired-1','EXPIRED')")
        result = maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(result['rows_written'], 1201)
        self.assertEqual(result['batches_written'], 3)
        self.assertEqual(result['archived_through_id'], 1201)
        manifests = [json.loads(p.read_text(encoding='utf-8')) for p in
                     (self.history/'异常历史'/'批次清单').glob('*.json')]
        self.assertEqual(sorted(p['rows'] for p in manifests), [201, 500, 500])
        self.assertEqual(len({r['id'] for r in self.archived_rows()}), 1201)
        again = maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(again['rows_written'], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM anomalies').fetchone()[0], 1201)
        self.assertEqual(self.store.db.execute('SELECT event_type FROM signals').fetchone()[0], 'EXPIRED')

    def test_late_anomaly_with_older_event_time_is_archived_by_new_id(self):
        self.anomalies(1)
        maintenance.archive_anomalies(self.store, self.config)
        self.anomalies(1, self.now-200*DAY)
        result = maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(result['rows_written'], 1)
        self.assertEqual(sorted(r['id'] for r in self.archived_rows()), [1, 2])
        self.assertEqual(len(list((self.history/'异常历史').glob('????-??'))), 2)

    def test_snapshot_does_not_mix_concurrent_new_events(self):
        self.anomalies(600)
        original = maintenance._write_anomaly_part
        inserted = False
        def write_then_insert(path, rows):
            nonlocal inserted
            value = original(path, rows)
            if not inserted:
                self.anomalies(1)
                inserted = True
            return value
        with patch.object(maintenance, '_write_anomaly_part', side_effect=write_then_insert):
            result = maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(result['snapshot_max_id'], 600)
        self.assertEqual(len(self.archived_rows()), 600)
        self.assertEqual(maintenance.archive_anomalies(self.store, self.config)['rows_written'], 1)

    def test_crash_after_manifest_before_watermark_replays_without_duplicates(self):
        self.anomalies(20)
        original = self.store.meta
        def fail_watermark(key, value):
            if key == maintenance.ANOMALY_KEY and not value.get('pending'):
                raise RuntimeError('simulated process loss')
            return original(key, value)
        with patch.object(self.store, 'meta', side_effect=fail_watermark):
            with self.assertRaisesRegex(RuntimeError, 'process loss'):
                maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(self.store.get_meta(maintenance.ANOMALY_KEY)['through_id'], 0)
        # New arrivals must not enlarge the already published partial batch.
        self.anomalies(7)
        maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(len(self.archived_rows()), 27)
        self.assertEqual(len({r['id'] for r in self.archived_rows()}), 27)
        self.assertEqual(self.store.get_meta(maintenance.ANOMALY_KEY)['through_id'], 27)

    def test_locked_anomaly_file_keeps_watermark_and_recovers(self):
        self.anomalies(3)
        original = os.replace
        def locked(source, target):
            if str(target).endswith('.jsonl.gz'):
                raise PermissionError('occupied')
            return original(source, target)
        with patch.object(maintenance.os, 'replace', side_effect=locked):
            result = maintenance.archive_anomalies(self.store, self.config)
        self.assertTrue(result['pending'])
        self.assertEqual(self.store.get_meta(maintenance.ANOMALY_KEY)['through_id'], 0)
        self.assertEqual(maintenance.archive_anomalies(self.store, self.config)['rows_written'], 3)

    def test_corrupt_file_is_not_acknowledged_or_silently_overwritten(self):
        self.anomalies(3)
        original = self.store.meta
        def crash_on_commit(key, value):
            if key == maintenance.ANOMALY_KEY and not value.get('pending'):
                raise RuntimeError('crash')
            return original(key, value)
        with patch.object(self.store, 'meta', side_effect=crash_on_commit):
            with self.assertRaises(RuntimeError):
                maintenance.archive_anomalies(self.store, self.config)
        path = next((self.history/'异常历史').glob('*/*.jsonl.gz'))
        path.write_bytes(b'corrupted')
        with self.assertRaisesRegex(RuntimeError, 'corruption'):
            maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(self.store.get_meta(maintenance.ANOMALY_KEY)['through_id'], 0)
        self.assertEqual(path.read_bytes(), b'corrupted')

    def test_tier_boundaries_preserve_bytes_and_both_manifest_registries(self):
        items = [self.workbook(age) for age in (30, 31, 90, 91)]
        expected = {str(path): path.read_bytes() for path, _ in items}
        result = maintenance.tier_archives(self.store, self.config, self.now)
        self.assertEqual(result['moved'], 3)
        self.assertTrue(items[0][0].exists())
        for path, start in items[1:]:
            value = json.loads(self.store.db.execute('SELECT manifest FROM signals_export_metadata WHERE start=?', (start,)).fetchone()[0])
            target = Path(value['files'][0])
            self.assertEqual(target.read_bytes(), expected[str(path)])
            self.assertEqual(self.store.get_meta('excel_archive_15m')['archived'][str(start)]['files'], [str(target)])
            self.assertFalse(path.exists())
        self.assertEqual(len(list((self.history/'月归档').glob('*/*.xlsx'))), 2)
        self.assertEqual(len(list((self.history/'年归档').glob('*/*.xlsx'))), 1)
        self.assertEqual(maintenance.tier_archives(self.store, self.config, self.now)['moved'], 0)

    def test_workbook_lock_is_journaled_and_retried_same_day(self):
        path, start = self.workbook(35)
        original = os.replace
        def locked(source, target):
            if resolved(source) == path:
                raise PermissionError('Excel occupied')
            return original(source, target)
        with patch.object(maintenance.os, 'replace', side_effect=locked):
            result = maintenance.tier_archives(self.store, self.config, self.now)
            next_day = maintenance.tier_archives(self.store, self.config, self.now+DAY)
        self.assertEqual(result['pending'], 1)
        self.assertEqual(next_day['pending'], 1)
        self.assertTrue(path.exists())
        result = maintenance.tier_archives(self.store, self.config, self.now)
        self.assertEqual((result['moved'], result['pending']), (1, 0))
        self.assertFalse(path.exists())

    def test_crash_after_move_before_metadata_recovers_references(self):
        source, start = self.workbook(35)
        with patch.object(maintenance, '_replace_references', side_effect=RuntimeError('process loss')):
            with self.assertRaisesRegex(RuntimeError, 'process loss'):
                maintenance.tier_archives(self.store, self.config, self.now)
        self.assertFalse(source.exists())
        journal = self.store.get_meta(maintenance.TIER_KEY)['pending'][0]
        self.assertEqual(maintenance.tier_archives(self.store, self.config, self.now)['pending'], 0)
        self.assertEqual(self.store.get_meta('excel_archive_15m')['archived'][str(start)]['files'], [journal['target']])
        manifest = json.loads(self.store.db.execute('SELECT manifest FROM signals_export_metadata WHERE start=?', (start,)).fetchone()[0])
        self.assertEqual(manifest['files'], [journal['target']])

    def test_exclusive_read_lock_also_keeps_a_retryable_intent(self):
        source, start = self.workbook(35)
        original = maintenance._hash
        def denied(path):
            if resolved(path) == source:
                raise PermissionError('Exclusive reader lock')
            return original(path)
        with patch.object(maintenance, '_hash', side_effect=denied):
            result = maintenance.tier_archives(self.store, self.config, self.now)
        self.assertEqual(result['pending'], 1)
        self.assertIsNone(self.store.get_meta(maintenance.TIER_KEY)['pending'][0]['sha256'])
        result = maintenance.tier_archives(self.store, self.config, self.now)
        self.assertEqual((result['moved'], result['pending']), (1, 0))
        self.assertFalse(source.exists())

    def test_collision_preserves_both_versions(self):
        source, _ = self.workbook(35)
        end = self.now-35*DAY
        month = dt.datetime.fromtimestamp(end/1000, dt.timezone.utc).strftime('%Y-%m')
        target = self.history/'月归档'/month/source.name
        target.parent.mkdir(parents=True)
        target.write_bytes(b'older-different-version')
        os.utime(target, (end/1000, end/1000))
        result = maintenance.tier_archives(self.store, self.config, self.now)
        self.assertEqual(result['moved'], 1)
        self.assertEqual(target.read_bytes(), b'older-different-version')
        self.assertEqual(len(list(target.parent.glob('*.xlsx'))), 2)

    def test_current_report_and_external_paths_are_not_moved(self):
        current = Path(self.config['excel_dir'])/'current.xlsx'
        current.write_bytes(b'current')
        external = self.root/'unrelated.xlsx'
        external.write_bytes(b'external')
        with self.store.db:
            self.store.db.execute('INSERT INTO signals_export_metadata VALUES(?,?,?,?,?,?)',
                ('15m', self.now-200*DAY, 1, self.now, 1,
                 json.dumps(dict(files=[str(current), str(external)], report_period_end=self.now-199*DAY))))
        self.assertEqual(maintenance.tier_archives(self.store, self.config, self.now)['moved'], 0)
        self.assertEqual(current.read_bytes(), b'current')
        self.assertEqual(external.read_bytes(), b'external')

    def test_forged_pending_move_cannot_escape_history_scope(self):
        source, _ = self.workbook(35)
        self.store.meta(maintenance.TIER_KEY, dict(pending=[dict(source=str(source),
            target=str(self.root/'escaped.xlsx'), sha256=maintenance._hash(source))]))
        with self.assertRaises(ValueError):
            maintenance.tier_archives(self.store, self.config, self.now)
        self.assertTrue(source.exists())
        self.assertFalse((self.root/'escaped.xlsx').exists())

    def test_windows_long_history_paths_keep_scope_and_recover(self):
        deep = self.root / ('workspace-'+'a'*65) / ('reports-'+'b'*65)
        self.config['excel_dir'] = str(deep/'excel')
        self.history = Path(self.config['excel_dir'])/ARCHIVE_DIR
        maintenance._io(self.history).mkdir(parents=True)
        self.anomalies(2)
        result = maintenance.archive_anomalies(self.store, self.config)
        self.assertEqual(result['rows_written'], 2)
        self.assertTrue(all(len(name) > 260 for name in result['files']))
        self.assertEqual(len(self.archived_rows()), 2)
        source, start = self.workbook(35)
        result = maintenance.tier_archives(self.store, self.config, self.now)
        self.assertEqual(result['moved'], 1)
        target = Path(self.store.get_meta('excel_archive_15m')['archived'][str(start)]['files'][0])
        self.assertTrue(maintenance._io(target).is_file())
        self.assertFalse(maintenance._io(source).exists())


if __name__ == '__main__':
    unittest.main()
