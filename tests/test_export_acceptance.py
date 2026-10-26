"""Acceptance evidence must preserve data and require real elapsed export runs."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import sys

from monitor.maintenance import consistent_backup, database_inventory, OfflineLock, offline_vacuum

_spec = importlib.util.spec_from_file_location('export_soak', Path(__file__).resolve().parents[1] / 'tools/export_soak.py')
soak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(soak)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import run_shadow as shadow
from monitor.core import Store, dumps


def sample_database(path):
    db = sqlite3.connect(path)
    db.executescript('PRAGMA journal_mode=WAL; CREATE TABLE signals(id TEXT PRIMARY KEY,interval TEXT);'
                     'CREATE TABLE anomalies(id INTEGER PRIMARY KEY,detail TEXT);'
                     "INSERT INTO signals VALUES('a','15m'); INSERT INTO signals VALUES('b','1h');"
                     "INSERT INTO anomalies VALUES(1,'retained');")
    return db


def workbook(path, ids, shared=False):
    strings = ['signal_id'] + ids
    with zipfile.ZipFile(path, 'w') as package:
        package.writestr('xl/workbook.xml', '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>'
            '<sheet name="触发记录" sheetId="1" r:id="r1"/></sheets></workbook>')
        package.writestr('xl/_rels/workbook.xml.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="r1" Target="worksheets/sheet1.xml"/></Relationships>')
        if shared:
            package.writestr('xl/sharedStrings.xml', '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' +
                ''.join('<si><t>' + text + '</t></si>' for text in strings) + '</sst>')
        rows = []
        for number, text in enumerate(strings, 1):
            value = f'<c r="Z{number}" t="s"><v>{number-1}</v></c>' if shared else f'<c r="Z{number}" t="inlineStr"><is><t>{text}</t></is></c>'
            rows.append(f'<row r="{number}">{value}</row>')
        package.writestr('xl/worksheets/sheet1.xml', '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>' +
                         ''.join(rows) + '</sheetData></worksheet>')


class MaintenanceAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = sample_database(self.root / 'monitor.sqlite3')

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def test_backup_includes_committed_wal_and_pins_before_concurrent_write(self):
        modified = []
        def progress(status, remaining, total):
            if not modified:
                self.db.execute("INSERT INTO signals VALUES('later','4h')")
                self.db.commit()
                modified.append(True)
        result = consistent_backup(self.root / 'monitor.sqlite3', self.root / 'backup.sqlite3', progress=progress)
        restored = sqlite3.connect(self.root / 'backup.sqlite3')
        try:
            self.assertEqual(['a', 'b'], [r[0] for r in restored.execute('SELECT id FROM signals ORDER BY id')])
            self.assertEqual('ok', result['quick_check'])
            self.assertEqual(2, result['counts']['signals'])
            self.assertEqual(result['signal_ids_sha256'], database_inventory(restored)['signal_ids_sha256'])
        finally:
            restored.close()
        self.assertEqual(3, self.db.execute('SELECT count(*) FROM signals').fetchone()[0])

    def test_backup_refuses_existing_destination(self):
        destination = self.root / 'backup.sqlite3'
        destination.write_bytes(b'preserve')
        with self.assertRaises(FileExistsError):
            consistent_backup(self.root / 'monitor.sqlite3', destination)
        self.assertEqual(b'preserve', destination.read_bytes())

    def test_vacuum_requires_offline_lock(self):
        with OfflineLock(self.root):
            with self.assertRaisesRegex(RuntimeError, 'running'):
                offline_vacuum(self.root, self.root / 'backups')
        self.assertFalse((self.root / 'backups').exists())

    def test_vacuum_creates_verified_backup_and_keeps_every_record(self):
        before = database_inventory(self.db)
        self.db.close()
        result = offline_vacuum(self.root, self.root / 'backups')
        self.db = sqlite3.connect(self.root / 'monitor.sqlite3')
        self.assertEqual(before, database_inventory(self.db))
        self.assertEqual(before, result['inventory'])
        self.assertTrue(Path(result['backup']['backup']).is_file())


class ReconciliationAcceptance(unittest.TestCase):
    def test_streams_inline_and_shared_ids_and_detects_missing_duplicate_unexpected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = sample_database(root / 'source.sqlite3')
            source.close()
            workbook(root / 'first.xlsx', ['a'], shared=True)
            workbook(root / 'second.xlsx', ['a', 'outside'])
            result = soak.reconcile(root / 'source.sqlite3', [root / 'first.xlsx', root / 'second.xlsx'], root / 'audit')
            self.assertEqual((1, 1, 1), (result['missing'], result['duplicate_ids'], result['unexpected']))
            self.assertFalse(result['passed'])

    def test_exact_signal_id_coverage_passes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = sample_database(root / 'source.sqlite3')
            source.close()
            workbook(root / 'correct.xlsx', ['a', 'b'])
            result = soak.reconcile(root / 'source.sqlite3', [root / 'correct.xlsx'], root / 'audit')
            self.assertTrue(result['passed'])
            self.assertEqual(2, result['actual_unique'])


class SoakAcceptance(unittest.TestCase):
    def health(self, stamp):
        return {'time': stamp, 'running': True, 'pid': 123456789, 'export_health': {'by_interval': {}}}

    def test_healthy_timestamp_alone_cannot_pass_72_hours(self):
        state = {'continuous_start_ms': 1, 'last_sample_ms': 72 * 3600_000 + 1, 'samples': 4321}
        self.assertEqual('INSUFFICIENT_EXPORT_EVIDENCE', soak.summarize_soak(state)['status'])

    def test_restart_and_sampling_gap_reset_continuous_window(self):
        with patch.object(soak, 'rss_bytes', return_value=10):
            state = soak.observe_sample({}, self.health(1000), 1000)
            state = soak.observe_sample(state, self.health(301000), 301000)
            self.assertEqual(301000, state['continuous_start_ms'])
            changed = dict(self.health(361000), pid=98765)
            state = soak.observe_sample(state, changed, 361000)
            self.assertEqual(361000, state['continuous_start_ms'])
            self.assertEqual(2, len(state['interruptions']))

    def test_repeated_health_does_not_count_same_export_twice(self):
        health = self.health(1000)
        health['export_health']['by_interval']['15m'] = {'last_success_ms': 1000, 'duration_ms': 30,
            'peak_rss_bytes': 100, 'report_boundary': 0, 'coverage': {'expected': 524, 'completed': 524}}
        with patch.object(soak, 'rss_bytes', return_value=10):
            state = soak.observe_sample({}, health, 1000)
            state = soak.observe_sample(state, dict(health, time=61000), 61000)
        self.assertEqual(1, len(state['exports']['15m']))

    def test_short_smoke_is_not_72_hour_pass(self):
        state = {'continuous_start_ms': 1, 'last_sample_ms': 3600001, 'samples': 61}
        self.assertEqual('OBSERVING', soak.summarize_soak(state)['status'])

    def test_repaired_same_close_is_not_counted_as_two_or_as_incomplete(self):
        first = {'time': 40000, 'boundary': 1000, 'reason': 'CANDLE_CLOSE', 'duration_ms': 20,
                 'peak_rss_bytes': 100, 'close_latency_ms': 39000,
                 'coverage': {'expected': 524, 'completed': 500}}
        repaired = dict(first, time=120000, close_latency_ms=119000,
                        coverage={'expected': 524, 'completed': 524})
        state = {'continuous_start_ms': 1, 'last_sample_ms': 300000,
                 'exports': {'15m': [first, repaired]}}
        result = soak.summarize_soak(state)['by_interval']['15m']
        self.assertEqual(2, result['exports_observed'])
        self.assertEqual(1, result['distinct_closes'])
        self.assertEqual(0, result['incomplete_exports'])
        self.assertEqual(0, result['late_exports'])

    def test_recovery_deadline_uses_actual_success_time(self):
        health = self.health(1000)
        with patch.object(soak, 'rss_bytes', return_value=10):
            state = soak.observe_sample({},health,1000)
            for when in (61000,121000,181000):
                state = soak.observe_sample(state,self.health(when),when)
            health = self.health(241000)
            health['export_health']['by_interval']['15m'] = {'last_success_ms':240000,'report_boundary':1000,
                'duration_ms':500,'peak_rss_bytes':100,'coverage':{'expected':524,'completed':524}}
            health['last_export'] = {'by_interval':{'15m':{'reason':'RECOVERY_COMPLETE','complete':True}}}
            state = soak.observe_sample(state,health,241000)
        result = soak.summarize_soak(state)['by_interval']['15m']
        self.assertEqual(1,result['late_exports'])

    def test_parent_memory_growth_is_checked_even_if_each_child_is_fresh(self):
        state = {'continuous_start_ms':1,'last_sample_ms':72*3600_000+1,
                 'producer_samples':[{'rss_bytes':100} for _ in range(12)]+
                                    [{'rss_bytes':200} for _ in range(12)]}
        report = soak.summarize_soak(state)
        self.assertEqual('FAILED',report['status'])
        self.assertEqual(1,report['producer_memory']['trend']['growth_ratio'])

    def test_new_continuous_segment_drops_old_failure_and_parent_samples(self):
        with patch.object(soak,'rss_bytes',return_value=10):
            state = soak.observe_sample({},self.health(1000),1000)
            state['failures'] = [['15m',1000,'NODE_OOM']]
            state = soak.observe_sample(state,dict(self.health(61000),pid=77),61000)
        self.assertEqual([],state['failures'])
        self.assertEqual(1,len(state['producer_samples']))

    def test_code_version_change_resets_continuous_segment(self):
        with patch.object(soak,'rss_bytes',return_value=10):
            state = soak.observe_sample({},dict(self.health(1000),code_hash='old'),1000)
            state = soak.observe_sample(state,dict(self.health(61000),code_hash='new'),61000)
        self.assertEqual(61000,state['continuous_start_ms'])
        self.assertTrue(state['interruptions'][-1]['changed_code'])

    def test_later_publication_does_not_replace_first_complete_close_deadline(self):
        complete = dict(time=30000,boundary=1000,reason='CANDLE_CLOSE',duration_ms=20,
            peak_rss_bytes=100,close_latency_ms=29000,coverage={'expected':524,'completed':524})
        later = dict(complete,time=500000,reason='RECOVERY_COMPLETE',close_latency_ms=499000)
        result = soak.summarize_soak(dict(continuous_start_ms=1,last_sample_ms=600000,
            exports={'15m':[complete,later]}))['by_interval']['15m']
        self.assertEqual(0,result['late_exports'])
        self.assertEqual(1,result['distinct_closes'])


class ShadowAcceptance(unittest.TestCase):
    def test_changed_history_is_drained_between_closes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root/'runtime'
            runtime.mkdir()
            store = Store(runtime/'monitor.sqlite3',{})
            clock = 1792630800000
            store.db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?,?)',
                ('old','v3','TESTUSDT','15m',clock-86400000,'EXPIRED',dumps({'discovered':clock-86400000})))
            store.db.commit()
            runner = shadow.ShadowRunner(root/'unused',store,{'data_dir':str(runtime)})
            runner.started=True
            runner.seen_boundary={i:clock//step*step for i,step in shadow.PERIODS.items()}
            def publication(_store,_config,intervals=None,refresh_reason=None,refresh_boundary=None,**kwargs):
                self.assertTrue(kwargs['backfill'])
                self.assertEqual('BACKFILL',refresh_reason)
                store.db.execute('UPDATE signal_export_periods SET exported_revision=revision')
                store.db.commit()
                store.meta('last_export',{'by_interval':{'15m':dict(time=clock,boundary=refresh_boundary,
                    reason=refresh_reason,coverage=dict(expected=524,completed=524,missing=[]))}})
            try:
                with patch.object(shadow,'now_ms',return_value=clock), \
                     patch.object(shadow,'mirror_snapshot',return_value={}), \
                     patch.object(shadow,'export_all',side_effect=publication) as export:
                    runner.tick()
                self.assertEqual(1,export.call_count)
                self.assertEqual(set(),runner.dirty_intervals())
            finally:
                store.close()

    def test_pending_close_reexports_only_when_coverage_becomes_complete(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root/'runtime'
            runtime.mkdir()
            store = Store(runtime/'monitor.sqlite3',{})
            runner = shadow.ShadowRunner(root/'unused.sqlite3',store,{'data_dir':str(runtime)})
            missing = dict(expected=524,completed=500,missing=['TESTUSDT'])
            complete = dict(expected=524,completed=524,missing=[])
            published_coverage = missing
            clock = 1792630800000
            sequence = 0
            def publication(_store,_config,intervals=None,refresh_reason=None,refresh_boundary=None,**kwargs):
                nonlocal sequence
                sequence += 1
                reports = store.get_meta('last_export',{'by_interval':{}})
                health = store.get_meta('export_health',{'by_interval':{}})
                for interval in intervals or shadow.PERIODS:
                    boundary = refresh_boundary or clock//shadow.PERIODS[interval]*shadow.PERIODS[interval]
                    reports['by_interval'][interval] = dict(time=clock+sequence,boundary=boundary,
                        reason=refresh_reason,coverage=published_coverage)
                    health['by_interval'][interval] = dict(duration_ms=10,peak_rss_bytes=100)
                store.meta('last_export',reports)
                store.meta('export_health',health)
            try:
                with patch.object(shadow,'now_ms',return_value=clock) as now, \
                     patch.object(shadow,'mirror_snapshot',return_value={}), \
                     patch.object(shadow,'export_all',side_effect=publication) as export, \
                     patch.object(shadow,'close_coverage',return_value=missing) as coverage:
                    runner.tick()
                    self.assertEqual(1,export.call_count)
                    runner.tick()
                    self.assertEqual(1,export.call_count)
                    coverage.return_value=complete
                    published_coverage=complete
                    runner.tick()
                    self.assertEqual(5,export.call_count)
                    self.assertFalse(any(runner.pending.values()))
                    self.assertTrue(all(call.kwargs['refresh_reason']=='RECOVERY_COMPLETE'
                                        for call in export.call_args_list[1:]))
                    runner.tick()
                    self.assertEqual(5,export.call_count)
                    # A completed source read cannot erase the fact that the
                    # just-published file used an earlier incomplete snapshot.
                    published_coverage=missing
                    now.return_value=clock+900000
                    runner.tick()
                    waiting=runner.pending['15m']
                    self.assertEqual({clock+900000},waiting)
                    now.return_value=clock+1800000
                    runner.tick()
                    self.assertEqual({clock+900000,clock+1800000},runner.pending['15m'])
                    restored=shadow.ShadowRunner(root/'unused.sqlite3',store,{'data_dir':str(runtime)})
                    self.assertEqual(runner.pending['15m'],restored.pending['15m'])
            finally:
                store.close()

    def test_read_only_source_mirror_preserves_payloads_and_does_not_dirty_unchanged_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = Store(root/'source.sqlite3',{})
            target = Store(root/'shadow.sqlite3',{})
            try:
                payload = dumps(dict(discovered=1792630800000,bar={'T':1792630799999},metrics={'trigger':True}))
                source.db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?,?)',
                    ('id1','legacy','TESTUSDT','15m',1792629900000,'EXPIRED',payload))
                source.db.execute('INSERT INTO outbox VALUES(?,?,?,?,?,?,?)',('id1',payload,'PENDING',0,0,None,None))
                source.db.commit()
                source.anomaly('TESTUSDT','15m','EARLY','original')
                before = source.db.total_changes
                first = shadow.mirror_snapshot(root/'source.sqlite3',target,{},page_size=1)
                revision = target.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0]
                target.anomaly('','', 'LOCAL_EXPORT_ERROR','local')
                second = shadow.mirror_snapshot(root/'source.sqlite3',target,{},page_size=1)
                self.assertEqual(revision,target.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0])
                self.assertEqual(payload,target.db.execute('SELECT payload FROM signals').fetchone()[0])
                self.assertEqual(before,source.db.total_changes)
                self.assertEqual((1,0),(first['counts']['anomalies_new'],second['counts']['anomalies_new']))
                source.anomaly('TESTUSDT','15m','LATER','retained')
                source.db.execute("UPDATE outbox SET status='SENT',sent=1 WHERE id='id1'")
                source.db.commit()
                shadow.mirror_snapshot(root/'source.sqlite3',target,{},page_size=1)
                self.assertGreater(target.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0],revision)
                self.assertEqual(3,target.db.execute('SELECT count(*) FROM anomalies').fetchone()[0])
                self.assertEqual(2,target.db.execute('SELECT count(*) FROM shadow_anomaly_source').fetchone()[0])
                self.assertEqual(0,target.db.execute('SELECT count(*) FROM bars').fetchone()[0])
            finally:
                source.close()
                target.close()

    def test_shadow_refuses_using_source_as_destination(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'same.sqlite3'
            store = Store(path,{})
            try:
                with self.assertRaisesRegex(ValueError,'different'):
                    shadow.mirror_snapshot(path,store,{})
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
