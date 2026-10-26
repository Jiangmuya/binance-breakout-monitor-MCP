"""Canonical membership and rename-journal recovery, including actual crashes."""
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from monitor import archive
from test_history_maintenance import TestStore


class ProcessLoss(BaseException):
    pass


class ArchiveRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = archive.resolved(self.temp.name)
        self.store = TestStore(self.root/'monitor.sqlite3')
        self.config = dict(excel_dir=str(self.root/'excel'))
        self.directory = Path(self.config['excel_dir'])
        self.directory.mkdir()
        self.old_start = 1792627200000
        self.next_start = self.old_start+archive.DAY
        self.prefix = '币安USDT合约监控_15分钟'
        self.source = self.directory/(self.prefix+'.xlsx')
        self.source.write_text(json.dumps({'triggers':['signal-1']}), encoding='utf-8')
        self.target = archive.archive_target(self.directory,
            self.prefix+'_'+archive.period_label(self.old_start,self.next_start)+'.xlsx')
        self.target.parent.mkdir()
        self.store.meta('excel_archive_15m',dict(first_start=self.old_start,current_start=self.old_start,
            current_signature='1',files=[str(self.source)],archived={},pending_moves=[]))
        self.canonical([self.source], 1)

    def tearDown(self):
        self.store.db.close()
        self.assertEqual(archive.resolved(self.temp.name), self.root)
        shutil.rmtree(archive._io(self.root))
        self.temp.cleanup()

    def canonical(self, files, count):
        value=dict(files=[str(p) for p in files],row_count=count,ids_sha256='preserved-id-hash')
        with self.store.db:
            self.store.db.execute('INSERT OR REPLACE INTO signals_export_metadata VALUES(?,?,?,?,?,?)',
                ('15m',self.old_start,17,self.old_start,count,json.dumps(value)))

    def manifest(self):
        return json.loads(self.store.db.execute('SELECT manifest FROM signals_export_metadata WHERE interval=? AND start=?',
            ('15m',self.old_start)).fetchone()[0])

    def begin(self):
        return archive.begin_publication(self.store,self.config,'15m','15分钟',
            self.next_start,self.next_start+archive.DAY,self.next_start)

    def test_rollover_moves_only_existing_canonical_membership(self):
        snapshot=self.directory/(self.prefix+'_snapshot_123.xlsx')
        snapshot.write_text(json.dumps({'triggers':['signal-1','signal-2']}),encoding='utf-8')
        state=self.store.get_meta('excel_archive_15m')
        state.update(files=[str(self.source),str(snapshot)],current_signature='2')
        self.store.meta('excel_archive_15m',state)
        self.canonical([snapshot],2)
        result=self.begin()
        canonical=self.manifest()
        self.assertEqual(len(canonical['files']),1)
        self.assertEqual(canonical['row_count'],2)
        self.assertEqual(canonical['ids_sha256'],'preserved-id-hash')
        values=json.loads(Path(canonical['files'][0]).read_text(encoding='utf-8'))['triggers']
        self.assertEqual(values,['signal-1','signal-2'])
        self.assertEqual(len(result['archived'][str(self.old_start)]['files']),2)
        self.assertTrue(all(move['completed'] for move in result['archived'][str(self.old_start)]['moves']))

    def test_collision_destination_is_durable_before_rename_and_recovers(self):
        self.target.write_bytes(b'old different workbook')
        original=os.replace
        def rename_then_crash(source,target):
            journal=self.store.get_meta('excel_archive_15m')['pending_moves'][0]
            self.assertEqual(archive.resolved(target),archive.resolved(journal['target']))
            original(source,target)
            raise ProcessLoss()
        with patch('monitor.archive.os.replace',side_effect=rename_then_crash):
            with self.assertRaises(ProcessLoss):self.begin()
        journal=self.store.get_meta('excel_archive_15m')['pending_moves'][0]
        actual=Path(journal['target'])
        self.assertIn('_快照_',actual.name)
        self.assertTrue(actual.exists())
        self.assertFalse(self.source.exists())
        self.assertEqual(self.manifest()['files'],[str(self.source)])
        result=self.begin()
        self.assertEqual(result['pending_moves'],[])
        self.assertEqual(self.manifest()['files'],[str(actual)])
        self.assertEqual(self.target.read_bytes(),b'old different workbook')

    def test_legacy_collision_journal_recovers_by_expected_hash(self):
        expected=archive._file_hash(self.source)
        actual=self.target.with_name(self.target.stem+'_快照_'+expected[:16]+'.xlsx')
        self.source.replace(actual)
        self.target.write_bytes(b'unrelated old target')
        state=self.store.get_meta('excel_archive_15m')
        state['pending_moves']=[dict(source=str(self.source),target=str(self.target),expected_hash=expected)]
        self.store.meta('excel_archive_15m',state)
        result=self.begin()
        self.assertEqual(result['pending_moves'],[])
        self.assertEqual(self.manifest()['files'],[str(actual)])

    def test_stale_journal_preserves_reused_source_and_maps_old_period(self):
        expected=archive._file_hash(self.source)
        self.source.replace(self.target)
        self.source.write_bytes(b'new period workbook')
        state=self.store.get_meta('excel_archive_15m')
        state.update(current_start=self.next_start,pending_moves=[dict(source=str(self.source),target=str(self.target),
            expected_hash=expected,period_start=self.old_start,period_signature='1')])
        self.store.meta('excel_archive_15m',state)
        self.begin()
        self.assertEqual(self.source.read_bytes(),b'new period workbook')
        self.assertEqual(self.manifest()['files'],[str(self.target)])

    def test_missing_source_and_destination_are_not_marked_complete(self):
        expected=archive._file_hash(self.source)
        state=self.store.get_meta('excel_archive_15m')
        state['pending_moves']=[dict(source=str(self.source),target=str(self.target),expected_hash=expected,
            period_start=self.old_start,period_signature='1')]
        self.store.meta('excel_archive_15m',state)
        self.source.unlink()
        result=self.begin()
        self.assertEqual(len(result['pending_moves']),1)
        self.assertFalse(result['pending_moves'][0]['completed'])
        self.assertEqual(self.manifest()['files'],[str(self.source)])

    def test_locked_move_defers_canonical_mapping_then_retries(self):
        with patch('monitor.archive.os.replace',side_effect=PermissionError('Excel occupied')):
            result=self.begin()
        self.assertEqual(len(result['pending_moves']),1)
        self.assertEqual(self.manifest()['files'],[str(self.source)])
        self.assertTrue(self.source.exists())
        result=self.begin()
        self.assertEqual(result['pending_moves'],[])
        self.assertEqual(self.manifest()['files'],[str(self.target)])

    def test_canonical_mapping_and_verified_state_are_one_transaction(self):
        # An injected failure after the manifest UPDATE must also roll it back.
        self.store.db.execute('''CREATE TRIGGER fail_verified_state BEFORE INSERT ON metadata
            WHEN NEW.key='excel_archive_15m' AND instr(NEW.value,'"completed":true')>0
            BEGIN SELECT RAISE(ABORT,'simulated state commit failure'); END''')
        self.store.db.commit()
        with self.assertRaisesRegex(Exception,'state commit failure'):
            self.begin()
        self.assertFalse(self.source.exists())
        self.assertTrue(self.target.exists())
        self.assertEqual(self.manifest()['files'],[str(self.source)])
        self.assertFalse(self.store.get_meta('excel_archive_15m')['pending_moves'][0]['completed'])
        self.store.db.execute('DROP TRIGGER fail_verified_state');self.store.db.commit()
        self.begin()
        self.assertEqual(self.manifest()['files'],[str(self.target)])


if __name__=='__main__':unittest.main()
