import datetime as dt
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from monitor.archive import DAY, report_period
from monitor.core import SCHEMA, Store, dumps
from monitor.export_index import ExportIndex, initialize_export_index


def stamp(value):
    return int(dt.datetime.fromisoformat(value).replace(tzinfo=dt.timezone.utc).timestamp()*1000)


NOW = stamp('2026-10-22T01:00:00')


def add(db, name, when=NOW, interval='15m', event='REALTIME', extra=None):
    value = dict(signal_id=name,interval=interval,symbol=name+'USDT',discovered=when,
                 bar={'T':when-1},event_type=event,metrics={'checks':[True]*6})
    if extra:
        value.update(extra)
    db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?,?)',
               (name,'v3-fixed',name+'USDT',interval,when,event,dumps(value)))
    return value


class ExportIndexTests(unittest.TestCase):
    def setUp(self):
        self.store = Store(':memory:',{})
        self.db = self.store.db
        self.index = ExportIndex(self.db)

    def tearDown(self):
        self.db.close()

    def test_generated_discovery_preserves_legacy_positional_inserts_and_payload(self):
        original = add(self.db,'one')
        row = self.db.execute('SELECT discovered,payload FROM signals').fetchone()
        self.assertEqual(row['discovered'],NOW)
        self.assertEqual(json.loads(row['payload']),original)
        self.assertEqual(self.index.read_signal('one')['metrics'],original['metrics'])

    def test_discovery_fallbacks_and_zero_are_preserved(self):
        for name,extra,expected in [
            ('d',{'discovered':0,'processed':NOW},0),
            ('p',{'discovered':None,'processed':NOW+1},NOW+1),
            ('b',{'discovered':None,'processed':None,'bar':{'T':NOW+2}},NOW+3),
        ]:
            add(self.db,name,extra=extra)
            self.assertEqual(self.db.execute('SELECT discovered FROM signals WHERE id=?',(name,)).fetchone()[0],expected)

    def test_sql_periods_match_calendar_before_and_after_anchor_and_month_year(self):
        for i,when in enumerate([0,stamp('2024-01-01T00:00:00'),
                                stamp('2026-10-19T00:00:00')-1,stamp('2026-10-19T00:00:00'),
                                stamp('2026-11-02T00:00:00'),stamp('2026-12-31T23:59:59'),
                                stamp('2027-01-01T00:00:00')]):
            for interval in ('15m','1h','4h','1d'):
                add(self.db,f'{i}-{interval}',when,interval)
                start,end = report_period(interval,when)
                row = self.db.execute('SELECT end FROM signal_export_periods WHERE interval=? AND start=?',(interval,start)).fetchone()
                self.assertIsNotNone(row,(interval,when))
                self.assertEqual(row[0],end)

    def test_plan_skips_successful_unchanged_history_without_reading_payload(self):
        add(self.db,'old',NOW-2*DAY)
        add(self.db,'new',NOW)
        plans = self.index.plan('15m',NOW)
        self.assertEqual(len(plans),2)
        old = next(p for p in plans if not p['current'])
        self.index.mark_exported('15m',old['start'],old['revision'],{'row_count':1})
        reads = []
        def authorizer(action, table, column, database, trigger):
            if action==sqlite3.SQLITE_READ and table=='signals':
                reads.append(column)
            return sqlite3.SQLITE_OK
        self.db.set_authorizer(authorizer)
        self.assertEqual(len(self.index.plan('15m',NOW)),1)
        self.db.set_authorizer(None)
        self.assertNotIn('payload',reads)

    def test_late_payload_update_reopens_only_affected_period_and_clear_hash(self):
        add(self.db,'old',NOW-2*DAY)
        old = self.index.plan('15m',NOW)[1]
        self.index.mark_exported('15m',old['start'],old['revision'],{'row_count':1})
        header = next(self.index.iter_signal_headers('15m',old['start'],old['end']))
        self.index.set_content_hash('old',header['revision'],'known')
        self.db.execute("UPDATE signals SET payload=json_set(payload,'$.extra','changed') WHERE id='old'")
        changed = self.index.plan('15m',NOW)[1]
        self.assertGreater(changed['revision'],old['revision'])
        newer = next(self.index.iter_signal_headers('15m',old['start'],old['end']))
        self.assertIsNone(newer['content_hash'])
        self.index.set_content_hash('old',header['revision'],'stale')
        self.assertIsNone(next(self.index.iter_signal_headers('15m',old['start'],old['end']))['content_hash'])

    def test_notification_update_changes_revision_but_noop_and_next_attempt_do_not(self):
        add(self.db,'old',NOW-2*DAY)
        self.db.execute("INSERT INTO outbox(id,payload,status,next_attempt) VALUES('old','{}','PENDING',0)")
        original = self.index.plan('15m',NOW)[1]
        self.index.mark_exported('15m',original['start'],original['revision'],{})
        self.db.execute("UPDATE outbox SET next_attempt=100,status='PENDING' WHERE id='old'")
        self.assertEqual(len(self.index.plan('15m',NOW)),1)
        self.db.execute("UPDATE outbox SET status='SENT',sent=10,attempts=1 WHERE id='old'")
        changed = self.index.plan('15m',NOW)[1]
        self.assertGreater(changed['revision'],original['revision'])
        self.assertEqual(self.index.read_signal('old')['notification_status'],'SENT')
        self.assertEqual(self.index.read_signal('old')['sent'],10)
        self.assertEqual(self.index.read_signal('old')['attempts'],1)

    def test_signal_moved_to_another_period_invalidates_both_and_daily_summary(self):
        add(self.db,'moved',NOW-2*DAY)
        first = self.index.plan('15m',NOW)[1]
        self.index.mark_exported('15m',first['start'],first['revision'],{})
        self.db.execute("UPDATE signals SET payload=json_set(payload,'$.discovered',?) WHERE id='moved'",(NOW,))
        plans = self.index.plan('15m',NOW)
        self.assertEqual([p['signal_count'] for p in plans],[1,0])
        counts = dict(self.db.execute('SELECT day,signal_count FROM signals_summary_daily'))
        self.assertEqual(counts[first['start']],0)
        self.assertEqual(counts[plans[0]['start']],1)

    def test_signal_delete_invalidates_empty_archive_and_daily_count(self):
        add(self.db,'removed',NOW-2*DAY)
        old = self.index.plan('15m',NOW)[1]
        self.index.mark_exported('15m',old['start'],old['revision'],{})
        self.db.execute("DELETE FROM signals WHERE id='removed'")
        self.assertEqual(self.index.plan('15m',NOW)[1]['signal_count'],0)
        self.assertEqual(self.db.execute('SELECT sum(signal_count) FROM signals_summary_daily').fetchone()[0],0)

    def test_keyset_pagination_tied_discovery_returns_all_ids_once_in_order(self):
        for n in range(37):
            add(self.db,f'item-{n:03}',NOW+(n%3))
        start,end = report_period('15m',NOW)
        got = list(self.index.iter_signal_headers('15m',start,end,page_size=4))
        self.assertEqual(len(got),37)
        self.assertEqual(len({x['id'] for x in got}),37)
        self.assertEqual([(x['discovered'],x['id']) for x in got],sorted((x['discovered'],x['id']) for x in got))
        query_plan = '\n'.join(str(tuple(r)) for r in self.db.execute('''EXPLAIN QUERY PLAN SELECT s.id,s.discovered
            FROM signals s WHERE interval=? AND discovered>=? AND discovered<?
            AND (discovered,id)>(?,?) ORDER BY discovered,id LIMIT ?''',('15m',start,end,start-1,'',4)))
        self.assertIn('signal_discovery_export',query_plan)
        self.assertNotIn('USE TEMP B-TREE',query_plan)

    def test_revision_filter_and_durable_ack_never_hide_later_changes(self):
        add(self.db,'a',NOW-2*DAY)
        first = self.index.plan('15m',NOW)[1]
        add(self.db,'b',NOW-2*DAY)
        self.index.mark_exported('15m',first['start'],first['revision'],{'row_count':1})
        second = self.index.plan('15m',NOW)[1]
        self.assertGreater(second['revision'],second['exported_revision'])
        self.assertEqual([r['id'] for r in self.index.iter_signal_headers('15m',first['start'],first['end'],
                                                                        changed_since_revision=first['revision'])],['b'])
        self.index.mark_exported('15m',first['start'],second['revision'],{'row_count':2})
        self.index.mark_exported('15m',first['start'],first['revision'],{'row_count':1})
        self.assertEqual(next(self.index.manifest('15m'))['row_count'],2)

    def test_recent_anomalies_are_bounded_aggregated_and_ids_not_invented(self):
        records = [(NOW-DAY-1,'A','15m','OLD','old'),
                   (NOW-100,'A','15m','RETRY','same'),(NOW-10,'A','15m','RETRY','same'),
                   (NOW-5,'B','15m','ERROR','text signal_id=pretend'),
                   (NOW-2,'C','','GLOBAL','{"signal_id":"proven"}'),
                   (NOW+1,'D','15m','FUTURE','future'),(NOW-1,'E','1h','OTHER','other')]
        self.db.executemany('INSERT INTO anomalies(time,symbol,interval,code,detail) VALUES(?,?,?,?,?)',records)
        result = self.index.recent_anomalies(NOW,'15m',limit=2)
        self.assertTrue(result['truncated'])
        self.assertEqual(result['total_groups'],3)
        self.assertEqual(result['raw_count'],4)
        self.assertEqual(result['group_count'],2)
        self.assertEqual(result['groups'][0]['signal_id'],'proven')
        self.assertIsNone(result['groups'][1]['signal_id'])
        full = self.index.recent_anomalies(NOW,'15m')
        repeated = next(g for g in full['groups'] if g['code']=='RETRY')
        self.assertEqual((repeated['first_time'],repeated['last_time'],repeated['occurrence_count']),(NOW-100,NOW-10,2))
        self.assertEqual(self.db.execute('SELECT count(*) FROM anomalies').fetchone()[0],7)

    def test_incremental_anomaly_archive_uses_id_watermark_including_late_timestamp(self):
        self.db.executemany('INSERT INTO anomalies(time,code) VALUES(?,?)',[(NOW,'a'),(NOW,'b'),(NOW-DAY,'late')])
        first = list(self.index.iter_anomalies_after(0,until=2,page_size=1))
        second = list(self.index.iter_anomalies_after(first[-1]['id'],page_size=1))
        self.assertEqual([r['id'] for r in first+second],[1,2,3])
        self.assertEqual(second[0]['code'],'late')

    def test_transaction_rollback_rolls_back_all_report_revisions(self):
        before = self.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0]
        self.db.execute('BEGIN')
        add(self.db,'rolled-back')
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0],before)
        self.assertEqual(self.db.execute('SELECT count(*) FROM signal_export_index').fetchone()[0],0)

    def test_reopen_migration_does_not_read_payload_or_change_revisions(self):
        add(self.db,'a')
        before = self.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0]
        seen = []
        self.db.set_trace_callback(seen.append)
        initialize_export_index(self.db)
        self.db.set_trace_callback(None)
        self.assertFalse(any('FROM signals' in sql for sql in seen))
        self.assertEqual(self.db.execute('SELECT revision FROM export_revision_clock').fetchone()[0],before)

    def test_initial_backfill_preserves_payload_and_includes_all_historical_periods(self):
        old = sqlite3.connect(':memory:')
        old.row_factory = sqlite3.Row
        old.executescript(SCHEMA)
        p = add(old,'legacy',NOW-50*DAY)
        add(old,'latest',NOW)
        old.commit()
        initialize_export_index(old)
        idx = ExportIndex(old)
        self.assertEqual(len(idx.plan('15m',NOW)),2)
        self.assertEqual(json.loads(old.execute("SELECT payload FROM signals WHERE id='legacy'").fetchone()[0]),p)
        self.assertEqual(old.execute('SELECT sum(signal_count) FROM signals_summary_daily').fetchone()[0],2)
        old.close()

    def test_readonly_snapshot_and_writer_revision_ack_interleave_safely(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'test.sqlite3'
            store = Store(path,{})
            add(store.db,'a',NOW-2*DAY)
            store.db.commit()
            reader = sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
            reader.row_factory = sqlite3.Row
            reader.execute('BEGIN')
            read_index = ExportIndex(reader)
            plan = read_index.plan('15m',NOW)[1]
            add(store.db,'b',NOW-2*DAY)
            store.db.commit()
            self.assertEqual(len(list(read_index.iter_signal_headers('15m',plan['start'],plan['end']))),1)
            reader.close()
            write_index = ExportIndex(store.db)
            write_index.mark_exported('15m',plan['start'],plan['revision'],{'row_count':1})
            store.db.commit()
            self.assertEqual(write_index.plan('15m',NOW)[1]['signal_count'],2)
            store.db.close()


if __name__=='__main__':
    unittest.main()
