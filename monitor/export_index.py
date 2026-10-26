"""Indexed report membership and durable change tracking, independent of strategy.

All clocks/revisions are maintained by SQLite triggers in the writer transaction.
An export acknowledges only the revision it read, so a later change cannot be
lost even when publication and market processing overlap. No history is deleted.
"""
from __future__ import annotations

import hashlib
import json
import time

from .archive import ANCHOR, DAY, report_period

SCHEMA_VERSION = 1
INTERVALS = ('15m', '1h', '4h', '1d')
NOW_SQL = "CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)"


def _period_start(alias):
    value = f'{alias}.discovered'
    day = f'(({value}/{DAY})*{DAY})'
    return (f"CASE {alias}.interval WHEN '15m' THEN {day} "
            f"WHEN '1h' THEN ((({day}+{3*DAY})/{7*DAY})*{7*DAY}-{3*DAY}) "
            f"WHEN '4h' THEN ({ANCHOR}+CAST(floor(({value}-{ANCHOR})*1.0/{14*DAY}) AS INTEGER)*{14*DAY}) "
            f"WHEN '1d' THEN CAST(strftime('%s',{value}/1000,'unixepoch','start of month') AS INTEGER)*1000 END")


def _period_end(alias):
    start = _period_start(alias)
    return (f"CASE {alias}.interval WHEN '15m' THEN ({start})+{DAY} "
            f"WHEN '1h' THEN ({start})+{7*DAY} WHEN '4h' THEN ({start})+{14*DAY} "
            f"WHEN '1d' THEN CAST(strftime('%s',{alias}.discovered/1000,'unixepoch','start of month','+1 month') AS INTEGER)*1000 END")


def _dirty(alias):
    return f"""INSERT INTO signal_export_periods(interval,start,end,revision)
        SELECT {alias}.interval,{_period_start(alias)},{_period_end(alias)},revision
        FROM export_revision_clock WHERE id=1 AND {alias}.interval IN ('15m','1h','4h','1d')
        ON CONFLICT(interval,start) DO UPDATE SET revision=excluded.revision;"""


def _daily(alias, delta):
    return f"""INSERT INTO signals_summary_daily(interval,day,signal_count,realtime_count,missed_count,expired_count)
        VALUES({alias}.interval,({alias}.discovered/{DAY})*{DAY},{delta},
               {delta}*({alias}.event_type='REALTIME'),{delta}*({alias}.event_type='MISSED'),
               {delta}*({alias}.event_type='EXPIRED'))
        ON CONFLICT(interval,day) DO UPDATE SET
            signal_count=signal_count+excluded.signal_count,
            realtime_count=realtime_count+excluded.realtime_count,
            missed_count=missed_count+excluded.missed_count,
            expired_count=expired_count+excluded.expired_count;"""


def initialize_export_index(db):
    """One atomic initial backfill. Reopening a migrated DB never scans payloads.

    The generated column deliberately preserves the original seven-column
    INSERT INTO signals VALUES(...) contract. Revision/hash data lives beside
    it; signal payload and historical decisions are never rewritten.
    """
    db.execute('CREATE INDEX IF NOT EXISTS evaluation_export_close ON evaluations(interval,t,symbol,version)')
    found = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='export_index_schema'").fetchone()
    if found and db.execute('SELECT version FROM export_index_schema WHERE id=1').fetchone()[0] == SCHEMA_VERSION:
        return
    if db.in_transaction:
        raise RuntimeError('Export index migration requires a transaction boundary')
    db.execute('BEGIN IMMEDIATE')
    try:
        columns = {r[1] for r in db.execute('PRAGMA table_xinfo(signals)')}
        if 'discovered' not in columns:
            db.execute('''ALTER TABLE signals ADD COLUMN discovered INTEGER GENERATED ALWAYS AS (
                CAST(COALESCE(CASE WHEN json_valid(payload) THEN json_extract(payload,'$.discovered') END,
                              CASE WHEN json_valid(payload) THEN json_extract(payload,'$.processed') END,
                              CASE WHEN json_valid(payload) THEN json_extract(payload,'$.bar.T')+1 END,
                              t+CASE interval WHEN '15m' THEN 900000 WHEN '1h' THEN 3600000
                                    WHEN '4h' THEN 14400000 WHEN '1d' THEN 86400000 ELSE 1 END) AS INTEGER)) VIRTUAL''')
        statements = [
            'CREATE TABLE export_index_schema(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL)',
            'CREATE TABLE export_revision_clock(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL)',
            'INSERT INTO export_revision_clock VALUES(1,1)',
            '''CREATE TABLE signal_export_index(id TEXT PRIMARY KEY,revision INTEGER NOT NULL,
                 updated_ms INTEGER NOT NULL,content_hash TEXT)''',
            '''CREATE TABLE signal_export_periods(interval TEXT,start INTEGER,end INTEGER NOT NULL,
                 revision INTEGER NOT NULL,exported_revision INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY(interval,start)) WITHOUT ROWID''',
            '''CREATE TABLE signals_summary_daily(interval TEXT,day INTEGER,signal_count INTEGER NOT NULL,
                 realtime_count INTEGER NOT NULL,missed_count INTEGER NOT NULL,expired_count INTEGER NOT NULL,
                 PRIMARY KEY(interval,day)) WITHOUT ROWID''',
            '''CREATE TABLE signals_export_metadata(interval TEXT,start INTEGER,revision INTEGER NOT NULL,
                 exported_ms INTEGER NOT NULL,row_count INTEGER NOT NULL,manifest TEXT NOT NULL,
                 PRIMARY KEY(interval,start)) WITHOUT ROWID''',
            'CREATE INDEX signal_discovery_export ON signals(interval,discovered,id)',
            'CREATE INDEX anomaly_time_id ON anomalies(time,id)',
            'CREATE INDEX anomaly_interval_time_id ON anomalies(interval,time,id)',
            '''CREATE INDEX anomaly_signal_time ON anomalies(
                CASE WHEN json_valid(detail) THEN json_extract(detail,'$.signal_id') END,time,id)''',
        ]
        for sql in statements:
            db.execute(sql)
        db.execute(f'INSERT INTO signal_export_index SELECT id,1,{NOW_SQL},NULL FROM signals')
        db.execute(f'''INSERT INTO signal_export_periods(interval,start,end,revision)
            SELECT s.interval,{_period_start('s')},{_period_end('s')},1 FROM signals s
            WHERE s.interval IN ('15m','1h','4h','1d') GROUP BY s.interval,{_period_start('s')}''')
        db.execute(f'''INSERT INTO signals_summary_daily
            SELECT interval,(discovered/{DAY})*{DAY},count(*),sum(event_type='REALTIME'),
                   sum(event_type='MISSED'),sum(event_type='EXPIRED') FROM signals GROUP BY interval,(discovered/{DAY})*{DAY}''')
        increment = 'UPDATE export_revision_clock SET revision=revision+1 WHERE id=1;'
        touch = f'''INSERT INTO signal_export_index(id,revision,updated_ms,content_hash)
            SELECT NEW.id,revision,{NOW_SQL},NULL FROM export_revision_clock WHERE id=1
            ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,updated_ms=excluded.updated_ms,content_hash=NULL;'''
        db.execute(f'''CREATE TRIGGER export_signal_insert AFTER INSERT ON signals BEGIN
            {increment}{touch}{_dirty('NEW')}{_daily('NEW',1)} END''')
        db.execute(f'''CREATE TRIGGER export_signal_update AFTER UPDATE ON signals
            WHEN OLD.payload IS NOT NEW.payload OR OLD.id IS NOT NEW.id OR OLD.interval IS NOT NEW.interval
              OR OLD.t IS NOT NEW.t OR OLD.event_type IS NOT NEW.event_type OR OLD.symbol IS NOT NEW.symbol
              OR OLD.version IS NOT NEW.version BEGIN
            {increment} DELETE FROM signal_export_index WHERE id=OLD.id AND OLD.id!=NEW.id;
            {touch}{_dirty('OLD')}{_dirty('NEW')}{_daily('OLD',-1)}{_daily('NEW',1)} END''')
        db.execute(f'''CREATE TRIGGER export_signal_delete AFTER DELETE ON signals BEGIN
            {increment}{_dirty('OLD')}{_daily('OLD',-1)} DELETE FROM signal_export_index WHERE id=OLD.id; END''')
        for operation, alias, condition in [
            ('INSERT','NEW',''), ('DELETE','OLD',''),
            ('UPDATE','NEW', '''WHEN OLD.status IS NOT NEW.status OR OLD.sent IS NOT NEW.sent
                  OR OLD.attempts IS NOT NEW.attempts OR OLD.error IS NOT NEW.error OR OLD.payload IS NOT NEW.payload''')]:
            # Only persisted fields that can affect a report invalidate a signal.
            # A retry's next_attempt alone never causes historical recomputation.
            db.execute(f'''CREATE TRIGGER export_outbox_{operation.lower()} AFTER {operation} ON outbox {condition} BEGIN
                {increment}
                UPDATE signal_export_index SET revision=(SELECT revision FROM export_revision_clock WHERE id=1),
                    updated_ms={NOW_SQL},content_hash=NULL WHERE id={alias}.id;
                INSERT INTO signal_export_periods(interval,start,end,revision)
                    SELECT s.interval,{_period_start('s')},{_period_end('s')},c.revision
                    FROM signals s CROSS JOIN export_revision_clock c WHERE s.id={alias}.id AND c.id=1
                    ON CONFLICT(interval,start) DO UPDATE SET revision=excluded.revision;
                END''')
        db.execute('INSERT INTO export_index_schema VALUES(1,?)',(SCHEMA_VERSION,))
        db.commit()
    except BaseException:
        db.rollback()
        raise


class ExportIndex:
    """Connection-local helper; callers own their read transaction and Store lock."""
    def __init__(self, db):
        self.db = db

    def plan(self, interval, stamp):
        start, end = report_period(interval,stamp)
        rows = [dict(r) for r in self.db.execute('''SELECT interval,start,end,revision,exported_revision
             FROM signal_export_periods WHERE interval=? AND start<=?
             AND (start=? OR revision>exported_revision) ORDER BY start''',(interval,start,start))]
        if not any(r['start']==start for r in rows):
            rows.append(dict(interval=interval,start=start,end=end,revision=0,exported_revision=0))
        for row in rows:
            row['current'] = row['start']==start
            row['signal_count'] = self.db.execute('''SELECT count(*) FROM signals
                WHERE interval=? AND discovered>=? AND discovered<?''',(interval,row['start'],row['end'])).fetchone()[0]
        return sorted(rows,key=lambda r:(not r['current'],r['start']))

    def iter_signal_headers(self, interval, start, end, page_size=500, changed_since_revision=None):
        if not 1 <= page_size <= 5000:
            raise ValueError('page_size must be between 1 and 5000')
        after = (start-1,'')
        revision_sql = ' AND x.revision>?' if changed_since_revision is not None else ''
        while True:
            args = [interval,start,end,*after]
            if changed_since_revision is not None:
                args.append(changed_since_revision)
            args.append(page_size)
            rows = self.db.execute(f'''SELECT s.id,s.discovered,x.revision,x.updated_ms,x.content_hash
                FROM signals s JOIN signal_export_index x ON x.id=s.id
                WHERE s.interval=? AND s.discovered>=? AND s.discovered<? AND (s.discovered,s.id)>(?,?)
                {revision_sql} ORDER BY s.discovered,s.id LIMIT ?''',args).fetchall()
            if not rows:
                break
            for row in rows:
                yield dict(row)
            after = (rows[-1]['discovered'],rows[-1]['id'])

    def read_signal(self, signal_id):
        row = self.db.execute('''SELECT s.payload,o.status,o.sent,o.attempts FROM signals s
            LEFT JOIN outbox o ON o.id=s.id WHERE s.id=?''',(signal_id,)).fetchone()
        if row is None:
            return None
        value = json.loads(row['payload'])
        value.update(notification_status=row['status'] or 'NOT_REQUESTED',sent=row['sent'],
                     attempts=row['attempts'] or 0)
        return value

    def iter_signals(self, interval, start, end, page_size=500, changed_since_revision=None):
        for header in self.iter_signal_headers(interval,start,end,page_size,changed_since_revision):
            value = self.read_signal(header['id'])
            if value is not None:
                yield value

    def set_content_hash(self, signal_id, revision, content_hash):
        # Best effort compare-and-set: never bless a cache for a newer revision.
        self.db.execute('UPDATE signal_export_index SET content_hash=? WHERE id=? AND revision=?',
                        (content_hash,signal_id,revision))

    def mark_exported(self, interval, start, revision, manifest, stamp=None):
        if stamp is None:
            stamp = time.time_ns()//1000000
        row_count = manifest.get('row_count',manifest.get('signal_count',0))
        encoded = json.dumps(manifest,ensure_ascii=False,separators=(',',':'))
        self.db.execute('''UPDATE signal_export_periods SET exported_revision=max(exported_revision,?)
            WHERE interval=? AND start=?''',(revision,interval,start))
        self.db.execute('''INSERT INTO signals_export_metadata VALUES(?,?,?,?,?,?)
            ON CONFLICT(interval,start) DO UPDATE SET revision=excluded.revision,
                exported_ms=excluded.exported_ms,row_count=excluded.row_count,manifest=excluded.manifest
            WHERE excluded.revision>=signals_export_metadata.revision''',
            (interval,start,revision,stamp,row_count,encoded))

    def invalidate_period(self, interval, start):
        """A missing/corrupt output can be rebuilt without reading other archives."""
        self.db.execute('UPDATE export_revision_clock SET revision=revision+1 WHERE id=1')
        self.db.execute('''UPDATE signal_export_periods SET revision=(SELECT revision FROM export_revision_clock WHERE id=1)
            WHERE interval=? AND start=?''',(interval,start))

    def manifest(self, interval=None):
        sql = 'SELECT * FROM signals_export_metadata'
        args = ()
        if interval is not None:
            sql += ' WHERE interval=?'
            args = (interval,)
        for row in self.db.execute(sql+' ORDER BY interval,start',args):
            value = dict(row)
            value['manifest'] = json.loads(value['manifest'])
            yield value

    def recent_anomalies(self, stamp, interval=None, hours=24, limit=2000):
        if hours <= 0 or not 1 <= limit <= 2000:
            raise ValueError('Invalid anomaly display range')
        start = int(stamp-hours*3600000)
        args = [start,stamp]
        clause = ''
        if interval is not None:
            clause = " AND (interval=? OR interval='' OR interval IS NULL)"
            args.append(interval)
        groups = [dict(r) for r in self.db.execute(f'''SELECT symbol,interval,code,detail,
            min(time) AS first_time,max(time) AS last_time,count(*) AS occurrence_count,
            min(id) AS first_id,max(id) AS last_id,
            count(*) OVER() AS total_groups,sum(count(*)) OVER() AS total_occurrences,
            CASE WHEN json_valid(detail) THEN json_extract(detail,'$.signal_id') END AS signal_id
            FROM anomalies WHERE time>=? AND time<=? {clause}
            GROUP BY symbol,interval,code,detail ORDER BY last_time DESC,last_id DESC LIMIT ?''',args+[limit+1])]
        total_groups = groups[0]['total_groups'] if groups else 0
        total_occurrences = groups[0]['total_occurrences'] if groups else 0
        truncated = total_groups>limit
        groups = groups[:limit]
        # Existing status row code consumes these names; aggregate fields remain.
        for group in groups:
            group['time'] = group['last_time']
            group['id'] = group['last_id']
            group.pop('total_groups',None)
            group.pop('total_occurrences',None)
        return dict(start=start,end=stamp,hours=hours,limit=limit,truncated=truncated,
                    groups=groups,group_count=len(groups),total_groups=total_groups,raw_count=total_occurrences,
                    displayed_occurrences=sum(g['occurrence_count'] for g in groups))

    def iter_anomalies_after(self, cursor=0, until=None, page_size=500):
        if not 1 <= page_size <= 5000:
            raise ValueError('page_size must be between 1 and 5000')
        if until is None:
            until = self.db.execute('SELECT coalesce(max(id),0) FROM anomalies').fetchone()[0]
        while cursor < until:
            rows = self.db.execute('''SELECT *,CASE WHEN json_valid(detail) THEN json_extract(detail,'$.signal_id') END
                AS signal_id FROM anomalies WHERE id>? AND id<=? ORDER BY id LIMIT ?''',(cursor,until,page_size)).fetchall()
            if not rows:
                return
            for row in rows:
                yield dict(row)
            cursor = rows[-1]['id']

    def audit_digest(self, interval, start, end):
        """Stable ID reconciliation without loading K-line payloads."""
        digest = hashlib.sha256()
        count = 0
        for header in self.iter_signal_headers(interval,start,end):
            digest.update(json.dumps([header['id'],header['discovered']],ensure_ascii=False,
                                     separators=(',',':')).encode('utf-8')+b'\n')
            count += 1
        return dict(signal_count=count,ids_sha256=digest.hexdigest())
