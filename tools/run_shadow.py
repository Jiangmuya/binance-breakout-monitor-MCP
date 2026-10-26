"""Real-clock, read-only-source export acceptance runner.

This is validation infrastructure, never the production ingestion path. It
mirrors small report tables in pages, preserving persisted strategy payloads,
and deliberately does not copy bars or recalculate indicators. The old source
schema has no update clock, so signal/outbox content comparisons are a bounded
validation-only compatibility bridge. Production exports use revision indexes.
No Binance requests, trading, signal delivery or source-database writes occur.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import traceback
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from monitor.core import Store, PERIODS, now_ms
from monitor.export import export_all, close_coverage
from monitor.maintenance import OfflineLock, _write_json
from export_soak import observe_sample, summarize_soak

TABLES = {
    'signals': ('id', 'version', 'symbol', 'interval', 't', 'event_type', 'payload'),
    'outbox': ('id', 'payload', 'status', 'attempts', 'next_attempt', 'sent', 'error'),
    'states': ('symbol', 'interval', 'state'),
    'universe': ('symbol', 'active', 'info', 'reason', 'updated'),
    'versions': ('version', 'config', 'created'),
}
KEYS = {'signals': ('id',), 'outbox': ('id',), 'states': ('symbol', 'interval'),
        'universe': ('symbol',), 'versions': ('version',)}


def code_hash(root=None):
    root = Path(root or Path(__file__).resolve().parents[1])
    paths = list((root/'monitor').glob('*.py'))+list(root.glob('*.mjs'))
    paths += [root/'tools/export_soak.py',root/'tools/run_shadow.py']
    paths += [root/name for name in ('export-settings.json','config.json','package-lock.json') if (root/name).is_file()]
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(str(path.relative_to(root)).replace('\\','/').encode()+b'\0')
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _upsert(table, columns, keys):
    changed = [name for name in columns if name not in keys]
    return ('INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES(' +
        ','.join('?' for _ in columns) + ') ON CONFLICT(' + ','.join(keys) +
        ') DO UPDATE SET ' + ','.join(name + '=excluded.' + name for name in changed) +
        ' WHERE ' + ' OR '.join(table + '.' + name + ' IS NOT excluded.' + name for name in changed))


def mirror_snapshot(source_path, store, boundaries, page_size=500):
    """One pinned source snapshot and one atomic shadow update, bounded by pages."""
    source_path = Path(source_path).resolve(strict=True)
    with store.lock:
        target = Path(store.db.execute('PRAGMA database_list').fetchone()[2]).resolve()
    if source_path == target:
        raise ValueError('Shadow source and destination must be different databases')
    source = sqlite3.connect(source_path.as_uri() + '?mode=ro', uri=True, timeout=30)
    source.row_factory = sqlite3.Row
    source.execute('PRAGMA query_only=ON')
    source.execute('PRAGMA cache_size=-4096')
    source.execute('BEGIN')
    source.execute('SELECT count(*) FROM sqlite_master').fetchone()
    stamp = now_ms()
    counts = {}
    try:
        with store.lock, store.db:
            store.db.execute('''CREATE TABLE IF NOT EXISTS shadow_anomaly_source(
                source_id INTEGER PRIMARY KEY,shadow_id INTEGER NOT NULL UNIQUE)''')
            store.db.execute('CREATE INDEX IF NOT EXISTS shadow_evaluations_close '
                             'ON evaluations(interval,t,symbol,version)')
            for table, columns in TABLES.items():
                cursor = source.execute('SELECT ' + ','.join(columns) + ' FROM ' + table + ' ORDER BY rowid')
                sql = _upsert(table, columns, KEYS[table])
                counts[table] = 0
                while True:
                    page = cursor.fetchmany(page_size)
                    if not page:
                        break
                    store.db.executemany(sql, [tuple(row) for row in page])
                    counts[table] += len(page)
            # Source data is retained even if a row later disappears. Active
            # universe membership is the source's committed active flag.
            active = {row[0] for row in source.execute('SELECT symbol FROM universe WHERE active=1')}
            for row in store.db.execute('SELECT symbol FROM universe WHERE active=1').fetchall():
                if row[0] not in active:
                    store.db.execute('UPDATE universe SET active=0 WHERE symbol=?', (row[0],))
            marker = store.db.execute('SELECT COALESCE(max(source_id),0) FROM shadow_anomaly_source').fetchone()[0]
            counts['anomalies_new'] = 0
            while True:
                page = source.execute('SELECT id,time,symbol,interval,code,detail FROM anomalies '
                    'WHERE id>? ORDER BY id LIMIT ?', (marker, page_size)).fetchall()
                if not page:
                    break
                for row in page:
                    inserted = store.db.execute('INSERT INTO anomalies(time,symbol,interval,code,detail) '
                        'VALUES(?,?,?,?,?)', tuple(row)[1:])
                    store.db.execute('INSERT INTO shadow_anomaly_source VALUES(?,?)', (row['id'], inserted.lastrowid))
                marker = page[-1]['id']
                counts['anomalies_new'] += len(page)
            versions = [row[0] for row in source.execute('SELECT version FROM versions')]
            evaluation_sql = _upsert('evaluations', ('version','symbol','interval','t','payload'),
                                     ('version','symbol','interval','t'))
            # Read only the exact close(s) required by the current/pending batch.
            # The source primary key is used by every lookup.
            for interval, targets in boundaries.items():
                targets = set(targets)
                for boundary in targets:
                    existing = {(row[0],row[1]) for row in store.db.execute(
                        'SELECT symbol,version FROM evaluations WHERE interval=? AND t=?',
                        (interval,boundary-PERIODS[interval]))}
                    for symbol in active:
                        for version in versions:
                            if (symbol,version) in existing:
                                continue
                            row = source.execute('SELECT version,symbol,interval,t,payload FROM evaluations '
                                'WHERE version=? AND symbol=? AND interval=? AND t=?',
                                (version,symbol,interval,boundary-PERIODS[interval])).fetchone()
                            if row:
                                store.db.execute(evaluation_sql, tuple(row))
                # Only the disposable validation buffer is bounded. Neither
                # source history nor shadow signal/anomaly history is deleted.
                if targets:
                    store.db.execute('DELETE FROM evaluations WHERE interval=? AND t<?',
                        (interval,min(targets)-PERIODS[interval]))
            for key in ('clock', 'universe_stale', 'universe_updated'):
                row = source.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
                if row:
                    store.db.execute('INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) '
                        'DO UPDATE SET value=excluded.value', (key,row[0]))
            report = dict(snapshot_ms=stamp, completed_ms=now_ms(), counts=counts,
                source=str(source_path), mode='READ_ONLY_SOURCE_VALIDATION_MIRROR', page_size=page_size)
            store.db.execute('INSERT INTO metadata VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                ('shadow_source_snapshot',json.dumps(report)))
        return report
    finally:
        source.rollback()
        source.close()


class ShadowRunner:
    def __init__(self, source, store, config):
        self.source = source
        self.store = store
        self.config = config
        self.started = False
        saved = store.get_meta('shadow_runner_state',{})
        self.seen_boundary = saved.get('seen_boundary',{})
        self.pending = {interval:set(boundaries) for interval,boundaries in saved.get('pending',{}).items()}
        self.events = saved.get('events',[])
        self._saved_state = None
        self.last_job = None

    def save(self):
        self.events = [row for row in self.events if row['time']>=now_ms()-96*3600_000]
        value = dict(seen_boundary=self.seen_boundary,
            pending={i:sorted(values) for i,values in self.pending.items() if values},events=self.events)
        encoded = json.dumps(value,sort_keys=True,separators=(',',':'))
        if encoded!=self._saved_state:
            self.store.meta('shadow_runner_state',value)
            self._saved_state = encoded

    def published(self,interval,boundary):
        entry = (self.store.get_meta('last_export',{}) or {}).get('by_interval',{}).get(interval,{})
        if entry.get('boundary')!=boundary:
            return None
        coverage = entry.get('coverage') or {}
        complete = coverage.get('expected',0)>0 and coverage.get('expected')==coverage.get('completed') and not coverage.get('missing')
        pending = self.pending.setdefault(interval,set())
        if complete:
            pending.discard(boundary)
        else:
            pending.add(boundary)
        metric = self.store.get_meta('export_health',{}).get('by_interval',{}).get(interval,{})
        event = dict(interval=interval,time=entry.get('time'),boundary=boundary,reason=entry.get('reason'),
            coverage=dict(expected=coverage.get('expected'),completed=coverage.get('completed'),
                          missing_count=len(coverage.get('missing') or [])),complete=complete,
            duration_ms=metric.get('duration_ms'),peak_rss_bytes=metric.get('peak_rss_bytes'))
        if event['time'] and not any(x['interval']==interval and x['time']==event['time'] for x in self.events):
            self.events.append(event)
        self.save()
        return entry

    def publish(self,interval,boundary,reason):
        # backfill=True drains every changed historical period in the same
        # bounded streaming publication, preserving the close's health reason.
        export_all(self.store,self.config,intervals=[interval],refresh_reason=reason,
                   refresh_boundary=boundary,backfill=True)
        return self.published(interval,boundary)

    def dirty_intervals(self):
        with self.store.lock:
            return {row[0] for row in self.store.db.execute('SELECT DISTINCT interval FROM signal_export_periods '
                'WHERE revision>exported_revision')}

    def tick(self):
        stamp = now_ms()
        closes = {interval: stamp//step*step for interval,step in PERIODS.items()}
        boundaries = {interval: {close} | self.pending.get(interval,set())
                      for interval,close in closes.items()}
        mirrored = mirror_snapshot(self.source,self.store,boundaries)
        if not self.started:
            export_all(self.store,self.config,refresh_reason='STARTUP',backfill=True)
            self.seen_boundary = dict(closes)
            for interval,boundary in closes.items():
                if self.published(interval,boundary) is None:
                    self.pending.setdefault(interval,set()).add(boundary)
            self.started = True
        else:
            # Due closes are serialized; no simultaneous Node workers or
            # competing publications can distort the production load evidence.
            for interval,boundary in closes.items():
                if boundary > self.seen_boundary.get(interval,0):
                    if self.publish(interval,boundary,'CANDLE_CLOSE') is not None:
                        self.seen_boundary[interval] = boundary
                    else:
                        self.pending.setdefault(interval,set()).add(boundary)
                repaired_old = False
                for waiting in sorted(self.pending.get(interval,set())):
                    coverage = close_coverage(self.store,interval,waiting)
                    if coverage['expected'] and not coverage['missing']:
                        self.publish(interval,waiting,'RECOVERY_COMPLETE')
                        repaired_old |= waiting<boundary
                if repaired_old:
                    # Keep the visible current workbook on the latest close;
                    # per-close publication evidence above remains durable.
                    self.publish(interval,boundary,'RECOVERY_COMPLETE')
            # Signals/outbox can alter old periods between closes. Every dirty
            # period is drained now; a blocked circuit is never bypassed.
            for interval in sorted(self.dirty_intervals()):
                self.publish(interval,closes[interval],'BACKFILL')
        self.save()
        with self.store.lock:
            files = sorted({path for row in self.store.db.execute('SELECT manifest FROM signals_export_metadata')
                            for path in json.loads(row[0]).get('files',[])})
        _write_json(Path(self.config['data_dir']).parent/'canonical-files.json',files)
        self.last_job = dict(time=now_ms(),source_snapshot=mirrored,
            pending={i:sorted(values) for i,values in self.pending.items() if values},
            dirty_intervals=sorted(self.dirty_intervals()),canonical_files=len(files))
        return self.last_job


def run(args):
    source = Path(args.source_db).resolve(strict=True)
    output = Path(args.output).resolve()
    config_path = Path(args.source_config).resolve(strict=True)
    if output == source.parent or source.parent in output.parents:
        raise ValueError('Validation output must be outside the production data directory')
    output.mkdir(parents=True,exist_ok=True)
    runtime = output/'runtime'
    runtime.mkdir(exist_ok=True)
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    config.update(data_dir=str(runtime),excel_dir=str(output/'excel'),webhook_authorized=False)
    if args.node:
        config['node_executable'] = str(Path(args.node).resolve(strict=True))
    (output/'excel').mkdir(exist_ok=True)
    acceptance = output/'acceptance'
    acceptance.mkdir(exist_ok=True)
    state_path = acceptance/'soak-state.json'
    observation = json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else {}
    instance = uuid.uuid4().hex
    pinned_code = code_hash()
    descriptor = dict(pid=os.getpid(),instance_id=instance,started_ms=now_ms(),source=str(source),
        output=str(output),code=str(Path(__file__).resolve().parents[1]),mode='SHADOW_EXPORT_ONLY',
        requested_hours=args.hours,source_database_access='read-only',signal_notifications=False,code_hash=pinned_code)
    deadline = time.monotonic()+args.hours*3600 if args.hours else float('inf')
    with OfflineLock(runtime):
        _write_json(output/'shadow-run.json',descriptor)
        store = Store(runtime/'monitor.sqlite3',config)
        runner = ShadowRunner(source,store,config)
        pool = ThreadPoolExecutor(max_workers=1,thread_name_prefix='shadow-export')
        future = None
        last_error = None
        code_changed = False
        health = {}
        try:
            while True:
                stamp = now_ms()
                stop_requested = (output/'STOP').exists()
                if not code_changed and code_hash()!=pinned_code:
                    code_changed = True
                    last_error = dict(time=stamp,type='CODE_CHANGED_RESTART_REQUIRED',
                        detail='Candidate code changed during acceptance; restart to begin a new continuous segment')
                if future and future.done():
                    try:
                        future.result()
                        if not code_changed:
                            last_error = None
                    except Exception as exc:
                        last_error = dict(time=stamp,type=type(exc).__name__,detail=str(exc)[-4000:])
                        traceback.print_exc()
                    future = None
                    if args.once:
                        break
                if future is None and time.monotonic()<deadline and not code_changed and not stop_requested:
                    future = pool.submit(runner.tick)
                health = dict(time=stamp,pid=os.getpid(),instance_id=instance,running=not code_changed,code_hash=pinned_code,
                    mode='SHADOW_EXPORT_ONLY',worker_busy=future is not None,stopping=stop_requested,
                    last_export=store.get_meta('last_export'),export_health=store.get_meta('export_health',{}),
                    source_snapshot=store.get_meta('shadow_source_snapshot'),last_job=runner.last_job,
                    shadow_error=last_error,export_events=list(runner.events[-64:]))
                _write_json(runtime/'health.json',health)
                observe_sample(observation,health,stamp,args.poll_seconds)
                _write_json(state_path,observation)
                _write_json(acceptance/'soak-report.json',summarize_soak(observation,args.memory_mib*1024*1024))
                with (acceptance/'soak-samples.jsonl').open('a',encoding='utf-8') as stream:
                    stream.write(json.dumps(dict(time=stamp,producer_rss_bytes=observation.get('producer_rss_bytes'),
                        export_health=health['export_health'],shadow_error=last_error))+'\n')
                if (time.monotonic()>=deadline or code_changed or stop_requested) and future is None:
                    break
                time.sleep(args.poll_seconds)
        except Exception as exc:
            last_error = dict(time=now_ms(),type=type(exc).__name__,detail=str(exc)[-4000:],
                              winerror=getattr(exc,'winerror',None))
            # Independent append-only evidence survives a blocked health-file
            # replacement. Persistent IO failure must not produce a false pass.
            print(json.dumps(dict(event='SHADOW_FATAL_ERROR',**last_error)),file=sys.stderr,flush=True)
            try:
                with (acceptance/'shadow-errors.jsonl').open('a',encoding='utf-8') as stream:
                    stream.write(json.dumps(last_error)+'\n')
            except OSError as log_error:
                print('Could not persist shadow error: '+str(log_error),file=sys.stderr,flush=True)
            raise
        finally:
            try:
                pool.shutdown(wait=True,cancel_futures=True)
                health.update(time=now_ms(),running=False,worker_busy=False,last_export=store.get_meta('last_export'),
                    export_health=store.get_meta('export_health',{}),shadow_error=last_error)
                try:
                    _write_json(runtime/'health.json',health)
                except OSError as exc:
                    print('Final shadow status publication failed: '+str(exc),file=sys.stderr,flush=True)
                    if last_error is None:
                        raise
            finally:
                store.close()
    if last_error:
        raise RuntimeError('Shadow runner failed: '+last_error['detail'])
    print(json.dumps(dict(output=str(output),last_job=runner.last_job),ensure_ascii=False,indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-db',required=True)
    parser.add_argument('--source-config',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--node')
    parser.add_argument('--hours',type=float,default=0,help='0 keeps the producer running until explicitly stopped')
    parser.add_argument('--poll-seconds',type=float,default=5)
    parser.add_argument('--memory-mib',type=int,default=512)
    parser.add_argument('--once',action='store_true',help='One startup export; never counts as 72-hour acceptance')
    args = parser.parse_args()
    if args.hours<0 or args.poll_seconds<=0 or args.memory_mib<=0:
        parser.error('Invalid duration, polling interval or memory budget')
    run(args)


if __name__ == '__main__':
    main()
