"""Bounded, observable Excel child processes; independent of strategy rules."""
import ctypes
import hashlib
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

DEFAULTS = dict(page_rows=500, block_rows=5000, block_bytes=50*1024*1024,
    worker_rss_bytes=512*1024*1024, node_heap_mb=256, timeout_seconds=180,
    recent_anomaly_hours=24, recent_anomaly_groups=2000, recovery_poll_seconds=5,
    failure_limit=3, historical_periods_per_refresh=8, format_version=4)
EXPORT_LOCK = threading.Lock()


def settings(config):
    path = Path(config.get('export_settings_path', Path(__file__).parent.parent/'export-settings.json'))
    result = dict(DEFAULTS)
    if path.exists():
        result.update(json.loads(path.read_text(encoding='utf-8-sig')))
    for key in DEFAULTS:
        value = result[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError('Invalid export setting: '+key)
    if result['page_rows'] > 5000 or result['recent_anomaly_groups'] > 2000:
        raise ValueError('Export page/anomaly limit exceeded')
    return result


def worker_revision(options):
    digest = hashlib.sha256(json.dumps(options,sort_keys=True).encode())
    for filename in ('export.py','export_runtime.py','report_snapshot.py','row_cache.py','history_maintenance.py'):
        digest.update(Path(__file__).with_name(filename).read_bytes())
    for filename in ('excel_builder.mjs','streaming_xlsx.mjs'):
        digest.update((Path(__file__).parent.parent/filename).read_bytes())
    return digest.hexdigest()


def child_peak_rss(process):
    if os.name == 'nt':
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [('cb',wintypes.DWORD),('faults',wintypes.DWORD)]+[
                (name,ctypes.c_size_t) for name in ('peak','working','quota_peak_paged','quota_paged',
                    'quota_peak_nonpaged','quota_nonpaged','pagefile','peak_pagefile')]
        value = Counters(); value.cb = ctypes.sizeof(value)
        function = ctypes.windll.psapi.GetProcessMemoryInfo
        function.argtypes = [wintypes.HANDLE,ctypes.c_void_p,wintypes.DWORD]
        if function(wintypes.HANDLE(int(process._handle)),ctypes.byref(value),value.cb):
            return int(value.peak)
        return 0
    try:
        for line in Path(f'/proc/{process.pid}/status').read_text().splitlines():
            if line.startswith('VmHWM:'):
                return int(line.split()[1])*1024
    except OSError:
        pass
    return 0


def run_builder(command, *, cache, options):
    """Bound output buffers and enforce RSS/time independently of Node's heap."""
    started = time.monotonic(); peak = 0; failure = None
    stderr = Path(cache)/'builder.stderr.log'; stdout = Path(cache)/'builder.stdout.log'
    with stdout.open('wb') as out, stderr.open('wb') as err:
        process = subprocess.Popen(command,stdout=out,stderr=err)
        try:
            while process.poll() is None:
                peak = max(peak,child_peak_rss(process))
                if peak > options['worker_rss_bytes']:
                    failure = 'MEMORY_BUDGET_EXCEEDED'; process.kill(); break
                if time.monotonic()-started > options['timeout_seconds']:
                    failure = 'EXPORT_TIMEOUT'; process.kill(); break
                time.sleep(.05)
            process.wait(timeout=15)
            peak = max(peak,child_peak_rss(process))
        finally:
            if process.poll() is None:
                process.kill(); process.wait(timeout=15)
    with stderr.open('rb') as handle:
        handle.seek(max(0,stderr.stat().st_size-8000)); detail=handle.read().decode('utf-8',errors='replace')
    if not failure and process.returncode:
        failure = 'NODE_OOM' if any(s in detail.lower() for s in
            ('heap out of memory','allocation failed','reached heap limit','outofmemory')) else 'EXPORT_ERROR'
    metrics = dict(duration_ms=round((time.monotonic()-started)*1000),peak_rss_bytes=peak)
    metrics_path = Path(cache)/'metrics.json'
    if metrics_path.exists() and metrics_path.stat().st_mtime >= time.time()-(time.monotonic()-started)-2:
        try:
            node = json.loads(metrics_path.read_text(encoding='utf-8'))
            metrics['peak_rss_bytes'] = max(peak,node.get('peak_rss_bytes',0))
            metrics['node'] = node
        except (ValueError,OSError):
            pass
    return SimpleNamespace(returncode=process.returncode if not failure else process.returncode or 1,
        stderr=detail,reason=failure,metrics=metrics)


def export_health(store):
    return store.get_meta('export_health',dict(by_interval={}))


def check_circuit(store, interval, revision):
    state=export_health(store).get('by_interval',{}).get(interval,{})
    circuit=state.get('circuit',{})
    return circuit.get('blocked') and circuit.get('worker_revision')==revision


def record_failure(store, intervals, reason, detail, metrics, revision):
    from .core import now_ms, dumps
    stamp=now_ms()
    with store.lock:
        state=export_health(store)
        for interval in intervals:
            row=state.setdefault('by_interval',{}).setdefault(interval,{})
            circuit=row.get('circuit',{})
            count=circuit.get('failure_count',0)+1 if circuit.get('worker_revision')==revision else 1
            blocked=reason in ('NODE_OOM','MEMORY_BUDGET_EXCEEDED') or count>=settings(store.config)['failure_limit']
            row.update(last_failure=dict(time=stamp,reason=reason,detail=str(detail)[-2000:],**metrics),
                duration_ms=metrics.get('duration_ms'),peak_rss_bytes=metrics.get('peak_rss_bytes'),
                circuit=dict(blocked=blocked,reason=reason,failure_count=count,worker_revision=revision))
        store.meta('export_health',state)
        store.anomaly('','',reason,dumps(dict(intervals=intervals,detail=str(detail)[-1200:],**metrics)))
    # Operational alerts are local and separate from trading signal notifications.
    message=dumps(dict(type='EXPORT_OPERATIONAL_ALERT',time=stamp,reason=reason,intervals=intervals,**metrics))
    path=Path(store.config['data_dir'])/'export-alerts.jsonl'
    with path.open('a',encoding='utf-8') as handle:
        handle.write(message+'\n');handle.flush()
    print(message,flush=True)


def record_success(store, interval, book, metrics, revision, completed_at):
    with store.lock:
        state=export_health(store); row=state.setdefault('by_interval',{}).setdefault(interval,{})
        coverage=book['coverage']; complete=coverage['expected']>0 and not coverage['missing']
        row.update(last_success_ms=completed_at,report_boundary=book['refresh_boundary'],coverage=coverage,
            complete=complete,duration_ms=metrics.get('duration_ms'),peak_rss_bytes=metrics.get('peak_rss_bytes'),
            circuit=dict(blocked=False,failure_count=0,worker_revision=revision))
        if complete:
            row['last_complete_boundary']=max(row.get('last_complete_boundary') or 0,book['refresh_boundary'])
        store.meta('export_health',state)
        return dict(row)


def reset_circuit(store):
    with store.lock:
        state=export_health(store)
        for row in state.get('by_interval',{}).values():
            row['circuit']=dict(blocked=False,failure_count=0)
        store.meta('export_health',state)
        maintenance=store.get_meta('history_maintenance_health',{})
        for row in maintenance.values():
            row.update(blocked=False,failure_count=0)
        store.meta('history_maintenance_health',maintenance)


def maintain_history(store, config, revision, stamp):
    """A failed archive never relabels a successfully published workbook as failed."""
    from .history_maintenance import archive_anomalies, tier_archives
    from .core import now_ms, dumps
    state=store.get_meta('history_maintenance_health',{}) or {}
    for name, operation in (('anomalies',lambda:archive_anomalies(store,config)),
                            ('workbooks',lambda:tier_archives(store,config,stamp))):
        previous=state.get(name,{})
        if previous.get('blocked') and previous.get('worker_revision')==revision:
            continue
        started=time.monotonic()
        try:
            result=operation()
            state[name]=dict(time=now_ms(),result=result,worker_revision=revision,
                failure_count=0,blocked=False,duration_ms=round((time.monotonic()-started)*1000))
        except Exception as exc:
            count=previous.get('failure_count',0)+1 if previous.get('worker_revision')==revision else 1
            state[name]=dict(time=now_ms(),error=str(exc)[-2000:],worker_revision=revision,
                failure_count=count,blocked=count>=settings(config)['failure_limit'],
                duration_ms=round((time.monotonic()-started)*1000))
            message=dumps(dict(type='HISTORY_OPERATIONAL_ALERT',component=name,**state[name]))
            store.anomaly('','','HISTORY_ARCHIVE_ERROR',message)
            with (Path(config['data_dir'])/'export-alerts.jsonl').open('a',encoding='utf-8') as handle:
                handle.write(message+'\n');handle.flush()
            print(message,flush=True)
        store.meta('history_maintenance_health',state)
