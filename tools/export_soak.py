"""Read-only acceptance observer and bounded-memory XLSX/SQLite reconciliation.

This utility never starts market-data connections, sends notifications or writes
to the source database. The producer must be the real or shadow export runner;
healthy timestamps alone cannot satisfy the 72-hour export acceptance gate.
"""
import argparse
import collections
import ctypes
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import statistics
import sys
import time
import xml.etree.ElementTree as ET
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from monitor.maintenance import _write_json

NS = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
PERIOD_SECONDS = {'15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}


if os.name == 'nt':
    from ctypes import wintypes

    # POINTER caches its structure type for the process lifetime. Defining a
    # new type on every sample permanently retained thousands of classes.
    class _ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD),
                    ('PeakWorkingSetSize', ctypes.c_size_t), ('WorkingSetSize', ctypes.c_size_t),
                    ('QuotaPeakPagedPoolUsage', ctypes.c_size_t), ('QuotaPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t), ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                    ('PagefileUsage', ctypes.c_size_t), ('PeakPagefileUsage', ctypes.c_size_t)]

    _rss_kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    _rss_psapi = ctypes.WinDLL('psapi', use_last_error=True)
    _rss_open_process = _rss_kernel.OpenProcess
    _rss_open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _rss_open_process.restype = wintypes.HANDLE
    _rss_close_handle = _rss_kernel.CloseHandle
    _rss_close_handle.argtypes = [wintypes.HANDLE]
    _rss_close_handle.restype = wintypes.BOOL
    _rss_get_memory = _rss_psapi.GetProcessMemoryInfo
    _rss_get_memory.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ProcessMemoryCounters), wintypes.DWORD]
    _rss_get_memory.restype = wintypes.BOOL


def rss_bytes(pid):
    """Current producer RSS; export worker peaks come from its export metrics."""
    if not pid:
        return None
    try:
        if os.name != 'nt':
            return int(Path(f'/proc/{int(pid)}/statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE')
        handle = _rss_open_process(0x1000 | 0x10, False, int(pid))
        if not handle:
            return None
        try:
            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(counters)
            if _rss_get_memory(handle, ctypes.byref(counters), counters.cb):
                return counters.WorkingSetSize
        finally:
            _rss_close_handle(handle)
    except (OSError, ValueError, IndexError):
        pass
    return None


def observe_sample(state, health, stamp, sample_seconds=60):
    """Pure state transition, including restart/gap and actual-export checks."""
    state.setdefault('started_ms', stamp)
    state.setdefault('continuous_start_ms', stamp)
    state.setdefault('samples', 0)
    state.setdefault('interruptions', [])
    state.setdefault('exports', {i: [] for i in PERIOD_SECONDS})
    state.setdefault('seen_success', {})
    state.setdefault('failures', [])
    state.setdefault('producer_samples', [])
    previous = state.get('last_sample_ms')
    stale = not health.get('time') or abs(stamp - health['time']) > max(120_000, sample_seconds * 2500)
    changed_pid = state.get('producer_pid') and health.get('pid') != state['producer_pid']
    changed_instance = (state.get('producer_instance') and health.get('instance_id') and
                        health['instance_id'] != state['producer_instance'])
    changed_code = state.get('code_hash') and health.get('code_hash') != state['code_hash']
    interrupted = (previous is not None and stamp - previous > sample_seconds * 2500) or stale or changed_pid or changed_instance or changed_code or not health.get('running')
    if interrupted:
        state['interruptions'].append({'time': stamp, 'stale': stale, 'changed_pid': bool(changed_pid),
                                       'changed_code':bool(changed_code),
                                       'running': health.get('running'), 'previous_sample_ms': previous})
        state['interruptions'] = state['interruptions'][-100:]
        state['continuous_start_ms'] = stamp
        state['exports'] = {i: [] for i in PERIOD_SECONDS}
        state['seen_success'] = {}
        state['failures'] = []
        state['producer_samples'] = []
    state['producer_pid'] = health.get('pid')
    state['producer_instance'] = health.get('instance_id')
    state['code_hash'] = health.get('code_hash')
    state['last_sample_ms'] = stamp
    state['samples'] += 1
    state['producer_rss_bytes'] = rss_bytes(health.get('pid'))
    state['producer_samples'].append({'time': stamp, 'rss_bytes': state['producer_rss_bytes']})
    state['producer_samples'] = [x for x in state['producer_samples'] if x['time'] >= stamp - 96 * 3600_000]
    export_health = health.get('export_health', {}).get('by_interval', {})
    last_export = health.get('last_export', {}).get('by_interval', {}) if health.get('last_export') else {}
    for interval in PERIOD_SECONDS:
        metric = export_health.get(interval, {})
        published = last_export.get(interval, {})
        success = metric.get('last_success_ms')
        failure = metric.get('last_failure') or {}
        if failure.get('time', 0) >= state['continuous_start_ms']:
            key = [interval, failure.get('time'), failure.get('reason')]
            if key not in state['failures']:
                state['failures'].append(key)
        candidates = [event for event in health.get('export_events', []) if event.get('interval')==interval]
        if success:
            candidates.append(dict(interval=interval,time=success,boundary=metric.get('report_boundary'),
                duration_ms=metric.get('duration_ms'),peak_rss_bytes=metric.get('peak_rss_bytes'),
                coverage=metric.get('coverage'),reason=published.get('reason'),complete=published.get('complete')))
        for event in sorted(candidates,key=lambda x:x.get('time',0)):
            success = event.get('time')
            if not success or success < state['continuous_start_ms'] or success <= state['seen_success'].get(interval,0):
                continue
            boundary = event.get('boundary')
            item = {'time': success, 'boundary': boundary, 'duration_ms': event.get('duration_ms'),
                    'peak_rss_bytes': event.get('peak_rss_bytes'), 'coverage': event.get('coverage'),
                    'reason': event.get('reason'), 'complete': event.get('complete'),
                    'producer_rss_bytes': state['producer_rss_bytes']}
            if boundary and event.get('reason') in ('CANDLE_CLOSE', 'RECOVERY_COMPLETE'):
                item['close_latency_ms'] = success - boundary
            state['exports'][interval].append(item)
            # Keep a 96-hour rolling window; memory stays bounded on indefinite runs.
            cutoff = stamp - 96 * 3600_000
            state['exports'][interval] = [x for x in state['exports'][interval] if x['time'] >= cutoff]
            state['seen_success'][interval] = success
    return state


def summarize_soak(state, budget_bytes=512 * 1024 * 1024, target_ms=180_000):
    elapsed = max(0, state.get('last_sample_ms', 0) - state.get('continuous_start_ms', 0))
    enough_time = elapsed >= 72 * 3600_000
    result = {'status': 'OBSERVING', 'continuous_hours': elapsed / 3600_000,
              'required_hours': 72, 'memory_budget_bytes': budget_bytes,
              'close_target_ms': target_ms, 'samples': state.get('samples', 0),
              'code_hash':state.get('code_hash'),
              'producer_rss_bytes': state.get('producer_rss_bytes'), 'by_interval': {},
              'interruptions': state.get('interruptions', []), 'failures': state.get('failures', [])}
    samples = [x['rss_bytes'] for x in state.get('producer_samples', [])
               if isinstance(x.get('rss_bytes'), (int, float))]
    producer_trend = {}
    if len(samples) >= 12:
        width = max(3, len(samples) // 6)
        first, last = statistics.median(samples[:width]), statistics.median(samples[-width:])
        producer_trend = dict(first_median=first, last_median=last, growth_ratio=(last-first)/max(1, first))
    result['producer_memory'] = dict(peak_rss_bytes=max(samples, default=None), trend=producer_trend,
        metrics_complete=bool(samples) and len(samples) == len(state.get('producer_samples', [])))
    failures = bool(state.get('failures')) or (bool(samples) and max(samples) > budget_bytes)
    failures |= producer_trend.get('growth_ratio', 0) > .30
    enough_exports = True
    for interval, seconds in PERIOD_SECONDS.items():
        rows = state.get('exports', {}).get(interval, [])
        peaks = [x['peak_rss_bytes'] for x in rows if isinstance(x.get('peak_rss_bytes'), (int, float))]
        durations = [x['duration_ms'] for x in rows if isinstance(x.get('duration_ms'), (int, float))]
        # A permitted provisional workbook must not fail the gate after the
        # same close is repaired. Count distinct closes, never retry attempts.
        batches = {}
        for row in rows:
            if row.get('boundary') and row.get('reason') in ('CANDLE_CLOSE', 'RECOVERY_COMPLETE'):
                previous = batches.get(row['boundary'])
                coverage = (previous or {}).get('coverage') or {}
                previously_complete = coverage.get('expected',0)>0 and coverage.get('expected')==coverage.get('completed')
                # Deadline means the first complete publication. A later
                # harmless re-publication must not turn an on-time close late.
                if not previously_complete:
                    batches[row['boundary']] = row
        settled = [row for row in batches.values() if
                   row['boundary'] <= state.get('last_sample_ms', 0) - target_ms]
        latency = [x['close_latency_ms'] for x in settled if isinstance(x.get('close_latency_ms'), (int, float))]
        required = max(0, (state.get('last_sample_ms', 0)-target_ms)//(seconds*1000) -
                       state.get('continuous_start_ms', 0)//(seconds*1000))
        observed_required = sum(state.get('continuous_start_ms', 0) < boundary <=
            state.get('last_sample_ms', 0)-target_ms for boundary in batches)
        measurable = len(peaks) == len(rows) == len(durations) and bool(rows)
        enough_exports &= observed_required >= required and measurable
        over_budget = bool(peaks and max(peaks) > budget_bytes)
        late = sum(x > target_ms for x in latency)
        incomplete = sum((x.get('coverage') or {}).get('completed', -1) != (x.get('coverage') or {}).get('expected', -2) for x in settled)
        trend = {}
        if len(rows) >= 12 and durations and peaks:
            width = max(3, len(rows) // 6)
            for field in ('duration_ms', 'peak_rss_bytes'):
                first = [x[field] for x in rows[:width] if isinstance(x.get(field), (int, float))]
                last = [x[field] for x in rows[-width:] if isinstance(x.get(field), (int, float))]
                if first and last:
                    baseline = statistics.median(first)
                    change = statistics.median(last) - baseline
                    trend[field] = {'first_median': baseline, 'last_median': statistics.median(last),
                                    'growth_ratio': change / max(1, baseline)}
        growing = any(x['growth_ratio'] > .30 for x in trend.values())
        failures |= over_budget or late > 0 or incomplete > 0 or growing
        result['by_interval'][interval] = {'exports_observed': len(rows), 'distinct_closes': len(batches),
            'required_closes': required,
            'metrics_complete': measurable, 'peak_rss_bytes': max(peaks, default=None),
            'max_duration_ms': max(durations, default=None), 'late_exports': late,
            'incomplete_exports': incomplete, 'trend': trend, 'growth_review_required': growing}
    if enough_time:
        result['status'] = 'FAILED' if failures else ('PASSED' if enough_exports and result['producer_memory']['metrics_complete'] and state.get('code_hash') else 'INSUFFICIENT_EXPORT_EVIDENCE')
    elif failures:
        result['status'] = 'OBSERVING_WITH_FAILURES'
    return result


def observe(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    state_path = output / 'soak-state.json'
    state = json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else {}
    source = str(Path(args.health).resolve())
    if state.get('health_source', source) != source:
        raise ValueError('Cannot resume an observation with a different health source')
    state['health_source'] = source
    deadline = time.monotonic() + args.hours * 3600
    while True:
        stamp = time.time_ns() // 1_000_000
        try:
            health = json.loads(Path(source).read_text(encoding='utf-8-sig'))
        except (OSError, ValueError) as exc:
            health = {'running': False, 'read_error': type(exc).__name__}
        observe_sample(state, health, stamp, args.sample_seconds)
        summary = summarize_soak(state, args.memory_mib * 1024 * 1024)
        with (output / 'soak-samples.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps({'time': stamp, 'health_time': health.get('time'),
                'producer_rss_bytes': state.get('producer_rss_bytes'), 'export_health': health.get('export_health'),
                'read_error': health.get('read_error')}, ensure_ascii=False) + '\n')
        _write_json(state_path, state)
        _write_json(output / 'soak-report.json', summary)
        if args.once or time.monotonic() >= deadline:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return
        time.sleep(min(args.sample_seconds, max(0, deadline - time.monotonic())))


def _xml_elements(source, tag):
    """Release completed rows/strings from their parent, not just cell text."""
    stack = []
    for event, element in ET.iterparse(source, events=('start', 'end')):
        if event == 'start':
            stack.append(element)
            continue
        if element.tag == tag:
            yield element
            if len(stack) > 1:
                stack[-2].remove(element)
            element.clear()
        stack.pop()


def xlsx_trigger_ids(path, scratch):
    """Stream the trigger worksheet; shared strings are indexed on disk, not RAM."""
    scratch.execute('CREATE TEMP TABLE IF NOT EXISTS shared_strings(idx INTEGER PRIMARY KEY,value TEXT)')
    scratch.execute('DELETE FROM shared_strings')
    with zipfile.ZipFile(path) as package:
        if 'xl/sharedStrings.xml' in package.namelist():
            with package.open('xl/sharedStrings.xml') as source:
                index = 0
                for element in _xml_elements(source, NS + 'si'):
                    scratch.execute('INSERT INTO shared_strings VALUES(?,?)', (index, ''.join(element.itertext())))
                    index += 1
        workbook = ET.fromstring(package.read('xl/workbook.xml'))
        relation = ET.fromstring(package.read('xl/_rels/workbook.xml.rels'))
        targets = {x.attrib['Id']: x.attrib['Target'] for x in relation}
        sheet = next((s for s in workbook.find(NS + 'sheets') if s.attrib.get('name') == '触发记录'), None)
        if sheet is None:
            raise ValueError('Missing 触发记录 sheet: ' + str(path))
        target = targets[sheet.attrib['{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']]
        target = target.lstrip('/') if target.startswith('/') else 'xl/' + target
        column = None
        with package.open(target) as source:
            for element in _xml_elements(source, NS + 'row'):
                cells = {}
                for cell in element:
                    if cell.tag != NS + 'c':
                        continue
                    ref = ''.join(x for x in cell.attrib.get('r', '') if x.isalpha())
                    value = cell.findtext(NS + 'v')
                    if cell.attrib.get('t') == 's' and value is not None:
                        row = scratch.execute('SELECT value FROM shared_strings WHERE idx=?', (int(value),)).fetchone()
                        value = row[0] if row else None
                    elif cell.attrib.get('t') == 'inlineStr':
                        value = ''.join(cell.find(NS + 'is').itertext())
                    cells[ref] = value
                if column is None:
                    column = next((key for key, value in cells.items() if value in ('signal_id', '信号编号')), None)
                elif cells.get(column):
                    yield cells[column]
        if column is None:
            raise ValueError('Missing signal_id header: ' + str(path))


def reconcile(source_db, files, output):
    """Use explicitly selected canonical workbooks; snapshot copies are not inputs."""
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    ledger_path = output / 'signal-reconciliation.sqlite3'
    if ledger_path.exists():
        raise FileExistsError('Use a fresh reconciliation output directory')
    ledger = sqlite3.connect(ledger_path)
    source = sqlite3.connect(Path(source_db).resolve().as_uri() + '?mode=ro', uri=True)
    try:
        source.execute('BEGIN')
        ledger.executescript('CREATE TABLE expected(id TEXT PRIMARY KEY,interval TEXT);'
            'CREATE TABLE actual(id TEXT,file TEXT); CREATE INDEX actual_id ON actual(id);')
        digest = hashlib.sha256()
        for row in source.execute('SELECT id,interval FROM signals ORDER BY id'):
            ledger.execute('INSERT INTO expected VALUES(?,?)', row)
            digest.update(json.dumps(list(row), ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n')
        for file in files:
            for signal_id in xlsx_trigger_ids(file, ledger):
                ledger.execute('INSERT INTO actual VALUES(?,?)', (signal_id, str(Path(file).resolve())))
            ledger.commit()
        count = lambda sql: ledger.execute(sql).fetchone()[0]
        result = {'expected': count('SELECT count(*) FROM expected'),
            'actual_rows': count('SELECT count(*) FROM actual'),
            'actual_unique': count('SELECT count(DISTINCT id) FROM actual'),
            'missing': count('SELECT count(*) FROM expected e WHERE NOT EXISTS(SELECT 1 FROM actual a WHERE a.id=e.id)'),
            'unexpected': count('SELECT count(*) FROM actual a WHERE NOT EXISTS(SELECT 1 FROM expected e WHERE e.id=a.id)'),
            'duplicate_ids': count('SELECT count(*) FROM (SELECT id FROM actual GROUP BY id HAVING count(*)>1)'),
            'signal_ids_sha256': digest.hexdigest(), 'files': [str(Path(p).resolve()) for p in files]}
        result['passed'] = not any(result[k] for k in ('missing', 'unexpected', 'duplicate_ids'))
        _write_json(output / 'signal-reconciliation.json', result)
        return result
    finally:
        source.close()
        ledger.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    observer = commands.add_parser('observe')
    observer.add_argument('--health', required=True)
    observer.add_argument('--output', required=True)
    observer.add_argument('--hours', type=float, default=72)
    observer.add_argument('--sample-seconds', type=float, default=60)
    observer.add_argument('--memory-mib', type=int, default=512)
    observer.add_argument('--once', action='store_true')
    check = commands.add_parser('reconcile')
    check.add_argument('--source-db', required=True)
    check.add_argument('--files-json', required=True, help='JSON list of canonical XLSX paths')
    check.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.command == 'observe':
        if args.sample_seconds <= 0 or args.hours < 0 or args.memory_mib <= 0:
            parser.error('Invalid duration, sample interval or memory budget')
        observe(args)
    else:
        print(json.dumps(reconcile(args.source_db, json.loads(Path(args.files_json).read_text(encoding='utf-8-sig')),
                                   args.output), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
