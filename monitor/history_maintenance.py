"""Bounded, recoverable archives. Source database rows are never removed.

Anomaly files are committed in immutable batches before their high-water mark.
Workbook tiers move bytes, preserving the existing calendar reporting periods.
"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from pathlib import Path

from .archive import ARCHIVE_DIR, DAY, ZONE, report_period, resolved

_LOCK = threading.RLock()
ANOMALY_KEY = 'anomaly_archive_v1'
TIER_KEY = 'history_tier_moves_v1'
PAGE_SIZE = 500


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _io(path):
    """Use extended Windows paths for IO, canonical plain paths in metadata."""
    path = Path(path)
    if os.name != 'nt':
        return path
    value = str(path.absolute())
    if value.startswith('\\\\?\\'):
        return path
    return Path('\\\\?\\UNC\\' + value[2:] if value.startswith('\\\\') else '\\\\?\\' + value)


def _hash(path):
    value = hashlib.sha256()
    with _io(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def _inside(path, root):
    path, root = resolved(path), resolved(root)
    if path == root or root not in path.parents:
        raise ValueError('History path escaped its intended directory')
    return path


def _history(config):
    root = resolved(config['excel_dir'])
    _io(root).mkdir(parents=True, exist_ok=True)
    history = resolved(root / ARCHIVE_DIR)
    if history.parent != root:
        raise ValueError('History directory escaped the Excel directory')
    _io(history).mkdir(exist_ok=True)
    return history


def _sync_directory(directory):
    # POSIX needs the directory entry flushed as well. Windows os.replace is
    # atomic; Python has no portable directory fsync handle there.
    if os.name != 'nt':
        descriptor = os.open(directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _publish_bytes(path, content):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.pending')
    try:
        with _io(temporary).open('wb') as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(_io(temporary), _io(path))
        _sync_directory(path.parent)
    finally:
        if _io(temporary).exists():
            _io(temporary).unlink()


def _write_anomaly_part(path, rows):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.pending')
    try:
        with _io(temporary).open('wb') as raw:
            # Replaying a crashed batch generates identical bytes.
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0) as compressed:
                for row in rows:
                    compressed.write((_json(row) + '\n').encode('utf-8'))
            raw.flush()
            os.fsync(raw.fileno())
        digest = _hash(temporary)
        if _io(path).exists():
            if _hash(path) != digest:
                raise RuntimeError('Anomaly archive collision or corruption: ' + str(path))
        else:
            os.replace(_io(temporary), _io(path))
            _sync_directory(path.parent)
        if _hash(path) != digest:
            raise RuntimeError('Anomaly archive verification failed: ' + str(path))
        return dict(path=str(path), sha256=digest, rows=len(rows),
                    first_id=rows[0]['id'], last_id=rows[-1]['id'])
    finally:
        if _io(temporary).exists():
            _io(temporary).unlink()


def _verify_manifest(manifest, directory):
    count = 0
    for part in manifest['files']:
        path = _inside(part['path'], directory)
        if not _io(path).is_file() or _hash(path) != part['sha256']:
            raise RuntimeError('Anomaly archive manifest verification failed: ' + str(path))
        count += part['rows']
    if count != manifest['rows']:
        raise RuntimeError('Anomaly archive manifest row count mismatch')


def archive_anomalies(store, config):
    """Archive append-only raw anomalies, in 500-row pages of one read snapshot.

    The id high-water mark, rather than event time, includes late events. Each
    page's manifest is the publication commit record; database metadata advances
    only after all file hashes have been verified. No source rows are modified.
    """
    with _LOCK:
        history = _history(config)
        directory = _inside(history / '异常历史', history)
        _io(directory).mkdir(exist_ok=True)
        manifests = _inside(directory / '批次清单', directory)
        _io(manifests).mkdir(exist_ok=True)
        with store.lock:
            previous = store.get_meta(ANOMALY_KEY, {}) or {}
            through = int(previous.get('through_id', 0))
            database = next(row[2] for row in store.db.execute('PRAGMA database_list') if row[1] == 'main')
        if not database:
            raise ValueError('Anomaly archival requires a file-backed database')
        reader = sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True, timeout=30)
        reader.row_factory = sqlite3.Row
        result = dict(snapshot_max_id=through, archived_through_id=through,
                      rows_written=0, batches_written=0, files=[], pending=False)
        try:
            reader.execute('PRAGMA query_only=ON')
            reader.execute('BEGIN')
            maximum = reader.execute('SELECT coalesce(max(id),0) FROM anomalies').fetchone()[0]
            result['snapshot_max_id'] = maximum
            if through > maximum:
                raise RuntimeError('Anomaly archive high-water mark exceeds this database snapshot')
            while through < maximum:
                intent = previous.get('pending') or {}
                upper = int(intent.get('last_id', maximum))
                if upper > maximum or upper <= through:
                    raise RuntimeError('Anomaly pending batch does not match this database snapshot')
                rows = [dict(row) for row in reader.execute(
                    'SELECT * FROM anomalies WHERE id>? AND id<=? ORDER BY id LIMIT ?',
                    (through, upper, PAGE_SIZE)).fetchall()]
                if not rows:
                    raise RuntimeError('Anomaly archive snapshot contains an unexpected id gap')
                first, last = rows[0]['id'], rows[-1]['id']
                if intent and (first != intent['first_id'] or last != intent['last_id'] or len(rows) != intent['rows']):
                    raise RuntimeError('Anomaly pending batch rows have changed')
                if not intent:
                    # Preserve a partial final batch's boundary across crashes,
                    # even if newer rows arrive before the next invocation.
                    previous = dict(previous, through_id=through,
                                    pending=dict(first_id=first, last_id=last, rows=len(rows)))
                    store.meta(ANOMALY_KEY, previous)
                manifest_path = _inside(manifests / f'batch_{first:012d}_{last:012d}.json', directory)
                # Only one bounded page is held, even on the first full backfill.
                groups = {}
                for row in rows:
                    try:
                        month = dt.datetime.fromtimestamp(row['time'] / 1000, ZONE).strftime('%Y-%m')
                    except (TypeError, ValueError, OSError, OverflowError):
                        month = 'unknown-time'
                    groups.setdefault(month, []).append(row)
                parts = []
                for month, part_rows in sorted(groups.items()):
                    folder = _inside(directory / month, directory)
                    _io(folder).mkdir(exist_ok=True)
                    path = _inside(folder / f'batch_{first:012d}_{last:012d}.jsonl.gz', directory)
                    parts.append(_write_anomaly_part(path, part_rows))
                manifest = dict(version=1, previous_id=through, first_id=first,
                                last_id=last, rows=len(rows), files=parts)
                _verify_manifest(manifest, directory)
                if _io(manifest_path).exists():
                    if json.loads(_io(manifest_path).read_text(encoding='utf-8')) != manifest:
                        raise RuntimeError('Anomaly batch manifest collision: ' + str(manifest_path))
                else:
                    _publish_bytes(manifest_path, (_json(manifest) + '\n').encode('utf-8'))
                # Re-read the durable commit record, not merely an in-memory copy.
                committed = json.loads(_io(manifest_path).read_text(encoding='utf-8'))
                _verify_manifest(committed, directory)
                store.meta(ANOMALY_KEY, dict(through_id=last, last_manifest=str(manifest_path),
                    last_manifest_sha256=_hash(manifest_path), rows_archived=int(previous.get('rows_archived', 0))+len(rows)))
                previous = store.get_meta(ANOMALY_KEY)
                through = last
                result['archived_through_id'] = last
                result['rows_written'] += len(rows)
                result['batches_written'] += 1
                # A bounded result is important during a multi-year first backfill.
                result['files'] = [part['path'] for part in parts]
        except PermissionError as exc:
            result.update(pending=True, error=str(exc))
        finally:
            reader.close()
        return result


def _replace_references(store, source, target):
    """Commit both path registries together after a verified move."""
    source, target = str(source), str(target)
    def replacement(paths):
        return [target if str(resolved(value)) == source else value for value in paths]
    with store.lock, store.db:
        table = store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='signals_export_metadata'").fetchone()
        if table:
            # Path changes touch metadata only; signal payloads are never scanned.
            for row in store.db.execute('''SELECT interval,start,manifest FROM signals_export_metadata m
                WHERE EXISTS (SELECT 1 FROM json_each(m.manifest,'$.files') f
                WHERE replace(f.value,char(92),'/')=?)''', (source.replace('\\', '/'),)).fetchall():
                value = json.loads(row['manifest'])
                files = replacement(value.get('files', []))
                if files != value.get('files', []):
                    value['files'] = files
                    store.db.execute('UPDATE signals_export_metadata SET manifest=? WHERE interval=? AND start=?',
                                     (_json(value), row['interval'], row['start']))
        for interval in ('15m', '1h', '4h', '1d'):
            key = 'excel_archive_' + interval
            row = store.db.execute('SELECT value FROM metadata WHERE key=?', (key,)).fetchone()
            if not row:
                continue
            state = json.loads(row[0]); changed = False
            for entry in state.get('archived', {}).values():
                files = replacement(entry.get('files', []))
                if files != entry.get('files', []):
                    entry['files'] = files; changed = True
            if changed:
                store.db.execute('UPDATE metadata SET value=? WHERE key=?', (_json(state), key))


def _resume_move(store, history, move):
    source = _inside(move['source'], history)
    target = _inside(move['target'], history)
    if source.suffix.lower() != '.xlsx' or target.suffix.lower() != '.xlsx':
        raise ValueError('Only workbook archives may be tiered')
    try:
        if _io(source).exists():
            if move.get('sha256') is None:
                # Some Excel/security tools deny even shared reads. Keep a
                # pending intent, then capture and commit identity once readable.
                digest = _hash(source)
                if _io(target).exists() and _hash(target) != digest:
                    target = _inside(target.with_name(target.stem+'_快照_'+digest[:16]+'.xlsx'), history)
                move.update(sha256=digest, target=str(target))
                state = store.get_meta(TIER_KEY, {}) or {}
                state['pending'] = [move if item['source'] == move['source'] else item
                                    for item in state.get('pending', [])]
                store.meta(TIER_KEY, state)
            if _hash(source) != move['sha256']:
                return dict(move, error='Source changed after journal publication')
            _io(target.parent).mkdir(parents=True, exist_ok=True)
            if _io(target).exists() and _hash(target) != move['sha256']:
                return dict(move, error='Destination changed after journal publication')
            os.replace(_io(source), _io(target))
            _sync_directory(source.parent)
            _sync_directory(target.parent)
        if not _io(target).exists() or _hash(target) != move['sha256']:
            return dict(move, error='Destination missing or hash mismatch')
        _replace_references(store, source, target)
    except PermissionError as exc:
        return dict(move, error=str(exc))
    return None


def _known_archives(store):
    """Small manifest rows only; no historical signal/anomaly payload reads."""
    with store.lock:
        table = store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='signals_export_metadata'").fetchone()
    if table:
        last_interval, last_start = '', -9223372036854775808
        while True:
            with store.lock:
                page = store.db.execute('''SELECT interval,start,manifest FROM signals_export_metadata
                    WHERE interval>? OR (interval=? AND start>?) ORDER BY interval,start LIMIT ?''',
                    (last_interval, last_interval, last_start, PAGE_SIZE)).fetchall()
            if not page:
                break
            for row in page:
                manifest = json.loads(row['manifest'])
                end = manifest.get('report_period_end') or report_period(row['interval'], row['start'])[1]
                for path in manifest.get('files', []):
                    yield path, int(end)
            last_interval, last_start = page[-1]['interval'], page[-1]['start']
    for interval in ('15m', '1h', '4h', '1d'):
        state = store.get_meta('excel_archive_' + interval, {}) or {}
        for start, entry in state.get('archived', {}).items():
            end = report_period(interval, int(start))[1]
            for path in entry.get('files', []):
                yield path, end


def _file_period_end(path):
    match = re.search(r'\d{8}_\d{4}至(\d{8}_\d{4})', path.stem)
    if match:
        return int(dt.datetime.strptime(match.group(1), '%Y%m%d_%H%M').replace(tzinfo=ZONE).timestamp()*1000)
    return int(_io(path).stat().st_mtime * 1000)


def tier_archives(store, config, stamp):
    """Keep <=30-day files; move 31–90-day files by month, older files by year.

    Age is measured from reporting-period end. Only completed history workbooks
    are eligible. Pending moves are retried every call; inventory runs daily.
    """
    with _LOCK:
        history = _history(config)
        state = store.get_meta(TIER_KEY, {}) or {}
        result = dict(moved=0, pending=0, files=[])
        waiting = []
        pending_by_source = {move['source']: move for move in state.get('pending', [])}
        for move in pending_by_source.values():
            pending = _resume_move(store, history, move)
            if pending:
                waiting.append(pending)
            else:
                result['moved'] += 1
                result['files'] = [move['target']]
        state['pending'] = waiting
        store.meta(TIER_KEY, state)
        day = int(stamp) // DAY
        if state.get('last_scan_day') == day:
            result['pending'] = len(waiting)
            return result
        seen = set()
        waiting_sources = {str(resolved(move['source'])) for move in waiting}
        known = _known_archives(store)
        def candidates():
            yield from known
            # Legacy cumulative files have no new export metadata. Only inspect
            # the workbook history tree, excluding compressed anomaly archives.
            for folder in (history, history / '月归档', history / '年归档'):
                if not _io(folder).exists():
                    continue
                iterator = _io(folder).glob('*.xlsx') if folder == history else _io(folder).glob('*/*.xlsx')
                for path in iterator:
                    yield str(path), _file_period_end(path)
        for filename, end in candidates():
            source = resolved(filename)
            # Current report manifests may point at the Excel root.
            if history not in source.parents or source.suffix.lower() != '.xlsx' or not _io(source).is_file():
                continue
            if str(source) in seen:
                continue
            seen.add(str(source))
            if str(source) in waiting_sources:
                continue
            age = (int(stamp) - int(end)) // DAY
            if age <= 30:
                continue
            moment = dt.datetime.fromtimestamp(end / 1000, ZONE)
            folder = history / ('月归档' if age <= 90 else '年归档') / moment.strftime('%Y-%m' if age <= 90 else '%Y')
            target = _inside(folder / source.name, history)
            if source == target:
                continue
            # Scope is checked before any mutation, including recursive mkdir.
            source = _inside(source, history)
            try:
                digest = _hash(source)
                if _io(target).exists() and _hash(target) != digest:
                    target = _inside(target.with_name(target.stem + '_快照_' + digest[:16] + '.xlsx'), history)
            except PermissionError:
                digest = None
            move = dict(source=str(source), target=str(target), sha256=digest, report_period_end=int(end))
            state['pending'] = waiting + [move]
            store.meta(TIER_KEY, state)
            pending = _resume_move(store, history, move)
            if pending:
                waiting.append(pending)
            else:
                result['moved'] += 1
                result['files'] = [str(target)]
            state['pending'] = waiting
            store.meta(TIER_KEY, state)
        state['last_scan_day'] = day
        store.meta(TIER_KEY, state)
        result['pending'] = len(waiting)
        return result
