"""Calendar periods and recoverable workbook moves; no market/strategy changes."""
import datetime as dt
import hashlib
import os
from pathlib import Path

ARCHIVE_DIR = '历史币安USDT合约监控'
DAY = 86400000
ZONE = dt.timezone(dt.timedelta(hours=8))
ANCHOR = int(dt.datetime(2026, 10, 19, tzinfo=dt.timezone.utc).timestamp()*1000)


def report_period(interval, stamp):
    # Beijing 08:00 is UTC 00:00, including the Monday/month boundaries.
    moment = dt.datetime.fromtimestamp(stamp/1000, dt.timezone.utc)
    midnight = int(moment.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()*1000)
    if interval == '15m':
        return midnight, midnight+DAY
    if interval == '1h':
        start = midnight-moment.weekday()*DAY
        return start, start+7*DAY
    if interval == '4h':
        start = ANCHOR+((int(stamp)-ANCHOR)//(14*DAY))*(14*DAY)
        return start, start+14*DAY
    if interval == '1d':
        start = moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(year=start.year+1, month=1) if start.month == 12 else start.replace(month=start.month+1)
        return int(start.timestamp()*1000), int(end.timestamp()*1000)
    raise ValueError('Invalid report interval')


def report_timestamp(payload):
    for key in ('discovered', 'processed'):
        if payload.get(key) is not None:
            return payload[key]
    return payload['bar']['T']+1


def period_label(start, end):
    def label(stamp):
        return dt.datetime.fromtimestamp(stamp/1000, ZONE).strftime('%Y%m%d_%H%M')
    return label(start)+'至'+label(end)


def fingerprint(rows):
    import json
    content = json.dumps(rows, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def split_rows(rows, cap):
    """Keep a reporting period together, splitting only at the existing row cap."""
    chunks = []; chunk = []; comparison_count = 0
    for row in rows:
        count = len(row.get('previous15', row.get('previous20', [])))
        if chunk and (len(chunk) >= cap or comparison_count+count > cap):
            chunks.append(chunk); chunk = []; comparison_count = 0
        chunk.append(row); comparison_count += count
    if chunk:
        chunks.append(chunk)
    return chunks or [[]]


def resolved(path):
    value = str(Path(path).resolve())
    # Windows may return an extended-length spelling only after another worker
    # creates the directory. Compare the same canonical spelling in either case.
    if os.name == 'nt':
        if value.startswith('\\\\?\\UNC\\'):
            value = '\\\\'+value[8:]
        elif value.startswith('\\\\?\\'):
            value = value[4:]
    return Path(value)


def archive_target(root, filename):
    root = resolved(root)
    directory = resolved(root/ARCHIVE_DIR)
    target = resolved(directory/filename)
    if directory.parent != root or target.parent != directory:
        raise ValueError(f'Archive path must stay inside the Excel directory: {root!s}; {directory!s}; {target!s}')
    return target


def _file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def move_pending(root, moves):
    """Retry only journaled paths; keep distinct existing snapshots on collision."""
    root = resolved(root)
    pending = []
    for move in moves:
        source = resolved(move['source'])
        target = archive_target(root, Path(move['target']).name)
        if source.parent != root or target != resolved(move['target']):
            raise ValueError('Workbook move escaped its intended directory')
        if not source.exists():
            continue
        try:
            source_hash = _file_hash(source)
            if move.get('expected_hash') and source_hash != move['expected_hash']:
                # A completed move followed by a crash can leave a stale journal while
                # this source name already belongs to the newly published workbook.
                continue
            if target.exists() and source_hash != _file_hash(target):
                target = target.with_name(target.stem+'_快照_'+source_hash[:16]+target.suffix)
            move['target'] = str(target)
            os.replace(source, target)
        except PermissionError:
            pending.append(dict(move, source=str(source), target=str(target)))
    return pending


def begin_publication(store, config, interval, label, start, end, stamp):
    """Journal old-file moves before publishing a new reporting period."""
    root = resolved(config['excel_dir'])
    root.mkdir(parents=True, exist_ok=True)
    directory = archive_target(root, 'placeholder.xlsx').parent
    directory.mkdir(exist_ok=True)
    key = 'excel_archive_'+interval
    with store.lock:
        state = store.get_meta(key)
        prefix = '币安USDT合约监控_'+label
        moves = []; closing = None
        if state is None:
            state = dict(first_start=start, current_start=start, files=[], archived={}, pending_moves=[])
            migration = dt.datetime.fromtimestamp(stamp/1000, ZONE).strftime('%Y%m%d_%H%M%S')
            for source in sorted(root.glob(prefix+'*.xlsx')):
                if source.name.endswith('.pending.xlsx'):
                    continue
                target = archive_target(root, source.stem+'_旧版累计_'+migration+'.xlsx')
                moves.append(dict(source=str(resolved(source)), target=str(target)))
        elif state['current_start'] != start:
            old_start, old_end = report_period(interval, state['current_start'])
            closing = (old_start, state.get('current_signature'))
            suffix = period_label(old_start, old_end)
            for name in state.get('files', []):
                source = resolved(name)
                # Capacity/archive books already in the archive directory must not be moved again.
                if source.parent == root:
                    base = source.stem
                    filename = (prefix+'_'+suffix+'.xlsx' if base == prefix
                                else prefix+'_'+suffix+'_'+base[len(prefix):].lstrip('_')+'.xlsx')
                    moves.append(dict(source=str(source), target=str(archive_target(root, filename))))
        combined = {m['source']: m for m in state.get('pending_moves', [])}
        for move in moves:
            source = Path(move['source'])
            if source.exists():
                move['expected_hash'] = _file_hash(source)
        combined.update({m['source']: m for m in moves})
        # Durable journal: a crash after a move cannot lose its intended destination.
        state['pending_moves'] = list(combined.values())
        store.meta(key, state)
    pending = move_pending(root, state['pending_moves'])
    if closing and closing[1] is not None and moves:
        state.setdefault('archived', {})[str(closing[0])] = dict(
            signature=closing[1], files=[m['target'] for m in moves])
    state['pending_moves'] = pending
    if state['current_start'] != start:
        state['files'] = []
    # Commit the new path ownership before any current workbook uses the base name.
    # This prevents a restart after publication from archiving a new file as the old period.
    state['current_start'] = start
    with store.lock:
        store.meta(key, state)
    return state
