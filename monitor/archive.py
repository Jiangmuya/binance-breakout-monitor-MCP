"""Calendar periods and recoverable workbook moves; no market/strategy changes."""
import datetime as dt
import hashlib
import json
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
    with _io(path).open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _io(path):
    path = Path(path)
    if os.name != 'nt':
        return path
    value = str(path.absolute())
    if value.startswith('\\\\?\\') or len(value) < 240:
        return path
    return Path('\\\\?\\UNC\\' + value[2:] if value.startswith('\\\\') else '\\\\?\\' + value)


def _collision_target(target, expected):
    """Resolve a collision before journaling; never replace different bytes."""
    if not _io(target).exists() or _file_hash(target) == expected:
        return target
    for suffix in (expected[:16], expected):
        candidate = target.with_name(target.stem+'_快照_'+suffix+target.suffix)
        if not _io(candidate).exists() or _file_hash(candidate) == expected:
            return candidate
    raise FileExistsError('Archive collision targets contain different bytes: '+str(target))


def move_pending(root, moves, *, prepared=None, verified=None):
    """Move journaled files, committing a selected target before its rename.

    ``prepared`` must durably save the mutated journal before filesystem changes.
    ``verified`` can atomically update canonical metadata after target hash checks.
    Missing or changed sources are never sufficient proof of a completed move.
    """
    root = resolved(root)
    pending = []
    for move in moves:
        source = resolved(move['source'])
        target = archive_target(root, Path(move['target']).name)
        if source.parent != root or target != resolved(move['target']):
            raise ValueError('Workbook move escaped its intended directory')
        try:
            expected = move.get('expected_hash')
            source_hash = _file_hash(source) if _io(source).exists() else None
            if expected and source_hash != expected:
                # Supports older journals whose collision destination was not
                # recorded, and a stale journal whose source name has been reused.
                candidates = [target, target.with_name(target.stem+'_快照_'+expected[:16]+target.suffix),
                              target.with_name(target.stem+'_快照_'+expected+target.suffix)]
                recovered = next((p for p in candidates if _io(p).is_file() and _file_hash(p) == expected), None)
                if recovered is None:
                    raise FileNotFoundError('Archive source missing/changed and verified destination unavailable: '+str(source))
                target = recovered
                move.update(source=str(source), target=str(target), completed=False)
                if prepared: prepared(move)
            else:
                if source_hash is None:
                    raise FileNotFoundError('Archive source missing without a verifiable content hash: '+str(source))
                expected = source_hash
                target = _collision_target(target, expected)
                move.update(source=str(source), target=str(target), expected_hash=expected, completed=False)
                move.pop('error', None)
                # A crash after replace will now point at this exact destination.
                if prepared: prepared(move)
                os.replace(_io(source), _io(target))
            if not _io(target).is_file() or _file_hash(target) != expected:
                raise OSError('Moved workbook failed its content verification: '+str(target))
            move.update(target=str(target), completed=True)
            move.pop('error', None)
            if verified: verified(move)
        except (PermissionError, FileNotFoundError, FileExistsError) as exc:
            move.update(source=str(source), target=str(target), completed=False, error=str(exc))
            pending.append(move)
    return pending


def _update_move_entry(state, move):
    period = move.get('period_start')
    if period is None:
        return
    entry = state.setdefault('archived', {}).setdefault(str(period),
        dict(signature=move.get('period_signature'), files=[], moves=[]))
    records = {item['source']: item for item in entry.get('moves', [])}
    records[move['source']] = dict(move)
    entry['moves'] = list(records.values())
    entry['files'] = [item['target'] for item in entry['moves']]


def _save_publication(store, key, state, interval, completed=None):
    """One transaction updates state and only existing canonical membership."""
    with store.lock, store.db:
        if completed and completed.get('period_start') is not None:
            table = store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='signals_export_metadata'").fetchone()
            if table:
                row = store.db.execute('SELECT manifest FROM signals_export_metadata WHERE interval=? AND start=?',
                    (interval, completed['period_start'])).fetchone()
                if row:
                    manifest = json.loads(row[0])
                    source = resolved(completed['source'])
                    original = manifest.get('files', [])
                    files = [completed['target'] if resolved(path) == source else path for path in original]
                    if files != original:
                        manifest['files'] = files
                        store.db.execute('UPDATE signals_export_metadata SET manifest=? WHERE interval=? AND start=?',
                            (json.dumps(manifest, ensure_ascii=False, separators=(',', ':')), interval, completed['period_start']))
        store.db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)',
            (key, json.dumps(state, ensure_ascii=False, separators=(',', ':'))))


def begin_publication(store, config, interval, label, start, end, stamp):
    """Journal old-file moves before publishing a new reporting period."""
    root = resolved(config['excel_dir'])
    _io(root).mkdir(parents=True, exist_ok=True)
    directory = archive_target(root, 'placeholder.xlsx').parent
    _io(directory).mkdir(exist_ok=True)
    key = 'excel_archive_'+interval
    with store.lock:
        state = store.get_meta(key)
        prefix = '币安USDT合约监控_'+label
        moves = []; closing = None
        if state is None:
            state = dict(first_start=start, current_start=start, files=[], archived={}, pending_moves=[])
            migration = dt.datetime.fromtimestamp(stamp/1000, ZONE).strftime('%Y%m%d_%H%M%S')
            for source in sorted(_io(root).glob(prefix+'*.xlsx')):
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
                    moves.append(dict(source=str(source), target=str(archive_target(root, filename)),
                        period_start=old_start, period_signature=closing[1]))
        combined = {m['source']: m for m in state.get('pending_moves', [])}
        for move in moves:
            if move['source'] in combined:
                # A restart must not erase an earlier journal's actual collision
                # target or content hash just because the source has disappeared.
                existing = combined[move['source']]
                for name in ('period_start', 'period_signature'):
                    if name in move: existing.setdefault(name, move[name])
                continue
            try:
                if _io(move['source']).exists():
                    move['expected_hash'] = _file_hash(Path(move['source']))
            except PermissionError:
                pass
            combined[move['source']] = move
        state['pending_moves'] = list(combined.values())
        for move in state['pending_moves']:
            _update_move_entry(state, move)
        _save_publication(store, key, state, interval)
    def prepared(move):
        _update_move_entry(state, move)
        _save_publication(store, key, state, interval)
    def verified(move):
        _update_move_entry(state, move)
        _save_publication(store, key, state, interval, completed=move)
    pending = move_pending(root, state['pending_moves'], prepared=prepared, verified=verified)
    state['pending_moves'] = pending
    if state['current_start'] != start:
        state['files'] = []
    # Commit the new path ownership before any current workbook uses the base name.
    # This prevents a restart after publication from archiving a new file as the old period.
    state['current_start'] = start
    _save_publication(store, key, state, interval)
    return state
