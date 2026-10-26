"""Explicit database maintenance; never invoked from the market-data loop.

Backups use SQLite's online backup API, not a filesystem copy of an open WAL
database. Vacuum takes the monitor's instance lock and makes its own verified
backup first. Neither operation removes signal, anomaly or EXPIRED records.
"""
import argparse
import hashlib
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path


def _write_json(path, value):
    """Atomically publish JSON, retrying bounded Windows sharing conflicts.

    Each writer owns its temporary file; exhausted retries leave the previous
    complete publication intact and propagate the error without changing ACLs.
    """
    path = Path(path)
    temporary = path.with_name(path.name + '.pending-' + uuid.uuid4().hex)
    try:
        with temporary.open('x', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        delays = (.05, .1, .2, .4, .8)
        for attempt in range(len(delays) + 1):
            try:
                os.replace(temporary, path)
                return
            except OSError as exc:
                if getattr(exc, 'winerror', None) not in (5, 32, 33) or attempt == len(delays):
                    raise
                time.sleep(delays[attempt])
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            # Preserve the original publication result if cleanup is blocked.
            pass


def database_inventory(connection):
    """Bounded-memory counts and a stable digest of all persisted signal IDs."""
    tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    counts = {table: connection.execute('SELECT count(*) FROM ' + table).fetchone()[0]
              for table in ('signals', 'anomalies', 'states', 'universe') if table in tables}
    digest = hashlib.sha256()
    if 'signals' in tables:
        for row in connection.execute('SELECT id,interval FROM signals ORDER BY id'):
            digest.update(json.dumps(list(row), ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
            digest.update(b'\n')
    return {'counts': counts, 'signal_ids_sha256': digest.hexdigest()}


def consistent_backup(source, destination, *, progress=None):
    source = Path(source).resolve(strict=True)
    destination = Path(destination).resolve()
    if source == destination or destination.exists() or destination.with_suffix('.json').exists():
        raise FileExistsError('Backup destination or manifest already exists; refusing to overwrite')
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + '.partial-' + uuid.uuid4().hex)
    started = time.time_ns() // 1_000_000
    reader = sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=30)
    writer = None
    try:
        reader.execute('BEGIN')
        reader.execute('SELECT count(*) FROM sqlite_master').fetchone()  # pin one WAL snapshot
        snapshot_time = time.time_ns() // 1_000_000
        before = database_inventory(reader)
        writer = sqlite3.connect(temporary, timeout=30)
        reader.backup(writer, pages=2048, progress=progress, sleep=.02)
        check = writer.execute('PRAGMA quick_check').fetchone()[0]
        after = database_inventory(writer)
        if check != 'ok' or before != after:
            raise RuntimeError('Consistent backup verification failed: ' + str(check))
        writer.execute('PRAGMA journal_mode=DELETE')
        writer.close()
        writer = None
        # Windows rename and POSIX same-directory hard links both publish
        # atomically and refuse to overwrite a concurrently created backup.
        if os.name == 'nt':
            os.rename(temporary, destination)
        else:
            os.link(temporary, destination)
            temporary.unlink()
        manifest = dict(source=str(source), backup=str(destination),
                        method='sqlite3.Connection.backup', started_ms=started,
                        snapshot_time_ms=snapshot_time, completed_ms=time.time_ns() // 1_000_000,
                        quick_check=check, bytes=destination.stat().st_size, **after)
        _write_json(destination.with_suffix('.json'), manifest)
        return manifest
    finally:
        if writer is not None:
            writer.close()
        reader.close()


class OfflineLock:
    """The same byte lock used by service.InstanceLock, without starting a service."""
    def __init__(self, directory):
        self.path = Path(directory) / 'instance.lock'

    def __enter__(self):
        self.file = self.path.open('a+b')
        self.file.seek(0)
        self.file.write(b'0')
        self.file.flush()
        self.file.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.file.close()
            raise RuntimeError('Monitor is running; offline maintenance refused') from exc
        return self

    def __exit__(self, *args):
        self.file.close()


def offline_vacuum(data_dir, backup_dir):
    """Explicit offline-only operation, guarded by a fresh verified backup."""
    data_dir = Path(data_dir).resolve(strict=True)
    source = data_dir / 'monitor.sqlite3'
    with OfflineLock(data_dir):
        backup = Path(backup_dir) / ('before-vacuum-' + str(time.time_ns()) + '.sqlite3')
        manifest = consistent_backup(source, backup)
        connection = sqlite3.connect(source, timeout=0)
        try:
            before = database_inventory(connection)
            connection.execute('PRAGMA busy_timeout=0')
            connection.execute('VACUUM')
            after = database_inventory(connection)
            check = connection.execute('PRAGMA quick_check').fetchone()[0]
            if before != after or check != 'ok':
                raise RuntimeError('Post-VACUUM inventory mismatch; verified backup: ' + str(backup))
            result = dict(operation='offline_vacuum', completed_ms=time.time_ns() // 1_000_000,
                          backup=manifest, quick_check=check, inventory=after)
            _write_json(backup.with_suffix('.vacuum.json'), result)
            return result
        finally:
            connection.close()


def main():
    parser = argparse.ArgumentParser(description='Explicit SQLite backup / offline maintenance')
    commands = parser.add_subparsers(dest='command', required=True)
    backup = commands.add_parser('backup')
    backup.add_argument('--source', required=True)
    backup.add_argument('--destination', required=True)
    vacuum = commands.add_parser('vacuum')
    vacuum.add_argument('--data-dir', required=True)
    vacuum.add_argument('--backup-dir', required=True)
    args = parser.parse_args()
    result = (consistent_backup(args.source, args.destination) if args.command == 'backup'
              else offline_vacuum(args.data_dir, args.backup_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
