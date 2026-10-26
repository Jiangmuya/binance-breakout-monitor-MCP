"""Offline acceptance of a verified, completed SQLite backup. Never opens production."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys
import time
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from monitor.core import Store, PERIODS
from monitor.export import export_all
from monitor.export_index import ExportIndex
from monitor.history_maintenance import ANOMALY_KEY
from monitor.maintenance import database_inventory, _write_json
from export_soak import reconcile
from run_shadow import code_hash


def payload_digest(db):
    result=hashlib.sha256()
    for row in db.execute('SELECT id,payload FROM signals ORDER BY id'):
        for value in row:
            result.update(value.encode('utf-8'));result.update(b'\0')
    return result.hexdigest()


def run(args):
    tested_hash=code_hash()
    backup=Path(args.backup).resolve(strict=True)
    manifest=json.loads(backup.with_suffix('.json').read_text(encoding='utf-8'))
    if manifest.get('method',manifest.get('backup_method'))!='sqlite3.Connection.backup' or manifest.get('quick_check')!='ok':
        raise ValueError('A completed verified online-backup manifest is required')
    if Path(manifest['backup']).resolve()!=backup:
        raise ValueError('Backup manifest does not identify this file')
    output=Path(args.output).resolve()
    output.mkdir(parents=True,exist_ok=False)
    runtime=output/'runtime';runtime.mkdir();(output/'excel').mkdir()
    target=runtime/'monitor.sqlite3'
    # This source is a completed, closed backup, never the running SQLite file.
    shutil.copyfile(backup,target)
    config=json.loads(Path(args.config).read_text(encoding='utf-8-sig'))
    config.update(data_dir=str(runtime),excel_dir=str(output/'excel'),node_executable=args.node,webhook_authorized=False)
    stamp=int(manifest.get('snapshot_time_ms',manifest['started_ms']))
    store=Store(target,config)
    try:
        before=dict(inventory=database_inventory(store.db),payload_sha256=payload_digest(store.db))
        # Discard only disposable destination ownership; copied paths refer to production.
        keys=[r[0] for r in store.db.execute('SELECT key FROM metadata')]
        for key in keys:
            if key.startswith(('excel_archive_','pending_export_batches_','anomaly_archive_','history_tier_')) or key in (
                'last_export','export_health','history_maintenance_health'):
                store.db.execute('DELETE FROM metadata WHERE key=?',(key,))
        store.db.commit()
        runs=[]
        for label in ('cold_backfill','unchanged_refresh'):
            started=time.monotonic()
            with patch('monitor.export.now_ms',return_value=stamp):
                export_all(store,config,backfill=label=='cold_backfill')
            spec=json.loads((runtime/'export-cache/15m-1h-4h-1d/snapshot.json').read_text(encoding='utf-8'))
            runs.append(dict(label=label,wall_seconds=round(time.monotonic()-started,3),
                books=len(spec['books']),history_books=sum(b['archive'] for b in spec['books']),
                signal_rows=sum(b['row_count'] for b in spec['books']),
                cache=spec['books'][-1]['row_cache'],health=store.get_meta('export_health'),
                history=store.get_meta('history_maintenance_health')))
        files=sorted({p for row in ExportIndex(store.db).manifest() for p in row['manifest']['files']})
        _write_json(output/'canonical-files.json',files)
        agreement=reconcile(target,files,output/'reconciliation')
        after=dict(inventory=database_inventory(store.db),payload_sha256=payload_digest(store.db))
        archive=store.get_meta(ANOMALY_KEY,{})
        anomalies=store.db.execute('SELECT coalesce(max(id),0) FROM anomalies').fetchone()[0]
        result=dict(source=str(backup),snapshot_ms=stamp,code_hash=tested_hash,code_unchanged=tested_hash==code_hash(),runs=runs,reconciliation=agreement,
            unchanged_payloads=before['payload_sha256']==after['payload_sha256'],before=before,after=after,
            anomaly_archive=archive,anomaly_archive_complete=archive.get('through_id')==anomalies,
            unchanged_history_skipped=runs[-1]['history_books']==0,
            strategy_version=store.version)
        result['passed']=all((result['code_unchanged'],agreement['passed'],result['unchanged_payloads'],result['anomaly_archive_complete'],
            result['unchanged_history_skipped'],not any(r.get('blocked') or r.get('error') for r in runs[-1]['history'].values())))
        _write_json(output/'acceptance.json',result)
        print(json.dumps(dict(passed=result['passed'],output=str(output),runs=[{k:r[k] for k in ('label','wall_seconds','books','history_books')} for r in runs]),indent=2))
        if not result['passed']:raise RuntimeError('Snapshot acceptance failed')
    finally:store.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('backup','config','output','node'):parser.add_argument('--'+name,required=True)
    run(parser.parse_args())
