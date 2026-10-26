"""Reproducible, offline streaming-export load and OOXML reconciliation check.

Writes synthetic input only under --output; never opens the production database.
The default load is 10,000 signals plus 150,000 historical comparison rows.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import zipfile
import xml.etree.ElementTree as ET


def dump_rows(path, rows):
    count = 0
    with path.open('w', encoding='utf-8') as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
            count += 1
    return {'ndjson': str(path.resolve()), 'count': count}


def source_hashes(candidate):
    """Bind results to bytes present immediately before each worker starts."""
    return {name: hashlib.sha256((candidate / name).read_bytes()).hexdigest()
            for name in ('excel_builder.mjs', 'streaming_xlsx.mjs',
                         'package.json', 'package-lock.json',
                         'tests/benchmark_streaming_excel.py')}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--signals', type=int, default=10_000)
    parser.add_argument('--runs', type=int, default=2)
    parser.add_argument('--reuse-input', type=Path,
                        help='Reuse NDJSON from this previous benchmark snapshot; use a new output/cache directory')
    args = parser.parse_args()
    if args.signals < 1 or args.runs < 1:
        parser.error('signals and runs must be positive')
    target = args.output.resolve()
    target.mkdir(parents=True, exist_ok=True)
    reused = None
    if args.reuse_input:
        reused = json.loads(args.reuse_input.read_text(encoding='utf-8'))['books'][0]
        if reused['triggers']['count'] != args.signals:
            parser.error('reused trigger count must equal --signals')
    def rows_for(key, filename, rows):
        return reused[key] if reused is not None else dump_rows(target / filename, rows)
    stamp = 1792627200000
    def triggers():
        for index in range(args.signals):
            yield dict(signal_id=f'synthetic-{index:08}', symbol=f'TEST{index%524:04}USDT',
                       contract_type='PERPETUAL', interval='15m', strategy_version='fixture-v3',
                       comparison_window=15, t=stamp-900000, T=stamp-1,
                       close_boundary=stamp, discovered=stamp+index,
                       o=1., h=1.2, l=.9, c=1.18, v=500.+index, q=550.+index,
                       EMA12=1.1, EMA50=1.05, EMA200=1., V_mean=100., V_max=300.,
                       body_share=.9, upper_wick_share=.1, volume_highlight=index%2==0,
                       check1=True, check2=True, check3=True, check4=True, check5=True,
                       check6=True, thresholds='{"volume_multiple":4}', event_type='TRIGGER',
                       notification_status='SENT', attempts=1)
    def previous():
        for index in range(args.signals):
            for prior in range(1,16):
                yield dict(signal_id=f'synthetic-{index:08}', symbol=f'TEST{index%524:04}USDT',
                           interval='15m', index=prior, comparison_window=15,
                           t=stamp-(16-prior)*900000, T=stamp-(15-prior)*900000-1,
                           close_boundary=stamp-(15-prior)*900000,
                           o=1., h=1.1, l=.9, c=1.01, v=100.+prior, q=100.+prior,
                           abs_body=.01, relative_body=.01, bar_return=.01, close_return=.005)
    book = dict(filename='synthetic-load.xlsx', interval='15m', generated=stamp+20000,
                comparison_window=15, report_period_start=stamp, report_period_end=stamp+86400000,
                refresh_boundary=stamp, refresh_reason='CANDLE_CLOSE',
                coverage=dict(completed=524,expected=524,missing=[]))
    book['triggers']=rows_for('triggers','triggers.ndjson',triggers())
    book['previous']=rows_for('previous','previous.ndjson',previous())
    book['overview']=rows_for('overview','overview.ndjson',(
        dict(symbol=f'TEST{i:04}USDT',contract_type='PERPETUAL',close_boundary=stamp,
             c=1.18,EMA12=1.1,EMA50=1.05,EMA200=1.,health='OK',readiness='READY',
             updated=stamp,comparison_window=15,volume_highlight=False) for i in range(524)))
    book['status']=rows_for('status','status.ndjson',(
        dict(time=stamp,symbol=f'TEST{i:04}USDT',interval='15m',code='OK',detail='合成负载测试') for i in range(524)))
    book['recent_anomalies']=rows_for('recent_anomalies','recent.ndjson',(
        dict(first_time=stamp-i,last_time=stamp,occurrence_count=i+1,symbol=f'TEST{i%524:04}USDT',
             interval='15m',code='SYNTHETIC',detail='合成异常，不是正式行情。') for i in range(2000)))
    book['parameters']=[dict(key='来源',value='离线合成负载，仅测试导出。')]
    spec = dict(format_version=4, books=[book], format_cache=str(target/'formatted.sqlite3'),
                budget=dict(rss_bytes=512*1024*1024,block_rows=5000,block_bytes=50*1024*1024))
    spec_path=target/'snapshot.json'
    spec_path.write_text(json.dumps(spec,ensure_ascii=False),encoding='utf-8')
    candidate=Path(__file__).resolve().parents[1]
    node_version=subprocess.run([args.node,'--version'],capture_output=True,text=True,check=True).stdout.strip()
    cache_initially_exists=(target/'formatted.sqlite3').exists()
    runs=[]
    for run in range(args.runs):
        provenance=dict(started_at_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                        source_sha256=source_hashes(candidate),node_executable=args.node,
                        node_version=node_version,
                        snapshot_sha256=hashlib.sha256(spec_path.read_bytes()).hexdigest())
        # Persist before launching: never attach current hashes to older evidence.
        (target/f'run-{run+1}-start.json').write_text(json.dumps(provenance,indent=2),encoding='utf-8')
        start=time.perf_counter()
        result=subprocess.run([args.node,'--max-old-space-size=256',str(candidate/'excel_builder.mjs'),
                               str(spec_path),str(target/'xlsx')],cwd=candidate,
                              capture_output=True,text=True,encoding='utf-8',check=True)
        metrics=json.loads((target/'xlsx'/'metrics.json').read_text(encoding='utf-8'))
        metrics['wall_seconds']=round(time.perf_counter()-start,3)
        metrics['provenance']=provenance
        metrics['source_unchanged_during_run']=source_hashes(candidate)==provenance['source_sha256']
        (target/f'run-{run+1}-metrics.json').write_text(json.dumps(metrics,indent=2),encoding='utf-8')
        assert metrics['source_unchanged_during_run'],'Writer or dependency lock changed during benchmark'
        assert metrics['status']=='SUCCESS',metrics
        assert metrics['peak_rss_bytes']<=512*1024*1024,metrics
        assert metrics['peak_heap_bytes']<=256*1024*1024,metrics
        if run: assert metrics['cache_hits']>=args.signals*16, metrics
        runs.append(metrics)
        print(json.dumps({'run':run+1,'duration_ms':metrics['duration_ms'],
                          'peak_rss_bytes':metrics['peak_rss_bytes'],'cache_hits':metrics['cache_hits'],
                          'cache_misses':metrics['cache_misses']}),flush=True)
    ns='{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
    actual_hash=hashlib.sha256()
    with zipfile.ZipFile(target/'xlsx'/'synthetic-load.xlsx') as archive:
        assert archive.testzip() is None
        counts=[]
        for sheet in range(1,7):
            count=0
            with archive.open(f'xl/worksheets/sheet{sheet}.xml') as stream:
                for event,row in ET.iterparse(stream,events=('end',)):
                    if row.tag != ns+'row': continue
                    number=int(row.attrib['r'])
                    if number>=7:
                        count+=1
                        if sheet==1:
                            cells=list(row)
                            assert cells[-2].attrib['r'].endswith(str(number))
                            value=''.join(cells[-2].itertext())
                            assert value==f'synthetic-{count-1:08}', (count,value)
                            actual_hash.update((value+'\n').encode())
                    row.clear()
            counts.append(count)
        assert counts==[args.signals,524,args.signals*15,524,1,2000],counts
    assert actual_hash.hexdigest()==runs[-1]['books'][0]['signal_id_sha256']
    report=dict(signals=args.signals,rows=sum(counts),reconciled=True,
                signal_id_sha256=actual_hash.hexdigest(),runs=runs,
                cache_initially_exists=cache_initially_exists)
    (target/'acceptance.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps({'reconciled':True,'signals':args.signals,'rows':sum(counts)}))


if __name__=='__main__':
    main()
