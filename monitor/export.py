"""Database snapshot -> artifact-tool workbooks, isolated from the market loop."""
import asyncio
import collections
import datetime as dt
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from .export_index import ExportIndex
from .row_cache import RowCache
from .export_runtime import (EXPORT_LOCK, settings, worker_revision, run_builder,
    check_circuit, record_failure, record_success, export_health)
from .core import PERIODS, WINDOW, dumps, now_ms, stored_volume_highlight, rules_for_interval
from .archive import (ARCHIVE_DIR, report_period, report_timestamp, period_label, fingerprint,
                      split_rows, archive_target, begin_publication, resolved)

LABELS={'15m':'15分钟','1h':'1小时','4h':'4小时','1d':'日线'}
SOURCE='https://fapi.binance.com/fapi/v1/klines'
# Export policy only: these constants do not change the versioned trading rules.
CLOSE_WAIT_SECONDS=30
CLOSE_POLL_SECONDS=.5

def next_boundary(stamp,interval):
    step=PERIODS[interval]
    return (stamp//step+1)*step

def close_coverage(store,interval,boundary):
    """Report committed close records separately from current health warnings."""
    from .close_applicability import close_applicable,load_exact_evaluations,close_processed
    target=boundary-PERIODS[interval]
    with store.lock:
        rows=store.db.execute("""SELECT u.symbol,u.info,json_extract(s.state,'$.last') AS last,
            json_extract(s.state,'$.latest.bar.t') AS evaluated,json_extract(s.state,'$.health') AS health
            FROM universe u LEFT JOIN states s ON s.symbol=u.symbol AND s.interval=? WHERE u.active=1
            AND json_extract(u.info,'$.contractType')='PERPETUAL' """,(interval,)).fetchall()
        exact=load_exact_evaluations(store.db,interval,target)
    applicable=[r for r in rows if close_applicable(json.loads(r['info']),boundary)]
    expected_symbols={r['symbol'] for r in applicable}
    not_applicable=[r['symbol'] for r in rows if r['symbol'] not in expected_symbols]
    missing=[r['symbol'] for r in applicable if not close_processed(r['last'],r['evaluated'],r['health'],target,r['symbol'] in exact)]
    warnings=[r['symbol'] for r in applicable if r['health']!='OK']
    return dict(expected=len(applicable),completed=len(applicable)-len(missing),missing=missing,
                total_symbols=len(rows),not_applicable=not_applicable,current_warnings=warnings)


def safe_replace(temp,target):
    try:
        os.replace(temp,target)
        return str(target),False
    except PermissionError:
        alternate=target.with_name(target.stem+'_snapshot_'+str(now_ms())+'.xlsx')
        os.replace(temp,alternate)
        return str(alternate),True

def comparison_window(p):
    """Retain the window that produced a persisted record, including v1 history."""
    m=p.get('metrics',{})
    return p.get('comparison_window',m.get('comparison_window',
        15 if 'V15_mean' in m or 'previous15' in p else 20))

def comparison_bars(p):
    return p.get('previous15',p.get('previous20',[]))

def flatten(p):
    b=p['bar'];m=p['metrics']
    row={**b,**m,'symbol':p['symbol'],'interval':p['interval'],'contract_type':p['contract_type'],
         'EMA12':p['ema'][0],'EMA50':p['ema'][1],'EMA200':p['ema'][2],
         'readiness':p['readiness'],'seed':p['seed'],'warmup':p['count'],
         'strategy_version':p['strategy_version'],'discovered':p['discovered'],
         'event_time':p.get('event_time'),'processed':p['processed'],'close_boundary':b['T']+1}
    window=comparison_window(p)
    row['comparison_window']=window
    for prefix,suffixes in (('V',('mean','median','max')),('B',('mean',)),
                            ('G',('mean','median','max')),('abs_body',('mean','median','max'))):
        for suffix in suffixes:
            row[f'{prefix}_{suffix}']=m.get(f'{prefix}{window}_{suffix}')
    row['H']=m.get(f'H{window}')
    row['volume_highlight']=stored_volume_highlight(p)
    row['volume_highlight_multiple']=m.get('volume_highlight_multiple',
        m.get('thresholds',{}).get('volume_highlight_multiple',5))
    row['received']=p.get('received')
    row.update({f'check{i+1}':bool(v) for i,v in enumerate(m['checks'])})
    row.update({k:p.get(k) for k in ('signal_id','event_type','origin','latency_ms')})
    row['thresholds']=dumps(m.get('thresholds',{}))
    return row

def previous_rows(p):
    window=comparison_window(p)
    output=[];prev=p.get(f'previous{window}_before_close')
    for n,b in enumerate(comparison_bars(p),1):
        row={**b,'signal_id':p['signal_id'],'symbol':p['symbol'],'interval':p['interval'],'index':n,
             'comparison_window':window,
             'abs_body':abs(b['c']-b['o']),'relative_body':abs(b['c']/b['o']-1),
             'bar_return':b['c']/b['o']-1,'close_return':b['c']/prev-1 if prev else None,'close_boundary':b['T']+1}
        output.append(row);prev=b['c']
    return output

def partitions(signals,cap):
    groups=collections.defaultdict(list)
    # Mixed historical/current records retain their actual 20/15 comparison rows.
    for p in signals:
        month=dt.datetime.fromtimestamp(p['bar']['t']/1000,dt.timezone.utc).strftime('%Y-%m')
        groups[month].append(p)
    result=[]
    for month,rows in sorted(groups.items()):
        chunk=[];comparison_count=0;number=1
        for p in rows:
            count=len(comparison_bars(p))
            if chunk and (comparison_count+count>cap or len(chunk)>=cap):
                result.append((month,number,chunk));number+=1;chunk=[];comparison_count=0
            chunk.append(p);comparison_count+=count
        if chunk: result.append((month,number,chunk))
    return result

def snapshot(store,config,intervals=None,refresh_reason='MANUAL',refresh_boundary=None,*,stream_dir=None,backfill=False):
    selected=list(PERIODS) if intervals is None else list(intervals)
    if not selected or any(i not in PERIODS for i in selected): raise ValueError('Invalid export intervals')
    from .report_snapshot import indexed_snapshot
    return indexed_snapshot(store,config,selected,refresh_reason,refresh_boundary,now_ms(),stream_dir,backfill)


def export_all(store,config,render=False,intervals=None,refresh_reason='MANUAL',refresh_boundary=None,*,backfill=False):
    selected=list(PERIODS) if intervals is None else list(intervals)
    if not selected or any(i not in PERIODS for i in selected): raise ValueError('Invalid export intervals')
    options=settings(config);revision=worker_revision(options)
    queued=time.monotonic()
    with EXPORT_LOCK:
        selected=[i for i in selected if not check_circuit(store,i,revision)]
        if not selected:return []
        started=time.monotonic()
        try:
            return _publish_locked(store,config,render,selected,refresh_reason,refresh_boundary,
                options=options,revision=revision,backfill=backfill,queue_wait_ms=round((started-queued)*1000))
        except Exception as exc:
            if not getattr(exc,'recorded_export_failure',False):
                record_failure(store,selected,type(exc).__name__,str(exc),
                    dict(duration_ms=round((time.monotonic()-started)*1000),peak_rss_bytes=0),revision)
            raise


def _publish_locked(store,config,render,intervals,refresh_reason,refresh_boundary,*,options,revision,backfill,queue_wait_ms):
    selected=list(PERIODS) if intervals is None else list(intervals)
    # Independent period workers must never overwrite one another's input/cache.
    cache=Path(config['data_dir'])/'export-cache'
    if intervals is not None: cache=cache/'-'.join(selected)
    cache.mkdir(parents=True,exist_ok=True)
    started=time.monotonic()
    books=snapshot(store,config,selected,refresh_reason,refresh_boundary,stream_dir=cache,backfill=backfill)
    spec=cache/'snapshot.json';spec.write_text(dumps({'format_version':4,'books':books,'render':render,
        'format_cache':str(Path(config['data_dir'])/'export-cache/formatted-rows.sqlite3'),
        'budget':dict(rss_bytes=options['worker_rss_bytes'],block_rows=options['block_rows'],block_bytes=options['block_bytes'])}),encoding='utf-8')
    command=[config['node_executable'],f'--max-old-space-size={int(options["node_heap_mb"])}',
        str(Path(__file__).parent.parent/'excel_builder.mjs'),str(spec),str(cache)]
    result=run_builder(command,cache=cache,options=options)
    metrics=getattr(result,'metrics',dict(duration_ms=round((time.monotonic()-started)*1000),peak_rss_bytes=0))
    if result.returncode:
        record_failure(store,selected,getattr(result,'reason',None) or 'EXPORT_ERROR',result.stderr,metrics,revision)
        failure=RuntimeError('Excel导出失败，详见export_health与本地导出告警')
        failure.recorded_export_failure=True
        raise failure
    # Build every workbook successfully before moving or replacing any existing file.
    publication={}
    for interval in selected:
        current=next(b for b in books if b['interval']==interval and not b['archive'])
        publication[interval]=begin_publication(store,config,interval,LABELS[interval],
            current['report_period_start'],current['report_period_end'],current['generated'])
    outputs=[];period_outputs={i:[] for i in selected};period_pending={i:False for i in selected}
    archive_updates={i:{} for i in selected}
    published={}
    for book in books:
        interval=book['interval'];state=publication[interval]
        temporary=cache/book['filename']
        root=resolved(config['excel_dir'])
        target=archive_target(root,book['filename']) if book['archive'] else resolved(root/book['filename'])
        if not book['archive'] and target.parent!=root: raise ValueError('Excel output escaped its directory')
        held={m['source'] for m in state.get('pending_moves',[])}
        archive_blocked=str(target) in held
        if archive_blocked:
            # A failed archive move must never be followed by overwriting its source.
            target=target.with_name(target.stem+'_本期_'+period_label(book['report_period_start'],
                book['report_period_end'])+'_snapshot_'+str(now_ms())+'.xlsx')
        # Move to the target filesystem before replace (data and excel dirs may use different volumes).
        local=target.with_suffix('.pending.xlsx');shutil.copy2(temporary,local)
        actual,locked=safe_replace(local,target)
        published.setdefault((interval,book['report_period_start']),[]).append(actual)
        if book['archive']:
            update=archive_updates[interval].setdefault(str(book['report_period_start']),
                dict(signature=book['period_signature'],files=[]))
            update['files'].append(actual)
        else:
            outputs.append(actual);period_outputs[interval].append(actual)
        period_pending[interval]|=locked or archive_blocked or bool(state.get('pending_moves'))
        if locked or archive_blocked: store.anomaly('',interval,'EXCEL_LOCKED',Path(actual).name)
    # Merge under the store lock so a 15m refresh retains the other three files.
    with store.lock:
        previous=store.get_meta('last_export',{}) or {}
        periods=dict(previous.get('by_interval',{}))
        for interval in PERIODS:
            if interval not in periods:
                prefix=f'币安USDT合约监控_{LABELS[interval]}'
                files=[p for p in previous.get('files',[]) if Path(p).name.startswith(prefix)]
                if files: periods[interval]=dict(time=previous.get('time'),files=files,pending_replace=previous.get('pending_replace',False))
        stamp=now_ms()
        metrics['duration_ms']=round((time.monotonic()-started)*1000)
        metrics['queue_wait_ms']=queue_wait_ms
        for interval in selected:
            book=next(b for b in books if b['interval']==interval and not b['archive'])
            archive_state=publication[interval]
            retained=archive_state.get('files',[]) if archive_state['current_start']==book['report_period_start'] else []
            archive_state.update(current_start=book['report_period_start'],current_signature=book['period_signature'],
                files=list(dict.fromkeys(retained+period_outputs[interval])))
            archive_state.setdefault('archived',{}).update(archive_updates[interval])
            store.meta('excel_archive_'+interval,archive_state)
            runtime=record_success(store,interval,book,metrics,revision,stamp)
            periods[interval]=dict(time=stamp,snapshot_time=book['generated'],files=period_outputs[interval],
                pending_replace=period_pending[interval],reason=refresh_reason,boundary=book['refresh_boundary'],coverage=book.get('coverage'),
                report_period_start=book['report_period_start'],report_period_end=book['report_period_end'],
                archive_directory=str(Path(config['excel_dir'])/ARCHIVE_DIR),
                pending_archive_moves=len(archive_state.get('pending_moves',[])),
                complete=book['complete'],last_complete_boundary=runtime.get('last_complete_boundary'),
                duration_ms=metrics['duration_ms'],peak_rss_bytes=metrics['peak_rss_bytes'],queue_wait_ms=queue_wait_ms)
        # Record canonical file ownership and only acknowledge the captured DB revision.
        index=ExportIndex(store.db)
        for (interval,start),files in published.items():
            book=next(b for b in books if b['interval']==interval and b['report_period_start']==start)
            index.mark_exported(interval,start,book['export_revision'],dict(files=files,
                row_count=book['period_signal_count'],ids_sha256=book['signal_ids_sha256'],
                snapshot_time=book['generated'],snapshot_revision=book['snapshot_revision'],
                report_period_start=start,report_period_end=book['report_period_end']),stamp)
        store.db.commit()
        state=dict(time=stamp,files=[p for i in PERIODS for p in periods.get(i,{}).get('files',[])],
                   pending_replace=any(p['pending_replace'] for p in periods.values()),by_interval=periods)
        manifest=Path(config['excel_dir'])/'导出清单.json'
        manifest_temp=manifest.with_suffix('.tmp')
        manifest_temp.write_text(dumps(dict(state,generated=stamp,
            note='各周期独立刷新；by_interval记录收盘边界、归属期、完成时间及当前文件；历史文件按触发发现时间分期；时间戳快照不是新增信号')),encoding='utf-8')
        os.replace(manifest_temp,manifest)
        store.meta('last_export',state)
    rowcache=RowCache(Path(config['data_dir'])/'export-cache/prepared-rows.sqlite3')
    try:
        for interval,start in published:
            book=next(b for b in books if b['interval']==interval and b['report_period_start']==start)
            rowcache.successful(interval,start,book['export_revision'],book['period_signal_count'],options['format_version'],stamp)
    finally:rowcache.close()
    # Keep only the most recent successfully built NDJSON generation for diagnostics.
    keep={Path(b['triggers']['ndjson']).parent for b in books if isinstance(b['triggers'],dict)}
    cache_root=resolved(cache)
    for old in cache.glob('rows-*'):
        target=resolved(old)
        if target.parent!=cache_root:raise ValueError('Unexpected export cache directory')
        if old.is_dir() and old not in keep:shutil.rmtree(target)
    from .export_runtime import maintain_history
    maintain_history(store,config,revision,stamp)
    return outputs

async def pause_or_stop(monitor,seconds):
    try: await asyncio.wait_for(monitor.stop.wait(),seconds)
    except asyncio.TimeoutError: pass

async def wait_for_close(monitor,interval,boundary):
    # Poll only around the close; this never refreshes a workbook on its own.
    while not monitor.stop.is_set():
        coverage=await asyncio.to_thread(close_coverage,monitor.store,interval,boundary)
        if coverage['expected'] and not coverage['missing']: return
        if monitor.client.time()>=boundary+CLOSE_WAIT_SECONDS*1000: return
        await pause_or_stop(monitor,CLOSE_POLL_SECONDS)

async def export_period_loop(monitor,interval):
    boundary=next_boundary(monitor.client.time(),interval)
    key='pending_export_batches_'+interval
    pending=set(monitor.store.get_meta(key,[]) if hasattr(monitor.store,'get_meta') else [])
    last_recovery_check=0;recovery_cursor=0
    async def publish(reason,close=None):
        checked=close if close is not None else monitor.client.time()//PERIODS[interval]*PERIODS[interval]
        # Persist before any worker/file mutation so a process kill cannot lose this batch.
        pending.add(checked)
        if hasattr(monitor.store,'meta'):await asyncio.to_thread(monitor.store.meta,key,sorted(pending))
        try:
            outputs=await asyncio.to_thread(export_all,monitor.store,monitor.config,intervals=[interval],
                                    refresh_reason=reason,refresh_boundary=close)
            published=(monitor.store.get_meta('last_export',{}).get('by_interval',{}).get(interval,{})
                if hasattr(monitor.store,'get_meta') else {})
            if published and outputs:
                checked=published['boundary']
                coverage=published['coverage']
            else:
                coverage=await asyncio.to_thread(close_coverage,monitor.store,interval,checked)
            # Only coverage captured in the published workbook can complete a batch.
            # A repair arriving during XLSX writing must schedule one more refresh.
            if coverage['expected'] and not coverage['missing'] and outputs!=[]:pending.discard(checked)
            else:pending.add(checked)
            if hasattr(monitor.store,'meta'):await asyncio.to_thread(monitor.store.meta,key,sorted(pending))
        except Exception as exc:
            pending.add(checked)
            if hasattr(monitor.store,'meta'):await asyncio.to_thread(monitor.store.meta,key,sorted(pending))
            if not getattr(exc,'recorded_export_failure',False):
                monitor.store.anomaly('',interval,'EXPORT_WORKER_ERROR',type(exc).__name__)
    await publish('STARTUP')
    while not monitor.stop.is_set():
        remaining=(boundary-monitor.client.time())/1000
        if remaining>0:
            if pending and monitor.client.time()-last_recovery_check>=settings(monitor.config)['recovery_poll_seconds']*1000:
                choices=sorted(pending);target=choices[recovery_cursor%len(choices)];recovery_cursor+=1
                last_recovery_check=monitor.client.time()
                coverage=await asyncio.to_thread(close_coverage,monitor.store,interval,target)
                if coverage['expected'] and not coverage['missing']:
                    # A repaired batch keeps its original close boundary; exact evaluations
                    # supply that candle even if the current state has advanced.
                    await publish('RECOVERY_COMPLETE',target)
            await pause_or_stop(monitor,min(remaining,1))
            continue
        # After suspension/very slow export, refresh the current close once.
        # Do not label a current snapshot with an obsolete candle boundary.
        latest=monitor.client.time()//PERIODS[interval]*PERIODS[interval]
        if latest>boundary:
            monitor.store.anomaly('',interval,'EXPORT_BOUNDARIES_SKIPPED',
                                  dumps(dict(first=boundary,last=latest-PERIODS[interval])))
            boundary=latest
        await wait_for_close(monitor,interval,boundary)
        if monitor.stop.is_set(): break
        await publish('CANDLE_CLOSE',boundary)
        boundary+=PERIODS[interval]

async def export_loop(monitor):
    # Independent close schedules share one bounded writer to prevent memory overlap.
    await asyncio.gather(*(export_period_loop(monitor,i) for i in PERIODS))
