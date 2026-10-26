"""Indexed, snapshot-consistent report inputs with a disk-backed row cache."""
import hashlib
import json
import sqlite3
import uuid
from pathlib import Path
from .core import PERIODS, WINDOW, eligible, rules_for_interval, dumps
from .archive import report_period, period_label
from .export_index import ExportIndex
from .export_runtime import settings
from .row_cache import RowCache
from .report_parameters import parameter_rows, LABELS
from .close_applicability import close_applicable, load_exact_evaluations, close_processed


class RowSink:
    def __init__(self,directory,name):
        self.rows=[] if directory is None else None
        self.path=None if directory is None else directory/name
        self.handle=None if self.path is None else self.path.open('w',encoding='utf-8',newline='\n')
        self.count=0

    def add(self,row):
        if self.handle:self.handle.write(dumps(row)+'\n')
        else:self.rows.append(row)
        self.count+=1

    def finish(self):
        if self.handle:self.handle.close()
        return self.rows if self.path is None else dict(ndjson=str(self.path),count=self.count)


def indexed_snapshot(store,config,intervals,reason,boundary,stamp,stream_dir=None,backfill=False):
    from .export import flatten, previous_rows
    options=settings(config); generation=uuid.uuid4().hex
    directory=Path(stream_dir)/('rows-'+generation) if stream_dir is not None else None
    if directory:directory.mkdir(parents=True)
    cache=RowCache(Path(config['data_dir'])/'export-cache/prepared-rows.sqlite3')
    with store.lock:
        path=store.db.execute('PRAGMA database_list').fetchone()[2]
    db=sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True,timeout=30)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA cache_size=-4096');db.execute('BEGIN')
    revision=db.execute('SELECT revision FROM export_revision_clock WHERE id=1').fetchone()[0]
    index=ExportIndex(db);books=[];sinks=[]
    try:
        universe={r['symbol']:info for r in db.execute('SELECT symbol,info FROM universe WHERE active=1 ORDER BY symbol')
            if eligible(info:=json.loads(r['info']))}
        versions=[dict(r) for r in db.execute('SELECT * FROM versions ORDER BY created')]
        for interval in intervals:
            close=boundary if boundary is not None else stamp//PERIODS[interval]*PERIODS[interval]
            states={r['symbol']:r['state'] for r in db.execute('''SELECT s.symbol,s.state FROM states s
                JOIN universe u ON u.symbol=s.symbol WHERE s.interval=? AND u.active=1''',(interval,))}
            target=close-PERIODS[interval]
            evaluations=load_exact_evaluations(db,interval,target)
            overview=[];status=[];missing=[];not_applicable=[];current_warnings=[]
            for symbol,info in universe.items():
                state=json.loads(states.get(symbol,'{}'));latest=state.get('latest')
                applicable=close_applicable(info,close)
                if not applicable:not_applicable.append(symbol)
                close_note=('' if applicable else
                    f"该收盘不适用：上市时间UTC毫秒={info.get('onboardDate')}，不早于对应收盘UTC毫秒={close}；")
                p=(evaluations.get(symbol) or latest) if applicable else None
                processed=close_processed(state.get('last'),(latest or {}).get('bar',{}).get('t'),
                    state.get('health'),target,symbol in evaluations)
                if applicable and state.get('health')!='OK':current_warnings.append(symbol)
                applicability=dict(close=close,onboardDate=info.get('onboardDate'),applicable=applicable,
                    exact_proof=symbol in evaluations,processed=processed)
                source_hash=hashlib.sha256((states.get(symbol,'{}')+dumps(p)+dumps(applicability)).encode()).hexdigest()
                key=interval+':'+symbol
                saved=cache.get('state',key,source_hash,options['format_version'])
                if saved:row=json.loads(saved['value'])
                else:
                    row=flatten(p) if p else dict(symbol=symbol,interval=interval,contract_type=info.get('contractType'),
                        readiness='NOT_READY',trigger=False,comparison_window=WINDOW)
                    row.update(health=state.get('health','INITIALIZING') if applicable else 'NOT_APPLICABLE',
                        warmup=state.get('count',0),detail=close_note+state.get('detail',''),
                        updated=state.get('updated'),price_precision=info.get('pricePrecision',8),close_applicable=applicable)
                    row['stale']=applicable and not processed
                    cache.put('state',key,source_hash,interval,0,row,[],state.get('updated'),options['format_version'],generation)
                incomplete=applicable and not processed
                if incomplete:missing.append(symbol)
                overview.append(row)
                status.append(dict(time=state.get('updated'),symbol=symbol,interval=interval,
                    code=('NOT_APPLICABLE' if not applicable else
                          'CLOSE_NOT_PROCESSED' if incomplete else
                          'CURRENT_STATE_WARNING' if state.get('health')!='OK' else state.get('readiness','NOT_READY')),
                    detail=f"health={state.get('health','INITIALIZING')}; warmup={state.get('count',0)}; "
                        f"对应收盘UTC毫秒={close}；"+close_note+('不完整；' if incomplete else '')+
                        ('该收盘已有判定；当前状态异常仍需处理；' if applicable and processed and state.get('health')!='OK' else '')+state.get('detail','')))
            expected=len(universe)-len(not_applicable)
            coverage=dict(expected=expected,completed=expected-len(missing),missing=missing,
                          total_symbols=len(universe),not_applicable=not_applicable,current_warnings=current_warnings)
            recent=index.recent_anomalies(stamp,interval,options['recent_anomaly_hours'],options['recent_anomaly_groups'])
            plans=index.plan(interval,stamp)
            if not backfill:plans=plans[:1+int(options['historical_periods_per_refresh'])]
            for plan in plans:
                start,end=plan['start'],plan['end'];count=plan['signal_count']
                digest=hashlib.sha256();seen=0
                for header in index.iter_signal_headers(interval,start,end,int(options['page_rows'])):
                    digest.update(dumps([header['id'],header['discovered']]).encode('utf-8')+b'\n');seen+=1
                    saved=cache.get('signal',header['id'],header['revision'],options['format_version'])
                    if saved:cache.touch('signal',header['id'],generation)
                    else:
                        payload=index.read_signal(header['id'])
                        row=flatten(payload)
                        row.update(notification_status=payload['notification_status'],sent=payload['sent'],attempts=payload['attempts'],
                            sending_latency_ms=payload['sent']-(payload['bar']['T']+1) if payload['sent'] else None)
                        cache.put('signal',header['id'],header['revision'],interval,start,row,previous_rows(payload),
                            header['updated_ms'],options['format_version'],generation)
                if seen!=count:raise RuntimeError('Snapshot signal count mismatch')
                cache.db.commit()
                chunks=[];chunk=None
                def new_chunk(number):
                    prefix=f'{interval}-{start}-{number}'
                    triggers=RowSink(directory,prefix+'-triggers.ndjson');sinks.append(triggers)
                    previous=RowSink(directory,prefix+'-previous.ndjson');sinks.append(previous)
                    return dict(triggers=triggers,previous=previous,number=number)
                for cached in cache.period_rows(interval,start,generation):
                    if chunk and (chunk['triggers'].count>=config['excel_max_data_rows'] or
                            chunk['previous'].count+cached['comparison_count']>config['excel_max_data_rows']):
                        chunk['triggers_data']=chunk['triggers'].finish();chunk['previous_data']=chunk['previous'].finish()
                        chunks.append(chunk);chunk=None
                    if chunk is None:chunk=new_chunk(len(chunks)+1)
                    chunk['triggers'].add(json.loads(cached['value']))
                    for previous in json.loads(cached['comparisons']):chunk['previous'].add(previous)
                if chunk is None:chunk=new_chunk(len(chunks)+1)
                chunk['triggers_data']=chunk['triggers'].finish();chunk['previous_data']=chunk['previous'].finish();chunks.append(chunk)
                for number,chunk in enumerate(chunks,1):
                    archived=not plan['current'];suffix='_'+period_label(start,end) if archived else ''
                    if number<len(chunks):suffix+=f'_卷{number:03}'
                    parameters=parameter_rows(store,config,interval,stamp,close,universe,missing,rules_for_interval(config,interval),
                        start,end,chunk['triggers'].count,versions,reason,not_applicable=not_applicable,current_warnings=current_warnings)
                    description=f"最近{recent['hours']:g}小时；UTC毫秒范围[{recent['start']},{recent['end']}]；最多{recent['limit']}组；"
                    description+=f"显示{recent['group_count']}组，范围内原始异常{recent['raw_count']}条；"+('已截断，完整记录已保留' if recent['truncated'] else '未截断')
                    parameters.extend([dict(key='近期异常范围',value=description),dict(key='数据完整性',
                        value=('完整' if expected and not missing else '不完整')+
                            f'；已处理{expected-len(missing)}/{expected}；全部标的{len(universe)}；该收盘不适用{len(not_applicable)}；当前状态异常{len(current_warnings)}'),
                        dict(key='增量与分页',value=f'数据库保留完整历史；信号按发现时间索引分页{options["page_rows"]}条；历史期未变更直接跳过；磁盘行缓存复用；追加新信号保持既有行序。')])
                    books.append(dict(filename=f'币安USDT合约监控_{LABELS[interval]}{suffix}.xlsx',interval=interval,
                        generated=stamp,comparison_window=WINDOW,archive=archived,report_period_start=start,report_period_end=end,
                        period_signature=str(plan['revision']),export_revision=plan['revision'],snapshot_revision=revision,
                        period_signal_count=count,signal_ids_sha256=digest.hexdigest(),refresh_reason=reason,refresh_boundary=close,
                        coverage=coverage,
                        complete=bool(expected) and not missing,overview=[] if archived else overview,
                        triggers=chunk['triggers_data'],previous=chunk['previous_data'],status=[] if archived else status,
                        recent_anomalies=[] if archived else recent['groups'],recent_anomalies_description=description,
                        parameters=parameters,row_count=chunk['triggers'].count,
                        row_cache=dict(hits=cache.hits,misses=cache.misses,format_version=options['format_version'])))
        return books
    finally:
        for sink in sinks:
            if sink.handle:sink.handle.close()
        db.rollback();db.close();cache.close()
