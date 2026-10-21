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
    """Only a committed, healthy state proves this close has been processed."""
    with store.lock:
        rows=store.db.execute('''SELECT u.symbol,json_extract(s.state,'$.last') AS last,
            json_extract(s.state,'$.latest.bar.t') AS evaluated,
            json_extract(s.state,'$.health') AS health FROM universe u
            LEFT JOIN states s ON s.symbol=u.symbol AND s.interval=? WHERE u.active=1
            AND json_extract(u.info,'$.contractType')='PERPETUAL' ''',(interval,)).fetchall()
    missing=[r['symbol'] for r in rows if r['last'] is None or
             r['last']<boundary-PERIODS[interval] or r['evaluated'] is None or
             r['evaluated']<boundary-PERIODS[interval] or r['health']!='OK']
    return dict(expected=len(rows),completed=len(rows)-len(missing),missing=missing)

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

def snapshot(store,config,intervals=None,refresh_reason='MANUAL',refresh_boundary=None):
    intervals=list(PERIODS) if intervals is None else list(intervals)
    if not intervals or any(i not in PERIODS for i in intervals): raise ValueError('Invalid export intervals')
    placeholders=','.join('?' for _ in intervals)
    with store.lock:
        # Each requested period is read after its close has been processed.
        store.db.execute('BEGIN')
        try:
            uni=store.universe()
            states={(r['symbol'],r['interval']):json.loads(r['state']) for r in store.db.execute(
                f'SELECT * FROM states WHERE interval IN ({placeholders})',intervals)}
            signals=collections.defaultdict(list)
            for r in store.db.execute(f'''SELECT s.payload,o.status,o.sent,o.attempts FROM signals s
                    LEFT JOIN outbox o ON o.id=s.id WHERE s.interval IN ({placeholders}) ORDER BY s.t,s.symbol''',intervals):
                p=json.loads(r['payload']);p['notification_status']=r['status'] or 'NOT_REQUESTED'
                p['sent']=r['sent'];p['attempts']=r['attempts'] or 0
                signals[p['interval']].append(p)
            errors=[dict(r) for r in store.db.execute('SELECT * FROM anomalies ORDER BY time,id')]
            versions=[dict(r) for r in store.db.execute('SELECT * FROM versions ORDER BY created')]
            exclusions=[dict(r) for r in store.db.execute('SELECT symbol,reason,updated AS time FROM universe WHERE active=0 ORDER BY symbol')]
            archive_states={i:store.get_meta('excel_archive_'+i) for i in intervals}
        finally: store.db.rollback()
    stamp=now_ms();books=[]
    for interval in intervals:
        overview=[];status=[]
        boundary=refresh_boundary if refresh_boundary is not None else stamp//PERIODS[interval]*PERIODS[interval]
        missing=[]
        for symbol,info in uni.items():
            s=states.get((symbol,interval),{})
            p=s.get('latest')
            row=flatten(p) if p else dict(symbol=symbol,interval=interval,contract_type=info.get('contractType'),
                                          readiness='NOT_READY',trigger=False,comparison_window=WINDOW)
            row.update(health=s.get('health','INITIALIZING'),warmup=s.get('count',0),
                       detail=s.get('detail',''),updated=s.get('updated'),price_precision=info.get('pricePrecision',8))
            row['stale']=((s.get('last') if s.get('last') is not None else -1)<boundary-PERIODS[interval] or
                          not p or p['bar']['t']<boundary-PERIODS[interval])
            if row['stale'] or s.get('health')!='OK': missing.append(symbol)
            overview.append(row)
            status.append(dict(time=s.get('updated'),symbol=symbol,interval=interval,code=s.get('readiness','NOT_READY'),
                               detail=f"health={s.get('health','INITIALIZING')}; warmup={s.get('count',0)}; {s.get('detail','')}"))
        status += [e for e in errors if e['interval'] in ('',interval)]
        status += [dict(e,interval=interval,code='EXCLUDED') for e in exclusions]
        if refresh_reason=='CANDLE_CLOSE' and missing:
            status += [dict(time=stamp,symbol=symbol,interval=interval,code='CLOSE_NOT_PROCESSED',
                detail=f'本次收盘边界UTC毫秒={boundary}；收盘数据尚未完整处理；本行旧数据不能作为该次收盘的判断结果') for symbol in missing]
        current_start,current_end=report_period(interval,stamp)
        archive_state=archive_states[interval] or {}
        first_start=archive_state.get('first_start',current_start)
        groups=collections.defaultdict(list)
        groups[current_start]=[]
        old_start=archive_state.get('current_start',current_start)
        if old_start<current_start: groups[old_start]=[]
        for signal in signals[interval]:
            start,_=report_period(interval,report_timestamp(signal))
            if first_start<=start<=current_start: groups[start].append(signal)
        chunks=[]
        for start in [current_start]+sorted(s for s in groups if s!=current_start):
            _,end=report_period(interval,start)
            archived=start!=current_start
            signature=fingerprint(groups[start])
            saved=archive_state.get('archived',{}).get(str(start),{})
            if (archived and start==old_start and archive_state.get('current_signature')==signature
                    and archive_state.get('files') and all(Path(p).is_file() for p in archive_state['files'])):
                # Preserve the completed previous workbook exactly when no late record/status changed.
                continue
            if archived and saved.get('signature')==signature and saved.get('files') and all(Path(p).is_file() for p in saved['files']):
                continue
            parts=split_rows(groups[start],config['excel_max_data_rows'])
            for number,rows in enumerate(parts,1):
                suffix=('_'+period_label(start,end)) if archived else ''
                if len(parts)>1 and number<len(parts): suffix+=f'_卷{number:03}'
                chunks.append((suffix,rows,start,end,archived,signature))
        status_chunks=[status[x:x+config['excel_max_data_rows']] for x in range(0,len(status),config['excel_max_data_rows'])] or [[]]
        rules=rules_for_interval(config,interval)
        for suffix,group,report_start,report_end,archived,signature in chunks:
            trigger=[];previous=[]
            for p in group:
                r=flatten(p);r.update(notification_status=p['notification_status'],sent=p['sent'],attempts=p['attempts'],
                                      sending_latency_ms=p['sent']-(p['bar']['T']+1) if p['sent'] else None)
                trigger.append(r);previous.extend(previous_rows(p))
            parameters=[{'key':'策略版本','value':store.version},{'key':'数据来源','value':SOURCE},
                        {'key':'生成时间UTC毫秒','value':stamp},
                        {'key':'刷新原因','value':refresh_reason},
                        {'key':'本次对应收盘UTC毫秒','value':boundary},
                        {'key':'本次收盘覆盖','value':f'已处理{len(uni)-len(missing)}/{len(uni)}；缺数/异常{len(missing)}'},
                        {'key':'自动刷新规则','value':'启动时各周期一次；之后仅本周期收盘判断后刷新一次。收盘后最多等待30秒汇齐，缺数明确标记；文件写入另需时间。'},
                        {'key':'触发时间口径','value':'触发发现时间=程序判断并发现信号的时间；触发K线收盘时间=API收盘UTC毫秒+1；两者不等同，均以北京时间显示。'},
                        {'key':'时间口径','value':'北京时间显示；UTC毫秒保留；API收盘时间=周期结束边界−1ms；日线北京08:00结束'},
                        {'key':'EMA公式','value':'alpha=2/(N+1); EMA=alpha*C+(1-alpha)*上次EMA；首根收盘价种子；仅12/50/200'},
                        {'key':'比较窗口','value':'当前策略为此前15根已收盘K线，不含目标K线；15分钟/1小时/4小时/日线各自独立。旧策略记录仍为20根，以各行“比较窗口根数”为准；排名包含目标K线，相同值并列。'},
                        {'key':'监控范围','value':'仅USDT保证金、USDT报价且正在交易的永续合约；旧策略交割合约历史记录保留，不再新增监控。'},
                        {'key':'EMA触发条件','value':'当前策略仅要求收盘价高于EMA12、EMA50和EMA200；不要求多头排列或EMA12上升。“EMA多头排列”仅展示客观状态。'},
                        {'key':'放量与红字','value':f'当前{LABELS[interval]}放量条件：V>={rules["volume_multiple"]:g}×V15_mean 且 V>V15_max；V>={rules["volume_highlight_multiple"]:g}×V15_mean 且 V>V15_max整行红字。旧记录保留其原策略判定和标红口径；旧20根统计不按15根口径标红。'},
                        {'key':'字段单位','value':'价格USDT；v基础币数量；q USDT成交额；收益率为小数；延迟毫秒；量比/实体比为倍数'},
                        {'key':'计算口径','value':'统计由版本化监控引擎计算并持久化，Excel为只读快照；修改Excel不会改变监控参数'},
                        {'key':'实体公式','value':'abs_body=abs(C-O); relative_body=abs(C/O-1); bar_return=C/O-1; close_return=C/前收盘-1'},
                        {'key':'量价比较','value':'volume_ratio=V/比较均量; relative_body_ratio=relative_body/比较相对实体均值；比较窗口根数随原策略保留，不重算历史记录。'},
                        {'key':'形态与突破','value':'当前分母=abs(C-O)+H-max(O,C)，不含下影线；body_share=abs(C-O)/分母>=60%，upper_wick_share=(H-max(O,C))/分母<=30%；breakout=C/比较最高价-1；零分母为空且不触发。'},
                        {'key':'旧策略记录','value':'旧20根记录的实体/上影占比仍以H-L为分母，EMA要求原多头排列及EMA12上升，放量阈值为原配置；原信号与统计不重算，详见策略版本及阈值。'},
                        {'key':'提醒区别','value':'HISTORICAL初始化；RECOVERED补数；REALTIME实时；EXPIRED发现超过120秒。NOT_REQUESTED未要求发送'},
                        {'key':'历史配置','value':dumps({'historical_analysis':config['historical_analysis'],'historical_summary':config['historical_summary']})},
                        {'key':'记录归属期 北京','value':period_label(report_start,report_end)+'；含起始，不含结束'},
                        {'key':'归属时间口径','value':'按触发发现时间discovered分期，08:00及以后发现的提醒进入新期；K线开收盘时间与事件类型保持原值。'},
                        {'key':'归档说明','value':f'全部记录保留数据库；本文件{len(group)}条信号；15分钟每日、1小时每周一、4小时以2026-10-19为起点每两周周一、日线每月1日，均北京时间08:00换期；历史文件在{ARCHIVE_DIR}；容量上限{config["excel_max_data_rows"]}行，超限按期内分卷。'},
                        {'key':'注意','value':'仅监控条件触发，不代表实际交易或盈利预测；快照中的stale反映已收盘K线缺口。'},
                        {'key':'配置','value':dumps(config)}]
            parameters += [{'key':f'历史策略 {v["version"]}', 'value':v['config']} for v in versions]
            books.append(dict(filename=f'币安USDT合约监控_{LABELS[interval]}{suffix}.xlsx',interval=interval,generated=stamp,
                              comparison_window=WINDOW,
                              archive=archived,report_period_start=report_start,report_period_end=report_end,
                              period_signature=signature,
                              refresh_reason=refresh_reason,refresh_boundary=boundary,
                              coverage=dict(expected=len(uni),completed=len(uni)-len(missing),missing=missing),
                              overview=[] if archived else overview,triggers=trigger,previous=previous,
                              status=[] if archived else status_chunks[-1],parameters=parameters))
        current_parameters=next(b['parameters'] for b in books if b['interval']==interval and not b['archive'])
        for index,chunk in enumerate(status_chunks[:-1],1):
            books.append(dict(filename=f'币安USDT合约监控_{LABELS[interval]}_异常归档_卷{index:03}.xlsx',interval=interval,
                              generated=stamp,comparison_window=WINDOW,refresh_reason=refresh_reason,refresh_boundary=boundary,
                              archive=False,report_period_start=current_start,report_period_end=current_end,
                              period_signature=fingerprint(groups[current_start]),
                              overview=[],triggers=[],previous=[],status=chunk,parameters=current_parameters))
    return books

def export_all(store,config,render=False,intervals=None,refresh_reason='MANUAL',refresh_boundary=None):
    selected=list(PERIODS) if intervals is None else list(intervals)
    # Independent period workers must never overwrite one another's input/cache.
    cache=Path(config['data_dir'])/'export-cache'
    if intervals is not None: cache=cache/'-'.join(selected)
    cache.mkdir(parents=True,exist_ok=True)
    books=snapshot(store,config,selected,refresh_reason,refresh_boundary)
    spec=cache/'snapshot.json';spec.write_text(dumps({'books':books,'render':render}),encoding='utf-8')
    command=[config['node_executable'],str(Path(__file__).parent.parent/'excel_builder.mjs'),str(spec),str(cache)]
    result=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',timeout=600)
    if result.returncode:
        store.anomaly('','','EXPORT_ERROR',result.stderr[-1500:]);raise RuntimeError('Excel导出失败，详见数据库异常记录')
    # Build every workbook successfully before moving or replacing any existing file.
    publication={}
    for interval in selected:
        current=next(b for b in books if b['interval']==interval and not b['archive'])
        publication[interval]=begin_publication(store,config,interval,LABELS[interval],
            current['report_period_start'],current['report_period_end'],current['generated'])
    outputs=[];period_outputs={i:[] for i in selected};period_pending={i:False for i in selected}
    archive_updates={i:{} for i in selected}
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
        for interval in selected:
            book=next(b for b in books if b['interval']==interval and not b['archive'])
            archive_state=publication[interval]
            retained=archive_state.get('files',[]) if archive_state['current_start']==book['report_period_start'] else []
            archive_state.update(current_start=book['report_period_start'],current_signature=book['period_signature'],
                files=list(dict.fromkeys(retained+period_outputs[interval])))
            archive_state.setdefault('archived',{}).update(archive_updates[interval])
            store.meta('excel_archive_'+interval,archive_state)
            periods[interval]=dict(time=stamp,snapshot_time=book['generated'],files=period_outputs[interval],
                pending_replace=period_pending[interval],reason=refresh_reason,boundary=book['refresh_boundary'],coverage=book.get('coverage'),
                report_period_start=book['report_period_start'],report_period_end=book['report_period_end'],
                archive_directory=str(Path(config['excel_dir'])/ARCHIVE_DIR),
                pending_archive_moves=len(archive_state.get('pending_moves',[])))
        state=dict(time=stamp,files=[p for i in PERIODS for p in periods.get(i,{}).get('files',[])],
                   pending_replace=any(p['pending_replace'] for p in periods.values()),by_interval=periods)
        manifest=Path(config['excel_dir'])/'导出清单.json'
        manifest_temp=manifest.with_suffix('.tmp')
        manifest_temp.write_text(dumps(dict(state,generated=stamp,
            note='各周期独立刷新；by_interval记录收盘边界、归属期、完成时间及当前文件；历史文件按触发发现时间分期；时间戳快照不是新增信号')),encoding='utf-8')
        os.replace(manifest_temp,manifest)
        store.meta('last_export',state)
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
    async def publish(reason,close=None):
        try:
            await asyncio.to_thread(export_all,monitor.store,monitor.config,intervals=[interval],
                                    refresh_reason=reason,refresh_boundary=close)
        except Exception as exc:
            monitor.store.anomaly('',interval,'EXPORT_WORKER_ERROR',type(exc).__name__)
    await publish('STARTUP')
    while not monitor.stop.is_set():
        remaining=(boundary-monitor.client.time())/1000
        if remaining>0:
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
    # Four independent schedules: coincident closes do not wait for other files.
    await asyncio.gather(*(export_period_loop(monitor,i) for i in PERIODS))
