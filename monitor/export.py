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
from .core import PERIODS, dumps, now_ms

LABELS={'15m':'15分钟','1h':'1小时','4h':'4小时','1d':'日线'}
SOURCE='https://fapi.binance.com/fapi/v1/klines'

def safe_replace(temp,target):
    try:
        os.replace(temp,target)
        return str(target),False
    except PermissionError:
        alternate=target.with_name(target.stem+'_snapshot_'+str(now_ms())+'.xlsx')
        os.replace(temp,alternate)
        return str(alternate),True

def flatten(p):
    b=p['bar'];m=p['metrics']
    row={**b,**m,'symbol':p['symbol'],'interval':p['interval'],'contract_type':p['contract_type'],
         'EMA12':p['ema'][0],'EMA50':p['ema'][1],'EMA200':p['ema'][2],
         'readiness':p['readiness'],'seed':p['seed'],'warmup':p['count'],
         'strategy_version':p['strategy_version'],'discovered':p['discovered'],
         'event_time':p.get('event_time'),'processed':p['processed'],'close_boundary':b['T']+1}
    row['received']=p.get('received')
    row.update({f'check{i+1}':bool(v) for i,v in enumerate(m['checks'])})
    row.update({k:p.get(k) for k in ('signal_id','event_type','origin','latency_ms')})
    row['thresholds']=dumps(m.get('thresholds',{}))
    return row

def previous_rows(p):
    output=[];prev=p.get('previous20_before_close')
    for n,b in enumerate(p['previous20'],1):
        row={**b,'signal_id':p['signal_id'],'symbol':p['symbol'],'interval':p['interval'],'index':n,
             'abs_body':abs(b['c']-b['o']),'relative_body':abs(b['c']/b['o']-1),
             'bar_return':b['c']/b['o']-1,'close_return':b['c']/prev-1 if prev else None,'close_boundary':b['T']+1}
        output.append(row);prev=b['c']
    return output

def partitions(signals,cap):
    groups=collections.defaultdict(list)
    # Limit signal chunks by the larger 20-row-per-signal comparison table.
    capacity=max(1,cap//20)
    for p in signals:
        month=dt.datetime.fromtimestamp(p['bar']['t']/1000,dt.timezone.utc).strftime('%Y-%m')
        groups[month].append(p)
    result=[]
    for month,rows in sorted(groups.items()):
        for offset in range(0,len(rows),capacity): result.append((month,offset//capacity+1,rows[offset:offset+capacity]))
    return result

def snapshot(store,config):
    with store.lock:
        # A single read transaction fixes a consistent view across all four reports.
        store.db.execute('BEGIN')
        try:
            uni=store.universe()
            states={(r['symbol'],r['interval']):json.loads(r['state']) for r in store.db.execute('SELECT * FROM states')}
            signals=collections.defaultdict(list)
            for r in store.db.execute('SELECT s.payload,o.status,o.sent,o.attempts FROM signals s LEFT JOIN outbox o ON o.id=s.id ORDER BY s.t,s.symbol'):
                p=json.loads(r['payload']);p['notification_status']=r['status'] or 'NOT_REQUESTED'
                p['sent']=r['sent'];p['attempts']=r['attempts'] or 0
                signals[p['interval']].append(p)
            errors=[dict(r) for r in store.db.execute('SELECT * FROM anomalies ORDER BY time,id')]
            versions=[dict(r) for r in store.db.execute('SELECT * FROM versions ORDER BY created')]
            exclusions=[dict(r) for r in store.db.execute('SELECT symbol,reason,updated AS time FROM universe WHERE active=0 ORDER BY symbol')]
        finally: store.db.rollback()
    stamp=now_ms();books=[]
    for interval in PERIODS:
        overview=[];status=[]
        for symbol,info in uni.items():
            s=states.get((symbol,interval),{})
            p=s.get('latest')
            row=flatten(p) if p else dict(symbol=symbol,interval=interval,contract_type=info.get('contractType'),readiness='NOT_READY',trigger=False)
            row.update(health=s.get('health','INITIALIZING'),warmup=s.get('count',0),
                       detail=s.get('detail',''),updated=s.get('updated'),price_precision=info.get('pricePrecision',8))
            row['stale']=s.get('last',-1)<stamp//PERIODS[interval]*PERIODS[interval]-PERIODS[interval]
            overview.append(row)
            status.append(dict(time=s.get('updated'),symbol=symbol,interval=interval,code=s.get('readiness','NOT_READY'),
                               detail=f"health={s.get('health','INITIALIZING')}; warmup={s.get('count',0)}; {s.get('detail','')}"))
        status += [e for e in errors if e['interval'] in ('',interval)]
        status += [dict(e,interval=interval,code='EXCLUDED') for e in exclusions]
        parts=partitions(signals[interval],config['excel_max_data_rows'])
        # Main workbook contains the newest signal chunk; older chunks are separate monthly archives.
        main=parts[-1][2] if parts else []
        chunks=[('',main)]+[(f'_{month}_卷{number:03}',rows) for month,number,rows in parts[:-1]]
        status_chunks=[status[x:x+config['excel_max_data_rows']] for x in range(0,len(status),config['excel_max_data_rows'])] or [[]]
        for suffix,group in chunks:
            trigger=[];previous=[]
            for p in group:
                r=flatten(p);r.update(notification_status=p['notification_status'],sent=p['sent'],attempts=p['attempts'],
                                      sending_latency_ms=p['sent']-(p['bar']['T']+1) if p['sent'] else None)
                trigger.append(r);previous.extend(previous_rows(p))
            parameters=[{'key':'策略版本','value':store.version},{'key':'数据来源','value':SOURCE},
                        {'key':'生成时间UTC毫秒','value':stamp},
                        {'key':'时间口径','value':'北京时间显示；UTC毫秒保留；API收盘时间=周期结束边界−1ms；日线北京08:00结束'},
                        {'key':'EMA公式','value':'alpha=2/(N+1); EMA=alpha*C+(1-alpha)*上次EMA；首根收盘价种子；仅12/50/200'},
                        {'key':'前20根窗口','value':'不含目标K线；各周期独立；排名为降序竞争排名，相同值并列'},
                        {'key':'字段单位','value':'价格USDT；v基础币数量；q USDT成交额；收益率为小数；延迟毫秒；量比/实体比为倍数'},
                        {'key':'计算口径','value':'统计由版本化监控引擎计算并持久化，Excel为只读快照；修改Excel不会改变监控参数'},
                        {'key':'实体公式','value':'abs_body=abs(C-O); relative_body=abs(C/O-1); bar_return=C/O-1; close_return=C/前收盘-1'},
                        {'key':'量价比较','value':'volume_ratio=V/前20均量; relative_body_ratio=relative_body/前20相对实体均值; body_share=abs(C-O)/(H-L)'},
                        {'key':'形态与突破','value':'upper_wick_share=(H-max(O,C))/(H-L); breakout=C/前20最高价-1；零分母为空且不触发'},
                        {'key':'提醒区别','value':'HISTORICAL初始化；RECOVERED补数；REALTIME实时；EXPIRED发现超过120秒。NOT_REQUESTED未要求发送'},
                        {'key':'历史配置','value':dumps({'historical_analysis':config['historical_analysis'],'historical_summary':config['historical_summary']})},
                        {'key':'归档说明','value':f'全部记录保留数据库；本文件{len(group)}条信号；较早信号按UTC月份分卷，数据容量上限{config["excel_max_data_rows"]}行'},
                        {'key':'注意','value':'仅监控条件触发，不代表实际交易或盈利预测；快照中的stale反映已收盘K线缺口。'},
                        {'key':'配置','value':dumps(config)}]
            parameters += [{'key':f'历史策略 {v["version"]}', 'value':v['config']} for v in versions]
            books.append(dict(filename=f'币安USDT合约监控_{LABELS[interval]}{suffix}.xlsx',interval=interval,generated=stamp,
                              overview=overview,triggers=trigger,previous=previous,status=status_chunks[-1],parameters=parameters))
        for index,chunk in enumerate(status_chunks[:-1],1):
            books.append(dict(filename=f'币安USDT合约监控_{LABELS[interval]}_异常归档_卷{index:03}.xlsx',interval=interval,
                              generated=stamp,overview=[],triggers=[],previous=[],status=chunk,parameters=parameters))
    return books

def export_all(store,config,render=False):
    cache=Path(config['data_dir'])/'export-cache';cache.mkdir(exist_ok=True)
    books=snapshot(store,config)
    spec=cache/'snapshot.json';spec.write_text(dumps({'books':books,'render':render}),encoding='utf-8')
    command=[config['node_executable'],str(Path(__file__).parent.parent/'excel_builder.mjs'),str(spec),str(cache)]
    result=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',timeout=600)
    if result.returncode:
        store.anomaly('','','EXPORT_ERROR',result.stderr[-1500:]);raise RuntimeError('Excel导出失败，详见数据库异常记录')
    outputs=[];pending=False
    for book in books:
        temporary=cache/book['filename'];target=Path(config['excel_dir'])/book['filename']
        # Move to the target filesystem before replace (data and excel dirs may use different volumes).
        local=target.with_suffix('.pending.xlsx');shutil.copy2(temporary,local)
        actual,locked=safe_replace(local,target);pending|=locked;outputs.append(actual)
        if locked: store.anomaly('',book['interval'],'EXCEL_LOCKED',Path(actual).name)
    manifest=Path(config['excel_dir'])/'导出清单.json'
    manifest_temp=manifest.with_suffix('.tmp')
    manifest_temp.write_text(dumps({'generated':now_ms(),'files':outputs,'pending_replace':pending,
                                   'note':'以本次清单文件集合为准；其他时间戳快照不是新增信号'}),encoding='utf-8')
    os.replace(manifest_temp,manifest)
    store.meta('last_export',dict(time=now_ms(),files=outputs,pending_replace=pending))
    return outputs

async def export_loop(monitor):
    while not monitor.stop.is_set():
        try: await asyncio.to_thread(export_all,monitor.store,monitor.config)
        except Exception as exc: monitor.store.anomaly('','','EXPORT_WORKER_ERROR',type(exc).__name__)
        try: await asyncio.wait_for(monitor.stop.wait(),monitor.config['export_seconds'])
        except asyncio.TimeoutError: pass
