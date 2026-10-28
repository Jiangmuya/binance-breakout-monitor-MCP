import asyncio
import json
import logging
import logging.handlers
import os
import signal
import math
import time
from pathlib import Path
from .core import Store, PERIODS, RULE_KEYS, now_ms, dumps, validate_volume_rules
from .transport import PublicClient
from .collection import CollectionWindows

def load_config(path):
    path=Path(path).resolve()
    config=json.loads(path.read_text(encoding='utf-8-sig'))
    config['node_executable']=os.getenv('MONITOR_NODE',config['node_executable'])
    for name in ('data_dir','excel_dir'):
        config[name]=str((path.parent/config[name]).resolve())
        Path(config[name]).mkdir(parents=True,exist_ok=True)
    if config['intervals']!=list(PERIODS): raise ValueError('必须保留四个独立周期')
    if config['warmup']!=1500: raise ValueError('本版本预热固定1500，防止改变EMA种子定义')
    if not 1<=config['streams_per_connection']<=1024: raise ValueError('订阅连接容量无效')
    if not 1<=config['rest_concurrency']<=20: raise ValueError('REST并发必须1..20')
    if not 10<=config['rest_weight_per_minute']<=1800: raise ValueError('权重预算必须10..1800')
    if not 20<=config['excel_max_data_rows']<=1000000: raise ValueError('Excel容量必须20..1000000')
    for k in RULE_KEYS:
        if not isinstance(config[k],(int,float)) or not math.isfinite(config[k]) or config[k]<0: raise ValueError('无效策略参数')
    validate_volume_rules(config)
    if not 0<=config['min_body_share']<=1 or not 0<=config['max_upper_wick_share']<=1: raise ValueError('形态占比必须0..1')
    if config['historical_summary'] and not config['historical_analysis']: raise ValueError('历史摘要须同时启用历史分析')
    return config

def logger(config):
    log=logging.getLogger('monitor.'+config['data_dir'])
    log.setLevel(logging.INFO)
    log.propagate=False
    if not log.handlers:
        handler=logging.handlers.RotatingFileHandler(Path(config['data_dir'])/'monitor.log',
                 maxBytes=config['log_max_bytes'],backupCount=config['log_backup_count'],encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(message)s'))
        log.addHandler(handler)
    return log

class InstanceLock:
    def __init__(self,directory): self.path=Path(directory)/'instance.lock'
    def __enter__(self):
        self.file=self.path.open('a+b');self.file.seek(0);self.file.write(b'0');self.file.flush();self.file.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            self.file.close();raise RuntimeError('此数据目录已有运行实例')
        return self
    def __exit__(self,*args): self.file.close()

class Monitor:
    def __init__(self,config):
        self.config=config
        self.store=Store(Path(config['data_dir'])/'monitor.sqlite3',config)
        self.log=logger(config)
        self.stop=asyncio.Event()
        self.queue=asyncio.Queue(config['market_queue_size'])
        self.repairs=asyncio.Queue(config['market_queue_size'])
        self.pending=set()
        self.pair_locks={}
        self.ws_tasks=[]
        self.completed=0
        self.failed=0
        self.client=None
        self.console_stage='初始化 / 恢复历史数据'
        self.collection=CollectionWindows(self)

    def repair(self,symbol,interval):
        pair=(symbol,interval)
        if pair in self.pending: return
        try: self.repairs.put_nowait(pair);self.pending.add(pair)
        except asyncio.QueueFull: self.log.error(dumps({'event':'REPAIR_QUEUE_FULL','symbol':symbol,'interval':interval}))

    async def history_pair(self,symbol,interval):
        lock=self.pair_locks.setdefault((symbol,interval),asyncio.Lock())
        async with lock:
            state=await asyncio.to_thread(self.store.state,symbol,interval)
            start=state.get('last')+PERIODS[interval] if state and state.get('last') is not None else None
            if start is not None and start>=self.client.time()//PERIODS[interval]*PERIODS[interval] and state.get('health')=='OK':
                return
            origin='RECOVERED' if start is not None else 'HISTORICAL'
            try:
                while True:
                    rows=await self.client.history(symbol,interval,start)
                    if not rows:
                        if start is None: await asyncio.to_thread(self.store.mark,symbol,interval,'NO_DATA','交易价格K线为空，保留监控行')
                        break
                    await asyncio.to_thread(self.store.ingest,symbol,interval,rows,origin,self.client.time())
                    self.completed+=1
                    next_start=int(rows[-1][0])+PERIODS[interval]
                    if start is None or len(rows)<499 or next_start>=self.client.time()//PERIODS[interval]*PERIODS[interval]: break
                    if next_start<=start: raise ValueError('NONADVANCING_HISTORY')
                    start=next_start
                if state and state.get('health') not in ('OK',None):
                    await asyncio.to_thread(self.store.anomaly,symbol,interval,'RECOVERY_COMPLETED')
            except Exception as exc:
                self.failed+=1
                if isinstance(exc,ValueError) and str(exc).startswith('GAP:'):
                    await asyncio.to_thread(self.store.retain_verified_bars,symbol,interval,rows,self.client.time())
                # Only bounded, credential-free exchange/data errors are stored.
                detail=str(exc)[:250] if isinstance(exc,ValueError) else type(exc).__name__
                await asyncio.to_thread(self.store.mark,symbol,interval,'DATA_ERROR',detail)
                self.log.error(dumps({'event':'HISTORY_ERROR','symbol':symbol,'interval':interval,'error':detail}))

    async def bootstrap(self):
        await self.store_async_meta_clock()
        try:
            info=await self.client.exchange_info()
            await asyncio.to_thread(self.store.refresh,info)
        except Exception as exc:
            await asyncio.to_thread(self.store.meta,'universe_stale',True)
            await asyncio.to_thread(self.store.anomaly,'','','UNIVERSE_REFRESH_ERROR',type(exc).__name__)
        universe=await asyncio.to_thread(self.store.universe)
        if not universe: raise RuntimeError('无有效交易对清单')
        jobs=asyncio.Queue()
        for symbol in universe:
            for interval in PERIODS: jobs.put_nowait((symbol,interval))
        async def worker():
            while not jobs.empty() and not self.stop.is_set():
                symbol,interval=jobs.get_nowait()
                await self.history_pair(symbol,interval)
                jobs.task_done()
        tasks=[asyncio.create_task(worker()) for _ in range(self.config['rest_concurrency'])]
        await asyncio.gather(*tasks)
        await asyncio.to_thread(self.store.meta,'bootstrap_finished',self.client.time())
        if self.config['historical_summary']:
            with self.store.lock:
                totals=dict(self.store.db.execute("SELECT interval,count(*) FROM signals WHERE event_type='HISTORICAL' GROUP BY interval").fetchall())
            summary={'event':'历史补发摘要','strategy_version':self.store.version,'counts':totals,'type':'历史汇总，非即时提醒'}
            if self.store.get_meta('historical_summary')!=summary:
                self.log.info(dumps(summary));print(dumps(summary),flush=True)
                await asyncio.to_thread(self.store.meta,'historical_summary',summary)

    async def store_async_meta_clock(self):
        clock=await self.client.sync_clock()
        await asyncio.to_thread(self.store.meta,'clock',clock)
        if abs(clock['offset_ms'])>5000: await asyncio.to_thread(self.store.anomaly,'','','CLOCK_SKEW',str(clock))

    async def repair_worker(self):
        while not self.stop.is_set():
            try: pair=await asyncio.wait_for(self.repairs.get(),1)
            except asyncio.TimeoutError: continue
            try: await self.history_pair(*pair)
            finally: self.pending.discard(pair);self.repairs.task_done()

    async def consume(self):
        while not self.stop.is_set() or not self.queue.empty():
            try: symbol,interval,bar,received,event_time=await asyncio.wait_for(self.queue.get(),1)
            except asyncio.TimeoutError: continue
            lock=self.pair_locks.setdefault((symbol,interval),asyncio.Lock())
            try:
                async with lock:
                    state=await asyncio.to_thread(self.store.state,symbol,interval)
                    if not state or not state.get('last') or state.get('health')!='OK':
                        self.repair(symbol,interval)
                    else:
                        await asyncio.to_thread(self.store.ingest,symbol,interval,[bar],'REALTIME',received,event_time)
            except Exception as exc:
                await asyncio.to_thread(self.store.mark,symbol,interval,'DATA_ERROR',type(exc).__name__)
                self.repair(symbol,interval)
            finally: self.queue.task_done()

    async def subscribe(self):
        # Reconcile only eligible collection windows; never reset overlapping batches.
        await self.collection.refresh()

    async def maintenance(self):
        refreshed=time.monotonic()
        while not self.stop.is_set():
            await asyncio.sleep(10)
            if time.monotonic()-refreshed>=min(self.config['universe_refresh_seconds'],86400):
                try:
                    await self.store_async_meta_clock()
                    info=await self.client.exchange_info()
                    added,removed=await asyncio.to_thread(self.store.refresh,info)
                    if added or removed: await self.subscribe()
                except Exception as exc:
                    await asyncio.to_thread(self.store.meta,'universe_stale',True)
                    await asyncio.to_thread(self.store.anomaly,'','','UNIVERSE_REFRESH_ERROR',type(exc).__name__)
                refreshed=time.monotonic()
            # Periodic reconciliation also repairs a missed close without a WS disconnect.
            universe=await asyncio.to_thread(self.store.universe)
            for symbol in universe:
                for interval,step in PERIODS.items():
                    state=await asyncio.to_thread(self.store.state,symbol,interval)
                    expected=self.client.time()//step*step-step
                    if not state or (state.get('last') or -1)<expected and self.client.time()%step>10000 or state.get('health')!='OK':
                        self.repair(symbol,interval)

    async def health(self):
        next_console=0.
        while not self.stop.is_set():
            report=await asyncio.to_thread(health_snapshot,self.store)
            report.update(pid=os.getpid(),running=True,market_queue=self.queue.qsize(),repair_queue=self.repairs.qsize(),
                          ws_connections=dict(self.client.connections),rest_weight=self.client.budget.server_used,
                          ws_messages=dict(self.client.messages),
                          history_batches=self.completed,history_failures=self.failed,
                          collection=dict(self.collection.report))
            target=Path(self.config['data_dir'])/'health.json'
            temp=target.with_suffix('.tmp');temp.write_text(dumps(report),encoding='utf-8');os.replace(temp,target)
            if time.monotonic()>=next_console:
                receiving=sum(status=='RECEIVING' for status in self.client.connections.values())
                errors=sum(p['ERROR'] for p in report['periods'].values())
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {self.console_stage}；"
                      f"交易对 {report['universe']}；行情连接 {receiving}/{len(self.client.connections)}；"
                      f"补数批次 {self.completed}；历史请求失败 {self.failed}；数据错误 {errors}；"
                      f"累计信号 {report['signals']}。",flush=True)
                next_console=time.monotonic()+60
            if (Path(self.config['data_dir'])/'STOP').exists(): self.stop.set()
            await asyncio.sleep(5)

    async def run(self,mode='run',seconds=0):
        async with PublicClient(self.config) as client:
            self.client=client
            health_task=asyncio.create_task(self.health())
            export_task=None
            try:
                if mode=='run':
                    from .export import export_loop
                    # Publish the startup snapshot immediately, including during recovery.
                    export_task=asyncio.create_task(export_loop(self))
                await self.bootstrap()
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 历史检查完成；"
                      f"本次补数 {self.completed} 批，失败 {self.failed} 批。",flush=True)
                if mode=='bootstrap': return
                from .notifications import dispatch
                await self.subscribe()
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 已启动 {len(self.ws_tasks)} 条行情订阅连接；"
                      "按收盘采集窗口自动启停；窗口外0条连接为正常待机。仅已收盘K线参与提醒，Excel按各周期收盘刷新。",flush=True)
                tasks=[asyncio.create_task(self.maintenance()),
                       asyncio.create_task(self.collection.loop()),
                       asyncio.create_task(dispatch(self))]
                tasks += [asyncio.create_task(self.consume()) for _ in range(8)]
                tasks += [asyncio.create_task(self.repair_worker()) for _ in range(self.config['rest_concurrency'])]
                if seconds:
                    try: await asyncio.wait_for(self.stop.wait(),seconds)
                    except asyncio.TimeoutError: self.stop.set()
                else: await self.stop.wait()
                await self.collection.close()
                self.client.connections={k:'STOPPED' for k in self.client.connections}
                await asyncio.wait_for(self.queue.join(),30)
                for t in tasks: t.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                # Let an in-progress atomic export finish before publishing running=false.
                await export_task
            finally:
                self.stop.set()
                await self.collection.close()
                health_task.cancel()
                await asyncio.gather(health_task,return_exceptions=True)
                if export_task is not None:
                    await export_task
                await asyncio.to_thread(self.store.meta,'last_stopped',now_ms())
                target=Path(self.config['data_dir'])/'health.json'
                report=await asyncio.to_thread(health_snapshot,self.store)
                report.update(running=False,ws_connections=dict(self.client.connections),ws_messages=dict(self.client.messages),
                              market_queue=self.queue.qsize(),repair_queue=self.repairs.qsize(),
                              collection=dict(self.collection.report))
                target.write_text(dumps(report),encoding='utf-8')

def health_snapshot(store):
    with store.lock:
        universe=store.universe()
        counts={i:{'expected':len(universe),'READY':0,'SHORT_WARMUP':0,'NOT_READY':0,'ERROR':0,'MISSING':len(universe),'max_delay_ms':0} for i in PERIODS}
        for row in store.db.execute('SELECT s.* FROM states s JOIN universe u USING(symbol) WHERE u.active=1'):
            if row['symbol'] not in universe:
                continue
            state=json.loads(row['state']); c=counts[row['interval']];c['MISSING']-=1
            c[state.get('readiness','NOT_READY')]+=1
            if state.get('health')!='OK': c['ERROR']+=1
            if state.get('last') is not None:
                c['max_delay_ms']=max(c['max_delay_ms'],max(0,now_ms()-(state['last']+PERIODS[row['interval']]*2)))
        return dict(time=now_ms(),universe=len(universe),periods=counts,
            signals=store.db.execute('SELECT count(*) FROM signals').fetchone()[0],
            notifications=dict(store.db.execute('SELECT status,count(*) FROM outbox GROUP BY status').fetchall()),
            last_export=store.get_meta('last_export'),clock=store.get_meta('clock'),universe_stale=store.get_meta('universe_stale',True),
            export_health=store.get_meta('export_health',dict(by_interval={})),
            history_maintenance=store.get_meta('history_maintenance_health',{}),
            universe_updated=store.get_meta('universe_updated'),
            database='OK' if store.db.execute('SELECT 1').fetchone()[0]==1 else 'ERROR')
