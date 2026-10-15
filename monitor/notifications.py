import asyncio
import datetime as dt
import json
import logging
import logging.handlers
import os
from pathlib import Path
from .core import dumps, now_ms

def beijing(ms):
    return dt.datetime.fromtimestamp(ms/1000,dt.timezone(dt.timedelta(hours=8))).isoformat()

def alert(payload):
    b=payload['bar'];m=payload['metrics']
    return dict(type='监控条件触发，不代表实际交易',signal_id=payload['signal_id'],
                strategy_version=payload['strategy_version'],symbol=payload['symbol'],
                contract_type=payload['contract_type'],interval=payload['interval'],
                open_beijing=beijing(b['t']),close_boundary_beijing=beijing(b['T']+1),
                open_utc_ms=b['t'],api_close_utc_ms=b['T'],close_boundary_utc_ms=b['T']+1,
                discovered_utc_ms=payload['discovered'],event_type=payload['event_type'],
                origin=payload['origin'],close=b['c'],EMA12=payload['ema'][0],
                EMA50=payload['ema'][1],EMA200=payload['ema'][2],
                EMA排列='EMA12 > EMA50 > EMA200：满足',base_volume=b['v'],quote_volume_usdt=b['q'],
                V20_mean=m['V20_mean'],V20_max=m['V20_max'],volume_ratio=m['volume_ratio'],
                body=m['body'],relative_body_ratio=m['relative_body_ratio'],bar_return=m['bar_return'],
                previous_max_return=m['G20_max'],breakout=m['breakout'],body_share=m['body_share'],
                upper_wick_share=m['upper_wick_share'],checks=m['checks'],
                readiness=payload['readiness'],discovery_latency_ms=payload['latency_ms'])

class ConsoleAdapter:
    name='console_local'
    def __init__(self,config):
        self.log=logging.getLogger('monitor.events.'+config['data_dir'])
        self.log.setLevel(logging.INFO);self.log.propagate=False
        if not self.log.handlers:
            handler=logging.handlers.RotatingFileHandler(Path(config['data_dir'])/'events.jsonl',
                    maxBytes=config['log_max_bytes'],backupCount=config['log_backup_count'],encoding='utf-8')
            handler.setFormatter(logging.Formatter('%(message)s'));self.log.addHandler(handler)
    async def send(self,message,client):
        line=dumps(message)
        await asyncio.to_thread(self.log.info,line)
        # A detached process may have no stdout; the durable local event log remains available.
        try: print(line,flush=True)
        except (OSError,UnicodeError): pass

class WebhookAdapter:
    name='authorized_webhook'
    def __init__(self,url):
        if not url.startswith('https://'): raise ValueError('外部通知仅接受HTTPS')
        self.url=url
    async def send(self,message,client):
        async with client.session.post(self.url,json=message,headers={'Idempotency-Key':message['signal_id']},
                                       allow_redirects=False) as response:
            if response.status<200 or response.status>=300: raise RuntimeError('WEBHOOK_DELIVERY_FAILED')

def prepare(store):
    with store.lock,store.db:
        store.db.execute('CREATE TABLE IF NOT EXISTS deliveries(id TEXT,adapter TEXT,sent INTEGER,PRIMARY KEY(id,adapter))')

async def dispatch_once(store,config,client,adapters):
    prepare(store)
    with store.lock:
        rows=list(store.db.execute("SELECT * FROM outbox WHERE status IN ('PENDING','RETRY') AND next_attempt<=? ORDER BY next_attempt LIMIT 100",(now_ms(),)))
    for row in rows:
        message=alert(json.loads(row['payload']))
        try:
            for adapter in adapters:
                with store.lock:
                    done=store.db.execute('SELECT 1 FROM deliveries WHERE id=? AND adapter=?',(row['id'],adapter.name)).fetchone()
                if done: continue
                message.update(sent_utc_ms=now_ms(),sent_beijing=beijing(now_ms()),
                               sending_latency_ms=now_ms()-(message['api_close_utc_ms']+1))
                await adapter.send(message,client)
                with store.lock,store.db:
                    store.db.execute('INSERT OR IGNORE INTO deliveries VALUES(?,?,?)',(row['id'],adapter.name,now_ms()))
            with store.lock,store.db:
                store.db.execute("UPDATE outbox SET status='SENT',attempts=attempts+1,sent=?,error=NULL WHERE id=?",(now_ms(),row['id']))
        except Exception as exc:
            attempts=row['attempts']+1
            status='DEAD' if attempts>=config['notification_max_attempts'] else 'RETRY'
            with store.lock,store.db:
                store.db.execute('UPDATE outbox SET status=?,attempts=?,next_attempt=?,error=? WHERE id=?',
                    (status,attempts,now_ms()+min(300,2**attempts)*1000,type(exc).__name__,row['id']))
            store.anomaly(message['symbol'],message['interval'],'NOTIFICATION_'+status,type(exc).__name__)
    return len(rows)

async def dispatch(monitor):
    adapters=[ConsoleAdapter(monitor.config)]
    if monitor.config['webhook_authorized']:
        value=os.environ.get(monitor.config['webhook_url_env'],'')
        if not value:
            monitor.store.anomaly('','','WEBHOOK_CONFIG_MISSING','已授权但未提供URL；阻止错误地标记全部通知成功')
            return
        try: adapters.append(WebhookAdapter(value))
        except ValueError:
            monitor.store.anomaly('','','WEBHOOK_CONFIG_INVALID','必须使用HTTPS');return
    while not monitor.stop.is_set():
        try: await dispatch_once(monitor.store,monitor.config,monitor.client,adapters)
        except Exception as exc: monitor.store.anomaly('','','NOTIFIER_ERROR',type(exc).__name__)
        await asyncio.sleep(1)
