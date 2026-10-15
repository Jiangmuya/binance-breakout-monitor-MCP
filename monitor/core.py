from __future__ import annotations
import hashlib
import json
import math
import sqlite3
import statistics
import threading
import time
from pathlib import Path

PERIODS = {'15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000}
RULE_KEYS = ('volume_multiple', 'body_multiple', 'min_body_share', 'max_upper_wick_share')
DEFAULT_RULES = dict(zip(RULE_KEYS, (2., 2., .6, .3)))

def now_ms():
    return time.time_ns() // 1000000

def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'), allow_nan=False)

def version(config):
    policy = dict(config)
    policy.update(formula_version=1, periods=PERIODS, seed='first_close', min_bars=200, window=20)
    return 'v1-' + hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()[:16]

def eligible(row):
    return all(row.get(k) == v for k, v in [('quoteAsset', 'USDT'), ('marginAsset', 'USDT'), ('status', 'TRADING')])

def parse_bar(raw, interval, closed_at):
    b = dict(t=int(raw[0]), o=float(raw[1]), h=float(raw[2]), l=float(raw[3]),
             c=float(raw[4]), v=float(raw[5]), T=int(raw[6]), q=float(raw[7]))
    duration = PERIODS[interval]
    if b['T'] + 1 > closed_at:
        raise ValueError('UNCLOSED')
    if b['t'] % duration or b['T'] != b['t'] + duration - 1:
        raise ValueError('INVALID_BOUNDARY')
    if not all(math.isfinite(b[k]) for k in ('o','h','l','c','v','q')):
        raise ValueError('NONFINITE')
    if min(b[k] for k in ('o','h','l','c')) <= 0 or min(b['v'], b['q']) < 0:
        raise ValueError('INVALID_PRICE_VOLUME')
    if b['h'] < max(b['o'], b['c']) or b['l'] > min(b['o'], b['c']) or b['h'] < b['l']:
        raise ValueError('INVALID_OHLC')
    return b

def metrics(b, window, ema, prev_ema, rules):
    if len(window) != 20:
        return {'available': False, 'reason': 'WINDOW_NOT_READY', 'checks': [False]*6, 'trigger': False}
    vs = [x['v'] for x in window]
    abs_bodies = [abs(x['c']-x['o']) for x in window]
    returns = [x['c']/x['o']-1 for x in window]
    positives = [x['c']-x['o'] for x in window if x['c'] > x['o']]
    m = dict(body=b['c']-b['o'], abs_body=abs(b['c']-b['o']),
             relative_body=abs(b['c']/b['o']-1), bar_return=b['c']/b['o']-1,
             close_return=b['c']/window[-1]['c']-1,
             V20_mean=statistics.fmean(vs), V20_median=statistics.median(vs), V20_max=max(vs),
             B20_mean=statistics.fmean(map(abs,returns)), G20_mean=statistics.fmean(returns),
             G20_median=statistics.median(returns), G20_max=max(returns), H20=max(x['h'] for x in window),
             abs_body20_mean=statistics.fmean(abs_bodies), abs_body20_median=statistics.median(abs_bodies),
             abs_body20_max=max(abs_bodies), positive_count=len(positives),
             positive_body_mean=statistics.fmean(positives) if positives else None)
    def ratio(a, z): return a/z if z > 0 else None
    m.update(volume_ratio=ratio(b['v'],m['V20_mean']),
             relative_body_ratio=ratio(m['relative_body'],m['B20_mean']),
             body_share=ratio(m['abs_body'],b['h']-b['l']),
             upper_wick_share=ratio(b['h']-max(b['o'],b['c']),b['h']-b['l']),
             abs_body_mean_ratio=ratio(m['abs_body'],m['abs_body20_mean']),
             abs_body_median_ratio=ratio(m['abs_body'],m['abs_body20_median']),
             abs_body_max_ratio=ratio(m['abs_body'],m['abs_body20_max']),
             abs_body_vs_mean=m['abs_body']-m['abs_body20_mean'],
             abs_body_vs_median=m['abs_body']-m['abs_body20_median'],
             abs_body_vs_max=m['abs_body']-m['abs_body20_max'],
             return_vs_mean=m['bar_return']-m['G20_mean'],
             return_vs_max=m['bar_return']-m['G20_max'],
             breakout=b['c']/m['H20']-1,
             volume_rank=1+sum(v>b['v'] for v in vs),
             body_rank=1+sum(x>m['abs_body'] for x in abs_bodies),
             return_rank=1+sum(x>m['bar_return'] for x in returns))
    m['ema_bullish'] = ema[0] > ema[1] > ema[2]
    m['available'] = all(m[k] is not None for k in ('volume_ratio','relative_body_ratio','body_share','upper_wick_share'))
    m['checks'] = [b['c']>b['o'] and b['c']>m['H20'],
                   b['c']>ema[0]>ema[1]>ema[2] and ema[0]>prev_ema[0],
                   b['v']>=rules['volume_multiple']*m['V20_mean'] and b['v']>m['V20_max'],
                   m['relative_body']>=rules['body_multiple']*m['B20_mean'],
                   m['bar_return']>m['G20_max'],
                   m['available'] and m['body_share']>=rules['min_body_share'] and m['upper_wick_share']<=rules['max_upper_wick_share']]
    m['trigger'] = m['available'] and all(m['checks'])
    m['reason'] = '' if m['available'] else 'ZERO_DENOMINATOR'
    m['thresholds'] = dict(rules)
    return m

SCHEMA = '''
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO schema_version VALUES(1);
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS universe(symbol TEXT PRIMARY KEY,active INTEGER NOT NULL,info TEXT NOT NULL,reason TEXT,updated INTEGER);
CREATE TABLE IF NOT EXISTS versions(version TEXT PRIMARY KEY,config TEXT NOT NULL,created INTEGER);
CREATE TABLE IF NOT EXISTS bars(symbol TEXT,interval TEXT,t INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,close_time INTEGER,q REAL,
 PRIMARY KEY(symbol,interval,t)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS states(symbol TEXT,interval TEXT,state TEXT NOT NULL,PRIMARY KEY(symbol,interval));
CREATE TABLE IF NOT EXISTS evaluations(version TEXT,symbol TEXT,interval TEXT,t INTEGER,payload TEXT NOT NULL,
 PRIMARY KEY(version,symbol,interval,t)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY,version TEXT,symbol TEXT,interval TEXT,t INTEGER,event_type TEXT,
 payload TEXT NOT NULL,UNIQUE(version,symbol,interval,t));
CREATE INDEX IF NOT EXISTS signal_period ON signals(interval,t);
CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY REFERENCES signals(id),payload TEXT NOT NULL,status TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL,sent INTEGER,error TEXT);
CREATE TABLE IF NOT EXISTS anomalies(id INTEGER PRIMARY KEY AUTOINCREMENT,time INTEGER,symbol TEXT,interval TEXT,code TEXT,detail TEXT);
'''

class Store:
    def __init__(self, path, config):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.db = sqlite3.connect(path,check_same_thread=False,timeout=30)
        self.db.row_factory=sqlite3.Row
        self.db.executescript(SCHEMA)
        self.lock=threading.RLock()
        self.config=config
        self.rules={k:config[k] for k in RULE_KEYS}
        self.version=version(config)
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO versions VALUES(?,?,?)',(self.version,dumps(config),now_ms()))

    def anomaly(self,symbol,interval,code,detail=''):
        with self.lock,self.db:
            self.db.execute('INSERT INTO anomalies(time,symbol,interval,code,detail) VALUES(?,?,?,?,?)',
                            (now_ms(),symbol,interval,code,str(detail)[:2000]))

    def meta(self,key,value):
        with self.lock,self.db:
            self.db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)',(key,dumps(value)))

    def get_meta(self,key,default=None):
        with self.lock:
            row=self.db.execute('SELECT value FROM metadata WHERE key=?',(key,)).fetchone()
            return json.loads(row[0]) if row else default

    def refresh(self,info):
        rows=info.get('symbols') or []
        chosen={x['symbol']:x for x in rows if eligible(x)}
        if not chosen:
            self.meta('universe_stale',True)
            self.anomaly('', '', 'UNIVERSE_REFRESH_EMPTY','保留上一次有效清单')
            return set(),set()
        with self.lock,self.db:
            old={r[0] for r in self.db.execute('SELECT symbol FROM universe WHERE active=1')}
            for x in rows:
                selected=eligible(x)
                reason='' if selected else ';'.join(f'{k}={x.get(k)}' for k,v in
                       [('quoteAsset','USDT'),('marginAsset','USDT'),('status','TRADING')] if x.get(k)!=v)
                self.db.execute('INSERT OR REPLACE INTO universe VALUES(?,?,?,?,?)',
                    (x['symbol'],int(selected),dumps(x),reason,now_ms()))
            for s in old-set(chosen):
                self.db.execute('UPDATE universe SET active=0,reason=? WHERE symbol=?',('REMOVED_OR_INACTIVE',s))
            for s in set(chosen)-old:
                self.db.execute('INSERT INTO anomalies(time,symbol,interval,code,detail) VALUES(?,?,?,?,?)',
                                (now_ms(),s,'','SYMBOL_ADDED',chosen[s].get('contractType','')))
            for s in old-set(chosen):
                self.db.execute('INSERT INTO anomalies(time,symbol,interval,code,detail) VALUES(?,?,?,?,?)',
                                (now_ms(),s,'','SYMBOL_REMOVED','保留历史'))
        self.meta('universe_stale',False)
        self.meta('universe_updated',now_ms())
        return set(chosen)-old,old-set(chosen)

    def universe(self):
        with self.lock:
            return {r['symbol']:json.loads(r['info']) for r in self.db.execute('SELECT * FROM universe WHERE active=1 ORDER BY symbol')}

    def state(self,symbol,interval):
        with self.lock:
            row=self.db.execute('SELECT state FROM states WHERE symbol=? AND interval=?',(symbol,interval)).fetchone()
            return json.loads(row[0]) if row else None

    def mark(self,symbol,interval,status,detail=''):
        with self.lock,self.db:
            state=self.state(symbol,interval) or {'count':0,'window':[]}
            state.update(health=status,detail=detail,updated=now_ms())
            self.db.execute('INSERT OR REPLACE INTO states VALUES(?,?,?)',(symbol,interval,dumps(state)))
        self.anomaly(symbol,interval,status,detail)

    def retain_verified_bars(self,symbol,interval,raw,received):
        """Keep valid source history even when continuity prevents indicator calculation."""
        bars=[parse_bar(x,interval,received) for x in raw]
        with self.lock,self.db:
            self.db.executemany('INSERT OR IGNORE INTO bars VALUES(?,?,?,?,?,?,?,?,?,?)',
                [(symbol,interval,*[b[k] for k in ('t','o','h','l','c','v','T','q')]) for b in bars])

    def ingest(self,symbol,interval,raw,origin='REALTIME',received=None,event_time=None):
        received=received or now_ms()
        processed=now_ms()
        discovered=max(received,processed+self.get_meta('clock',{}).get('offset_ms',0))
        bars=[parse_bar(x,interval,received) for x in raw]
        # Validate the complete batch before mutating EMA. Inconsistent duplicate data is quarantined.
        unique={}
        for b in bars:
            if b['t'] in unique and b!=unique[b['t']]:
                raise ValueError('CONFLICTING_DUPLICATE')
            unique[b['t']]=b
        bars=sorted(unique.values(),key=lambda x:x['t'])
        with self.lock,self.db:
            s=dict(count=0,window=[],ema=None,last=None,seed=None)
            s.update(self.state(symbol,interval) or {})
            active=self.db.execute('SELECT info FROM universe WHERE symbol=? AND active=1',(symbol,)).fetchone()
            for b in bars:
                old=self.db.execute('SELECT t,o,h,l,c,v,close_time,q FROM bars WHERE symbol=? AND interval=? AND t=?',
                                    (symbol,interval,b['t'])).fetchone()
                if old and dict(zip(('t','o','h','l','c','v','T','q'),old))!=b:
                    raise ValueError('CONFLICTING_DUPLICATE')
            pending=[b for b in bars if s.get('last') is None or b['t']>s['last']]
            previous=s.get('last')
            for b in pending:
                if previous is not None and b['t'] != previous+PERIODS[interval]:
                    raise ValueError(f'GAP:{previous+PERIODS[interval]}:{b["t"]}')
                previous=b['t']
            # Even historical bars are durable; duplicate inserts leave the original values unchanged.
            self.db.executemany('INSERT OR IGNORE INTO bars VALUES(?,?,?,?,?,?,?,?,?,?)',
                [(symbol,interval,*[b[k] for k in ('t','o','h','l','c','v','T','q')]) for b in bars])
            events=[]
            for index,b in enumerate(pending):
                prev=s['ema'] or [b['c']]*3
                ema=[(2/(n+1))*b['c']+(1-2/(n+1))*x for n,x in zip((12,50,200),prev)]
                count=s['count']+1
                ready='READY' if count>=1500 else ('SHORT_WARMUP' if count>=200 else 'NOT_READY')
                assess=origin!='HISTORICAL' or self.config.get('historical_analysis') or index==len(pending)-1
                m=metrics(b,s['window'][-20:],ema,prev,self.rules) if assess else None
                if m:
                    m['trigger']=bool(m['trigger'] and count>=200 and active)
                    payload=dict(symbol=symbol,contract_type=json.loads(active[0]).get('contractType','') if active else '',
                                 interval=interval,bar=b,ema=ema,previous_ema=prev,metrics=m,readiness=ready,
                                 count=count,seed=s['seed'] or b['t'],strategy_version=self.version,
                                 discovered=discovered,received=received,processed=processed,event_time=event_time,origin=origin)
                    self.db.execute('INSERT OR IGNORE INTO evaluations VALUES(?,?,?,?,?)',
                                    (self.version,symbol,interval,b['t'],dumps(payload)))
                    s['latest']=payload
                    if m['trigger']:
                        signal_id=hashlib.sha256(f'{self.version}:{symbol}:{interval}:{b["t"]}'.encode()).hexdigest()
                        age=discovered-(b['T']+1)
                        event_type='HISTORICAL' if origin=='HISTORICAL' else ('EXPIRED' if age>self.config['live_max_age_seconds']*1000 else origin)
                        payload=dict(payload,signal_id=signal_id,event_type=event_type,latency_ms=age,
                                     previous20=list(s['window'][-20:]),previous20_before_close=s.get('before_window_close'))
                        inserted=self.db.execute('INSERT OR IGNORE INTO signals VALUES(?,?,?,?,?,?,?)',
                             (signal_id,self.version,symbol,interval,b['t'],event_type,dumps(payload))).rowcount
                        if inserted:
                            events.append(payload)
                            if event_type in ('REALTIME','RECOVERED'):
                                self.db.execute('INSERT INTO outbox(id,payload,status,next_attempt) VALUES(?,?,?,?)',
                                                (signal_id,dumps(payload),'PENDING',received))
                s.update(count=count,ema=ema,last=b['t'],seed=s['seed'] if s['seed'] is not None else b['t'],
                         readiness=ready,health='OK',detail='',updated=processed)
                if len(s['window'])>=20:
                    s['before_window_close']=s['window'][0]['c']
                s['window']=(s['window']+[b])[-20:]
            if bars:
                s.update(health='OK',detail='',updated=processed)
            self.db.execute('INSERT OR REPLACE INTO states VALUES(?,?,?)',(symbol,interval,dumps(s)))
            return events

    def close(self):
        with self.lock:
            self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            self.db.close()
