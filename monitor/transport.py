"""GET-only Binance public REST adapter and closed-candle public streams."""
import asyncio
import collections
import json
import random
import time
import urllib.request
import aiohttp
from .core import PERIODS, now_ms

REST_BASE='https://fapi.binance.com'
WS_BASE='wss://fstream.binance.com/market/stream'
ALLOWED=frozenset(('/fapi/v1/time','/fapi/v1/exchangeInfo','/fapi/v1/klines'))

class WeightBudget:
    def __init__(self,limit):
        self.limit=limit
        self.used=collections.deque()
        self.lock=asyncio.Lock()
        self.server_used=0
        self.blocked_until=0.

    async def acquire(self,weight):
        while True:
            async with self.lock:
                current=time.monotonic()
                while self.used and self.used[0][0]<=current-60:
                    self.used.popleft()
                total=sum(x[1] for x in self.used)
                if current>=self.blocked_until and total+weight<=self.limit:
                    self.used.append((current,weight))
                    return
                delay=max(.1,self.blocked_until-current,
                          self.used[0][0]+60-current if self.used else .1)
            await asyncio.sleep(min(delay,5))

class PublicClient:
    def __init__(self,config):
        self.config=config
        self.budget=WeightBudget(config['rest_weight_per_minute'])
        self.semaphore=asyncio.Semaphore(config['rest_concurrency'])
        self.session=None
        self.offset_ms=0
        self.last_clock_sync=0
        self.connections={}
        self.messages={'open_ignored':0,'closed_received':0,'last_event_ms':None}

    async def __aenter__(self):
        self.session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=35),trust_env=True,
                    connector=aiohttp.TCPConnector(limit=40,ttl_dns_cache=300))
        return self

    async def __aexit__(self,*args):
        await self.session.close()

    def time(self): return now_ms()+self.offset_ms

    async def get(self,path,params=None,weight=1):
        if path not in ALLOWED:
            raise ValueError('Endpoint not in public GET allowlist')
        for attempt in range(6):
            await self.budget.acquire(weight)
            try:
                async with self.semaphore,self.session.get(REST_BASE+path,params=params) as response:
                    used=int(response.headers.get('X-MBX-USED-WEIGHT-1M','0'))
                    self.budget.server_used=used
                    if used>=self.budget.limit:
                        self.budget.blocked_until=time.monotonic()+61
                    if response.status in (418,429):
                        retry=max(60,float(response.headers.get('Retry-After','60')))
                        self.budget.blocked_until=time.monotonic()+retry
                        raise RuntimeError(f'REST_RATE_LIMIT_{response.status}')
                    if response.status>=500:
                        raise RuntimeError(f'REST_{response.status}')
                    response.raise_for_status()
                    return await response.json()
            except (aiohttp.ClientError,asyncio.TimeoutError,RuntimeError):
                if attempt==5: raise
                await asyncio.sleep(min(30,2**attempt)+random.random())

    async def sync_clock(self):
        before=now_ms()
        value=await self.get('/fapi/v1/time')
        after=now_ms()
        self.offset_ms=int(value['serverTime']-(before+after)//2)
        self.last_clock_sync=after
        return {'offset_ms':self.offset_ms,'roundtrip_ms':after-before,'synced':after}

    async def exchange_info(self):
        info=await self.get('/fapi/v1/exchangeInfo')
        limits=[r['limit'] for r in info.get('rateLimits',[]) if r.get('rateLimitType')=='REQUEST_WEIGHT'
                and r.get('interval')=='MINUTE' and r.get('intervalNum')==1]
        if limits:
            self.budget.limit=min(self.config['rest_weight_per_minute'],int(min(limits)*.75))
        return info

    async def history(self,symbol,interval,start=None):
        boundary=self.time()//PERIODS[interval]*PERIODS[interval]
        limit=1500 if start is None else 499
        params={'symbol':symbol,'interval':interval,'endTime':boundary-1,'limit':limit}
        if start is not None: params['startTime']=start
        rows=await self.get('/fapi/v1/klines',params,10 if limit==1500 else 2)
        return rows

    async def stream(self,index,streams,queue,repair,store,stop,*,active_streams=None,target_close=None):
        attempt=0
        while not stop.is_set():
            try:
                wanted=set(streams) if active_streams is None else set(streams)&active_streams()
                if not wanted:
                    self.connections[index]='STOPPED'
                    return
                self.connections[index]='CONNECTING'
                async with self.session.ws_connect(WS_BASE,proxy=urllib.request.getproxies().get('https'),autoping=True,timeout=aiohttp.ClientWSTimeout(ws_close=10)) as ws:
                    await ws.send_json({'method':'SUBSCRIBE','params':sorted(wanted),'id':str(index)})
                    self.connections[index]='CONNECTED'
                    connected=time.monotonic()
                    last_received=connected
                    last_membership_check=connected
                    subscribed=wanted
                    for stream in subscribed:
                        symbol,interval=stream.split('@kline_')
                        repair(symbol.upper(),interval)
                    attempt=0
                    while not stop.is_set() and time.monotonic()-connected<23*3600:
                        if active_streams is not None and time.monotonic()-last_membership_check>=1:
                            wanted=set(streams)&active_streams()
                            if not wanted:
                                self.connections[index]='STOPPED'
                                return
                            removed=subscribed-wanted
                            added=wanted-subscribed
                            if removed:
                                await ws.send_json({'method':'UNSUBSCRIBE','params':sorted(removed),'id':str(index)+'u'})
                            if added:
                                await ws.send_json({'method':'SUBSCRIBE','params':sorted(added),'id':str(index)+'a'})
                            subscribed=wanted
                            last_membership_check=time.monotonic()
                        try:
                            message=await asyncio.wait_for(ws.receive(),timeout=1 if active_streams is not None else 240)
                        except asyncio.TimeoutError:
                            if active_streams is not None and time.monotonic()-last_received<240:
                                continue
                            raise
                        last_received=time.monotonic()
                        if message.type==aiohttp.WSMsgType.TEXT:
                            data=json.loads(message.data)
                            data=data.get('data',data)
                            if 'code' in data: raise RuntimeError(f'WS_SUBSCRIPTION_{data["code"]}')
                            if data.get('e')!='kline': continue
                            k=data['k']
                            if active_streams is not None and f"{k['s'].lower()}@kline_{k['i']}" not in active_streams():
                                continue
                            self.connections[index]='RECEIVING'
                            self.messages['last_event_ms']=data.get('E')
                            if not k['x']:
                                self.messages['open_ignored']+=1
                                continue
                            if target_close is not None and int(k['T'])+1!=target_close:
                                continue
                            self.messages['closed_received']+=1
                            item=(k['s'],k['i'],[k['t'],k['o'],k['h'],k['l'],k['c'],k['v'],k['T'],k['q']],
                                  self.time(),data.get('E'))
                            try: queue.put_nowait(item)
                            except asyncio.QueueFull:
                                repair(k['s'],k['i'])
                                await asyncio.to_thread(store.mark,k['s'],k['i'],'BACKPRESSURE','行情队列满，安排REST补数')
                        elif message.type in (aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                            raise ConnectionError('WS_DISCONNECTED')
            except asyncio.CancelledError: raise
            except Exception as exc:
                self.connections[index]='RECONNECTING'
                await asyncio.to_thread(store.anomaly,'','','WS_RECONNECT',type(exc).__name__)
                retry_streams=set(streams) if active_streams is None else set(streams)&active_streams()
                for stream in retry_streams:
                    symbol,interval=stream.split('@kline_')
                    repair(symbol.upper(),interval)
                attempt+=1
                await asyncio.sleep(min(60,2**min(attempt,6))+random.random())
        self.connections[index]='STOPPED'
