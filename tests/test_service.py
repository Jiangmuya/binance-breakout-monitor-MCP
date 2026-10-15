import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import AsyncMock
from types import SimpleNamespace
from test_monitor import CONFIG, info, bars, signal_bar
from monitor.service import Monitor
from monitor.core import PERIODS, now_ms
from monitor.notifications import dispatch_once

class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.config=dict(CONFIG,data_dir=self.temp.name,excel_dir=self.temp.name)
        self.monitor=Monitor(self.config)
        self.monitor.store.refresh({'symbols':[info()]})

    async def asyncTearDown(self):
        self.monitor.store.close()
        for handler in list(self.monitor.log.handlers):
            handler.close();self.monitor.log.removeHandler(handler)
        self.temp.cleanup()

    async def test_gap_repair_paginated_and_seed_preserved(self):
        rows=bars(1200);store=self.monitor.store
        store.ingest('TESTUSDT','1h',rows[:200],'HISTORICAL')
        calls=[]
        class Client:
            def time(self):return now_ms()
            async def history(self,symbol,interval,start):
                calls.append(start)
                return [b for b in rows if b[0]>=start][:499]
        self.monitor.client=Client()
        await self.monitor.history_pair('TESTUSDT','1h')
        self.assertEqual(store.state('TESTUSDT','1h')['count'],1200)
        self.assertEqual(store.state('TESTUSDT','1h')['seed'],rows[0][0])
        self.assertEqual(len(calls),3)

    async def test_repair_queue_coalesces(self):
        for _ in range(30):self.monitor.repair('TESTUSDT','1h')
        self.assertEqual(self.monitor.repairs.qsize(),1)

    async def test_notification_retry_skips_delivered_adapter(self):
        store=self.monitor.store;rows=bars(200);store.ingest('TESTUSDT','1h',rows,'HISTORICAL')
        b=signal_bar(rows[-1]);received=b[6]+10000
        with patch('monitor.core.now_ms',return_value=received):store.ingest('TESTUSDT','1h',[b],'REALTIME',received)
        calls={'good':0,'bad':0}
        class Good:
            name='good'
            async def send(self,*args):calls['good']+=1
        class Bad:
            name='bad'
            async def send(self,*args):
                calls['bad']+=1
                if calls['bad']==1:raise TimeoutError('temporary')
        adapters=[Good(),Bad()]
        await dispatch_once(store,self.config,None,adapters)
        with store.db:store.db.execute('UPDATE outbox SET next_attempt=0')
        await dispatch_once(store,self.config,None,adapters)
        self.assertEqual(calls,{'good':1,'bad':2})
        self.assertEqual(store.db.execute('SELECT status FROM outbox').fetchone()[0],'SENT')

    async def test_slow_processing_marks_expired(self):
        store=self.monitor.store;rows=bars(200);store.ingest('TESTUSDT','1h',rows,'HISTORICAL')
        b=signal_bar(rows[-1]);received=b[6]+1000
        with patch('monitor.core.now_ms',return_value=b[6]+121001):
            p=store.ingest('TESTUSDT','1h',[b],'REALTIME',received)[0]
        self.assertEqual(p['event_type'],'EXPIRED')
        self.assertEqual(p['received'],received)

    async def test_incomplete_source_retained_without_signals(self):
        rows=bars(210);rows.pop(100)
        class Client:
            def time(self):return now_ms()
            async def history(self,*args):return rows
        self.monitor.client=Client()
        await self.monitor.history_pair('TESTUSDT','1h')
        self.assertEqual(self.monitor.store.state('TESTUSDT','1h')['health'],'DATA_ERROR')
        self.assertEqual(self.monitor.store.db.execute('SELECT count(*) FROM bars').fetchone()[0],209)
        self.assertEqual(self.monitor.store.db.execute('SELECT count(*) FROM signals').fetchone()[0],0)

    async def test_shutdown_waits_for_export(self):
        gate=asyncio.Event();started=asyncio.Event()
        class Client:
            connections={};messages={};budget=SimpleNamespace(server_used=0)
            def __init__(self,*args):pass
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
        async def export(_):started.set();await gate.wait()
        with patch('monitor.service.PublicClient',Client),patch.object(self.monitor,'bootstrap',new=AsyncMock()),patch.object(self.monitor,'subscribe',new=AsyncMock()),patch('monitor.notifications.dispatch',new=AsyncMock()),patch('monitor.export.export_loop',new=export):
            task=asyncio.create_task(self.monitor.run(seconds=.02))
            await started.wait();await asyncio.sleep(.08)
            self.assertFalse(task.done())
            gate.set();await task
        health=json.loads((Path(self.temp.name)/'health.json').read_text(encoding='utf-8'))
        self.assertFalse(health['running'])

if __name__=='__main__':unittest.main()
