"""Offline clock, committed-coverage and simulated WebSocket window regressions."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import aiohttp
from monitor.collection import close_window,active_windows,next_window_start,window_snapshot
from monitor.core import PERIODS
from monitor.service import Monitor
from monitor.transport import PublicClient
from test_monitor import BASE,CONFIG,info


DAY=BASE+PERIODS['1d']


class WindowClockTests(unittest.TestCase):
    def test_four_leads_and_coincident_periods(self):
        for boundary,lead,intervals in ((DAY+900000,120000,('15m',)),
                (DAY+3600000,300000,('15m','1h')),
                (DAY+14400000,600000,('15m','1h','4h')),
                (DAY,900000,('15m','1h','4h','1d'))):
            w=close_window(boundary)
            self.assertEqual(w['start'],boundary-lead)
            self.assertEqual(w['intervals'],intervals)
            self.assertNotIn(boundary,[x['boundary'] for x in active_windows(w['start']-1)])
            self.assertIn(boundary,[x['boundary'] for x in active_windows(w['start'])])
            self.assertIn(boundary,[x['boundary'] for x in active_windows(boundary+119999)])
            self.assertNotIn(boundary,[x['boundary'] for x in active_windows(boundary+120000)])

    def test_daily_window_overlaps_prior_quarter_close(self):
        windows=active_windows(DAY-900000)
        self.assertEqual([w['boundary'] for w in windows],[DAY-900000,DAY])
        self.assertEqual(next_window_start(DAY-900001),DAY-900000)
        self.assertEqual(next_window_start(DAY),DAY+780000)

    def test_midnight_week_month_alignment_is_epoch_based(self):
        import datetime as dt
        for date in ('2026-11-01','2026-11-02','2027-01-01'):
            stamp=int(dt.datetime.fromisoformat(date).replace(tzinfo=dt.timezone.utc).timestamp()*1000)
            w=close_window(stamp)
            self.assertEqual(w['intervals'],tuple(PERIODS))
            self.assertEqual(w['start'],stamp-900000)
        with self.assertRaises(ValueError):close_window(DAY+1)


class FakeClient:
    def __init__(self,stamp):
        self.stamp=stamp;self.connections={};self.started=[];self.ended=[]
    def time(self):return self.stamp
    async def stream(self,index,streams,queue,repair,store,stop,**kwargs):
        self.started.append((index,tuple(streams),kwargs))
        self.connections[index]='RECEIVING'
        try:await asyncio.Event().wait()
        finally:self.ended.append(index);self.connections[index]='STOPPED'


class CollectionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.monitor=Monitor(dict(CONFIG,data_dir=self.temp.name,excel_dir=self.temp.name))
        self.monitor.store.refresh({'symbols':[info()]})
        self.monitor.client=FakeClient(DAY-1000000)
        self.manager=self.monitor.collection

    async def asyncTearDown(self):
        self.monitor.stop.set();await self.manager.close()
        self.monitor.store.close()
        for handler in list(self.monitor.log.handlers):
            handler.close();self.monitor.log.removeHandler(handler)
        self.temp.cleanup()

    async def refresh(self,stamp):
        self.monitor.client.stamp=stamp;await self.monitor.subscribe();await asyncio.sleep(0)

    def commit(self,boundary,intervals,symbol='TESTUSDT'):
        for interval in intervals:
            start=boundary-PERIODS[interval]
            bar=[start,100.,102.,99.,101.,100.,boundary-1,10100.]
            self.monitor.store.ingest(symbol,interval,[bar],'HISTORICAL',boundary)

    async def test_idle_then_daily_start_does_not_restart_existing_jobs(self):
        await self.refresh(DAY-180000)
        self.assertIn(DAY,self.manager.batches)
        calls=len(self.monitor.client.started)
        await self.refresh(DAY-170000)
        self.assertEqual(len(self.monitor.client.started),calls)
        self.assertTrue(all(k.isalnum() for k,_,_ in self.monitor.client.started))
        self.assertEqual(set(self.manager.batches[DAY].streams),
            {f'testusdt@kline_{i}' for i in PERIODS})

    async def test_outside_windows_is_healthy_idle_no_subscriptions(self):
        await self.refresh(DAY+180000)
        self.assertEqual(self.monitor.client.started,[])
        self.assertEqual(self.manager.report['state'],'IDLE')
        self.assertEqual(self.manager.report['next_start_ms'],DAY+780000)
        self.assertIn('正常待机',self.monitor.console_stage)

    async def test_queue_receipt_does_not_close_before_committed_coverage(self):
        await self.refresh(DAY-900000)
        self.monitor.queue.put_nowait(('not-committed',))
        await self.refresh(DAY+1000)
        self.assertIn(DAY,self.manager.batches)
        self.assertEqual(self.manager.batches[DAY].missing,4)
        self.commit(DAY,PERIODS)
        await self.refresh(DAY+2000)
        self.assertNotIn(DAY,self.manager.batches)
        started=len(self.monitor.client.started)
        await self.refresh(DAY+3000)
        self.assertEqual(len(self.monitor.client.started),started)

    async def test_prior_window_completion_does_not_cancel_overlapping_daily_window(self):
        prior=DAY-900000
        await self.refresh(prior)
        self.assertEqual(set(self.manager.batches),{prior,DAY})
        daily_tasks={task for _,task in self.manager.batches[DAY].jobs.values()}
        self.commit(prior,('15m',))
        await self.refresh(prior+1000)
        self.assertNotIn(prior,self.manager.batches)
        self.assertIn(DAY,self.manager.batches)
        self.assertTrue(all(not task.done() for task in daily_tasks))

    async def test_slow_prior_socket_shutdown_does_not_delay_new_window_start(self):
        prior=DAY-900000;old_closing=asyncio.Event();release=asyncio.Event();new_started=asyncio.Event()
        original_stream=self.monitor.client.stream
        async def stream(index,streams,*args,**kwargs):
            if kwargs['target_close']==DAY:new_started.set()
            try:await original_stream(index,streams,*args,**kwargs)
            finally:
                if kwargs['target_close']==prior:
                    old_closing.set();await release.wait()
        self.monitor.client.stream=stream
        await self.refresh(prior-1000)
        self.commit(prior,('15m',))
        task=asyncio.create_task(self.refresh(prior))
        try:
            await asyncio.wait_for(old_closing.wait(),2)
            await asyncio.wait_for(new_started.wait(),2)
            self.assertFalse(task.done())
        finally:
            release.set();await task

    async def test_deadline_closes_socket_and_queues_only_missing_pairs(self):
        await self.refresh(DAY-1000)
        self.commit(DAY,('15m','1h'))
        await self.refresh(DAY+120000)
        self.assertNotIn(DAY,self.manager.batches)
        self.assertEqual(self.monitor.pending,{('TESTUSDT','4h'),('TESTUSDT','1d')})
        self.assertEqual(self.monitor.ws_tasks,[])

    async def test_restart_in_post_close_grace_collects_only_unprocessed_batch(self):
        await self.refresh(DAY+1000)
        self.assertIn(DAY,self.manager.batches)
        self.assertEqual(self.manager.batches[DAY].expected,4)
        self.commit(DAY,PERIODS)
        await self.refresh(DAY+2000)
        self.assertEqual(self.monitor.ws_tasks,[])

    async def test_universe_add_remove_does_not_rebuild_unaffected_streams(self):
        await self.refresh(DAY-1000)
        first={task for _,task in self.manager.batches[DAY].jobs.values()}
        self.monitor.store.refresh({'symbols':[info(),info('NEWUSDT')]})
        await self.refresh(DAY-900)
        self.assertEqual(self.manager.batches[DAY].expected,8)
        self.assertTrue(all(not task.done() for task in first))
        self.monitor.store.refresh({'symbols':[info('NEWUSDT')]})
        await self.refresh(DAY-800)
        self.assertEqual(self.manager.batches[DAY].expected,4)
        self.assertTrue(all(s.startswith('newusdt@') for s in self.manager.batches[DAY].streams))
        self.assertTrue(all(task.done() for task in first))

    async def test_523_symbols_four_periods_chunk_without_missing_or_duplicate_streams(self):
        symbols=[info(f'T{n:04}USDT') for n in range(523)]
        self.monitor.store.refresh({'symbols':symbols})
        await self.refresh(DAY-1000)
        calls=self.monitor.client.started
        streams=[stream for _,part,_ in calls for stream in part]
        self.assertEqual(len(calls),11)
        self.assertEqual(len(streams),2092)
        self.assertEqual(len(set(streams)),2092)
        self.assertTrue(all(len(part)<=200 for _,part,_ in calls))
        self.assertEqual(self.manager.batches[DAY].expected,2092)

    async def test_prelisting_close_not_expected_but_state_universe_retained(self):
        self.monitor.store.refresh({'symbols':[info(),info('NEWUSDT',onboardDate=DAY)]})
        await self.refresh(DAY-1000)
        self.assertEqual(len(self.monitor.store.universe()),2)
        self.assertEqual(self.manager.batches[DAY].expected,4)
        self.assertTrue(all(s.startswith('testusdt@') for s in self.manager.batches[DAY].streams))

    async def test_backward_clock_correction_closes_and_can_reopen_window(self):
        boundary=DAY+900000
        await self.refresh(boundary-120000)
        self.assertIn(boundary,self.manager.batches)
        await self.refresh(boundary-121000)
        self.assertNotIn(boundary,self.manager.batches)
        await self.refresh(boundary-120000)
        self.assertIn(boundary,self.manager.batches)

    async def test_snapshot_uses_only_target_close_even_if_current_health_warns(self):
        self.commit(DAY,PERIODS)
        self.monitor.store.mark('TESTUSDT','1h','DATA_ERROR','later warning')
        expected,missing=window_snapshot(self.monitor.store,close_window(DAY),DAY+1000)
        self.assertEqual(len(expected),4);self.assertEqual(missing,set())


class FakeSocket:
    def __init__(self):self.incoming=asyncio.Queue();self.sent=[];self.closed=False;self.unsubscribed=asyncio.Event()
    async def __aenter__(self):return self
    async def __aexit__(self,*args):self.closed=True
    async def send_json(self,data):
        self.sent.append(data)
        if data['method']=='UNSUBSCRIBE':self.unsubscribed.set()
    async def receive(self):return await self.incoming.get()
    def message(self,symbol='TESTUSDT',close=DAY,closed=True):
        k=dict(s=symbol,i='15m',t=close-PERIODS['15m'],T=close-1,o='100',h='102',l='99',c='101',v='1200',q='121200',x=closed)
        self.incoming.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT,data=json.dumps(dict(data=dict(e='kline',E=close,k=k)))))


class WindowTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_target_closed_candle_is_queued(self):
        ws=FakeSocket();client=PublicClient(CONFIG)
        client.session=SimpleNamespace(ws_connect=lambda *a,**k:ws);client.time=lambda:DAY+1000
        queue=asyncio.Queue();repairs=[];stop=asyncio.Event();streams={'testusdt@kline_15m'}
        ws.message(close=DAY,closed=False);ws.message(close=DAY-900000);ws.message()
        task=asyncio.create_task(client.stream('w1s0',list(streams),queue,
            lambda *pair:repairs.append(pair),None,stop,active_streams=lambda:streams,target_close=DAY))
        try:
            item=await asyncio.wait_for(queue.get(),2)
            self.assertEqual(item[:2],('TESTUSDT','15m'))
            self.assertEqual(int(item[2][6])+1,DAY)
            self.assertEqual(client.messages['open_ignored'],1)
            self.assertEqual(client.messages['closed_received'],1)
            self.assertEqual(repairs,[('TESTUSDT','15m')])
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        self.assertTrue(ws.closed)

    async def test_removed_symbol_unsubscribed_without_reconnecting_remaining(self):
        ws=FakeSocket();client=PublicClient(CONFIG)
        client.session=SimpleNamespace(ws_connect=lambda *a,**k:ws);client.time=lambda:DAY+1000
        queue=asyncio.Queue();stop=asyncio.Event();streams={'testusdt@kline_15m','newusdt@kline_15m'}
        task=asyncio.create_task(client.stream('w1s0',list(streams),queue,lambda *pair:None,None,stop,
            active_streams=lambda:streams,target_close=DAY))
        try:
            await asyncio.sleep(.01);streams.remove('newusdt@kline_15m')
            await asyncio.wait_for(ws.unsubscribed.wait(),2.5)
            self.assertEqual(ws.sent[-1]['params'],['newusdt@kline_15m'])
            ws.message(symbol='NEWUSDT');ws.message()
            item=await asyncio.wait_for(queue.get(),2)
            self.assertEqual(item[0],'TESTUSDT')
            self.assertEqual(sum(x['method']=='SUBSCRIBE' for x in ws.sent),1)
            self.assertEqual(client.messages['closed_received'],1)
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)


if __name__=='__main__':unittest.main()
