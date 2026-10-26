import asyncio
import json
import tempfile
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from monitor.core import Store, PERIODS
from monitor.export import export_all, export_period_loop, snapshot, close_coverage
from monitor.export_runtime import check_circuit, worker_revision, settings, reset_circuit, maintain_history, run_builder
from test_monitor import CONFIG, BASE, info, bars


class ExportFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.config=dict(CONFIG,data_dir=str(self.root),excel_dir=str(self.root/'excel'))
        (self.root/'excel').mkdir()
        self.store=Store(self.root/'monitor.sqlite3',self.config)

    def tearDown(self):
        self.store.close();self.temp.cleanup()

    def test_oom_records_alert_once_and_survives_restart(self):
        failure=SimpleNamespace(returncode=1,reason='NODE_OOM',stderr='heap out of memory',
            metrics=dict(duration_ms=17,peak_rss_bytes=123456))
        with patch('monitor.export.run_builder',return_value=failure) as build:
            with self.assertRaises(RuntimeError):export_all(self.store,self.config,intervals=['15m'])
            self.assertEqual(export_all(self.store,self.config,intervals=['15m']),[])
            self.store.close();self.store=Store(self.root/'monitor.sqlite3',self.config)
            self.assertEqual(export_all(self.store,self.config,intervals=['15m']),[])
            self.assertEqual(build.call_count,1)
        health=self.store.get_meta('export_health')['by_interval']['15m']
        self.assertTrue(health['circuit']['blocked'])
        self.assertEqual(health['last_failure']['peak_rss_bytes'],123456)
        self.assertEqual(len((self.root/'export-alerts.jsonl').read_text().splitlines()),1)
        reset_circuit(self.store)
        self.assertFalse(check_circuit(self.store,'15m',worker_revision(settings(self.config))))

    def test_generic_failure_stops_after_three_attempts(self):
        failure=SimpleNamespace(returncode=1,stderr='disk write error')
        with patch('monitor.export.run_builder',return_value=failure) as build:
            for attempt in range(3):
                with self.assertRaises(RuntimeError):export_all(self.store,self.config,intervals=['1h'])
            for attempt in range(5):self.assertEqual(export_all(self.store,self.config,intervals=['1h']),[])
            self.assertEqual(build.call_count,3)

    def test_parent_records_actual_v8_fatal_oom_without_child_metrics(self):
        node=Path(os.environ.get('USERPROFILE',''))/'.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node.exe'
        if not node.is_file():self.skipTest('Bundled Node unavailable')
        result=run_builder([str(node),'--max-old-space-size=16','-e',
            "const arrays=[];while(true){arrays.push(new Array(100000).fill('retained'))}"],
            cache=self.root,options=dict(settings(self.config),timeout_seconds=20))
        self.assertNotEqual(result.returncode,0)
        self.assertEqual(result.reason,'NODE_OOM')
        self.assertGreater(result.metrics['peak_rss_bytes'],0)
        self.assertGreater(result.metrics['duration_ms'],0)
        self.assertFalse((self.root/'metrics.json').exists())

    def test_archive_error_has_independent_health_and_bounded_retries(self):
        self.store.meta('export_health',dict(by_interval={'15m':dict(last_success_ms=123,complete=True)}))
        with patch('monitor.history_maintenance.archive_anomalies',side_effect=RuntimeError('checksum')) as archive:
            with patch('monitor.history_maintenance.tier_archives',return_value=dict(moved=0,pending=0)):
                for attempt in range(8):maintain_history(self.store,self.config,'v',BASE)
        self.assertEqual(archive.call_count,3)
        self.assertTrue(self.store.get_meta('history_maintenance_health')['anomalies']['blocked'])
        self.assertEqual(self.store.get_meta('export_health')['by_interval']['15m']['last_success_ms'],123)

    def test_old_close_recovery_uses_exact_saved_evaluation(self):
        self.store.refresh({'symbols':[info()]})
        data=bars(23,'15m')
        self.store.ingest('TESTUSDT','15m',data[:21],'HISTORICAL',BASE+100000000)
        close=data[20][6]+1
        self.store.ingest('TESTUSDT','15m',data[21:],'REALTIME',BASE+100000000)
        self.assertFalse(close_coverage(self.store,'15m',close)['missing'])
        book=snapshot(self.store,self.config,['15m'],'RECOVERY_COMPLETE',close)[0]
        self.assertTrue(book['complete'])
        self.assertEqual(book['overview'][0]['close_boundary'],close)
        self.assertEqual(book['overview'][0]['c'],float(data[20][4]))


class RecoveryRaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_is_durable_before_worker_process_interruption(self):
        class ProcessInterrupted(BaseException):pass
        metadata={}
        store=SimpleNamespace(get_meta=lambda key,default=None:metadata.get(key,default),
            meta=lambda key,value:metadata.update({key:value}),anomaly=lambda *args:None)
        monitor=SimpleNamespace(store=store,config={},client=SimpleNamespace(time=lambda:BASE+1000),stop=asyncio.Event())
        def terminate(*args,**kwargs):
            self.assertEqual(metadata['pending_export_batches_15m'],[BASE])
            raise ProcessInterrupted()
        with patch('monitor.export.export_all',side_effect=terminate):
            with self.assertRaises(ProcessInterrupted):
                await export_period_loop(monitor,'15m')
        self.assertEqual(metadata['pending_export_batches_15m'],[BASE])

    async def test_repair_during_publication_refreshes_same_batch(self):
        metadata={};clock=[BASE+1000];stop=asyncio.Event();calls=[]
        store=SimpleNamespace(get_meta=lambda key,default=None:metadata.get(key,default),
            meta=lambda key,value:metadata.update({key:value}),anomaly=lambda *args:None)
        monitor=SimpleNamespace(store=store,config={},client=SimpleNamespace(time=lambda:clock[0]),stop=stop)
        loop=asyncio.get_running_loop()
        def publish(*args,**kwargs):
            calls.append(kwargs)
            complete=len(calls)>1
            metadata['last_export']={'by_interval':{'15m':dict(boundary=BASE,
                coverage=dict(expected=2,completed=2 if complete else 1,missing=[] if complete else ['LATEUSDT']))}}
            if complete:loop.call_soon_threadsafe(stop.set)
            return ['workbook.xlsx']
        async def pause(*args):
            clock[0]+=1000;await asyncio.sleep(0)
        with patch('monitor.export.export_all',side_effect=publish), \
             patch('monitor.export.close_coverage',return_value=dict(expected=2,completed=2,missing=[])), \
             patch('monitor.export.pause_or_stop',side_effect=pause):
            await asyncio.wait_for(export_period_loop(monitor,'15m'),5)
        self.assertEqual([c['refresh_reason'] for c in calls],['STARTUP','RECOVERY_COMPLETE'])
        self.assertEqual(calls[1]['refresh_boundary'],BASE)
        self.assertEqual(metadata['pending_export_batches_15m'],[])


if __name__=='__main__':unittest.main()
