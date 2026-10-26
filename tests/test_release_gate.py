"""Synthetic fixtures test gate refusals; they are not 24-hour run evidence."""
import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import check_release as gate


class ReleaseGateTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.candidate=self.root/'candidate';self.shadow=self.root/'shadow'
        for path in (self.candidate/'monitor',self.candidate/'tools',self.shadow/'runtime',self.shadow/'acceptance'):
            path.mkdir(parents=True,exist_ok=True)
        for name in ('export_soak.py','run_shadow.py'):(self.candidate/'tools'/name).write_text('fixture',encoding='utf-8')
        self.hash=gate.code_hash(self.candidate);self.now=1792800000000;self.start=self.now-gate.HOURS_REQUIRED
        self.state=dict(continuous_start_ms=self.start,started_ms=self.start,last_sample_ms=self.now,
            producer_pid=12345,producer_instance='test-instance',code_hash=self.hash,failures=[],interruptions=[],samples=4321,
            producer_samples=[dict(time=stamp,rss_bytes=10000000) for stamp in range(self.start,self.now+1,60000)],exports={})
        metrics={}
        for interval,seconds in gate.PERIOD_SECONDS.items():
            step=seconds*1000;rows=[]
            for boundary in range((self.start//step+1)*step,self.now-180000+1,step):
                rows.append(dict(time=boundary+10000,boundary=boundary,reason='CANDLE_CLOSE',duration_ms=100,
                    peak_rss_bytes=50000000,coverage=dict(expected=524,completed=524,missing=[]),close_latency_ms=10000))
            if not rows:
                rows.append(dict(time=self.start+1000,boundary=self.start,reason='STARTUP',duration_ms=100,peak_rss_bytes=50000000,coverage=dict(expected=524,completed=524,missing=[])))
            self.state['exports'][interval]=rows
            metrics[interval]=dict(report_boundary=(self.now-180000)//step*step,
                coverage=dict(expected=524,completed=524,missing=[]),circuit=dict(blocked=False))
        self.health=dict(time=self.now,pid=12345,instance_id='test-instance',code_hash=self.hash,running=True,
            source_snapshot=dict(completed_ms=self.now),export_health=dict(by_interval=metrics),shadow_error=None)
        self.descriptor=dict(pid=12345,instance_id='test-instance',code_hash=self.hash,started_ms=self.start)
        self.write(self.shadow/'acceptance/soak-state.json',self.state)
        self.write(self.shadow/'acceptance/soak-24h-report.json',gate.summarize_soak(self.state))
        self.write(self.shadow/'runtime/health.json',self.health)
        self.write(self.shadow/'shadow-run.json',self.descriptor)
        with (self.shadow/'acceptance/soak-samples.jsonl').open('w',encoding='utf-8') as stream:
            for row in self.state['producer_samples']:
                stream.write(json.dumps(dict(time=row['time'],producer_rss_bytes=row['rss_bytes']))+'\n')
        self.history=dict(anomalies=dict(result=dict(snapshot_max_id=1,archived_through_id=1,pending=False),blocked=False,failure_count=0),
                          workbooks=dict(result=dict(pending=0),blocked=False,failure_count=0))
        self.db=sqlite3.connect(self.shadow/'runtime/monitor.sqlite3')
        self.db.executescript('CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT); '
            'CREATE TABLE signal_export_periods(revision INTEGER,exported_revision INTEGER); '
            'CREATE TABLE signals(id TEXT); INSERT INTO signals VALUES("original");')
        self.meta('shadow_runner_state',dict(pending={}))
        self.meta('history_maintenance_health',self.history)
        self.meta('last_export',dict(pending_replace=False,by_interval={}))
        inventory=dict(counts=dict(signals=1,anomalies=1),signal_ids_sha256='a'*64)
        before=dict(inventory=inventory,payload_sha256='b'*64)
        static_metrics={i:dict(peak_rss_bytes=50000000,duration_ms=100) for i in gate.PERIOD_SECONDS}
        self.static=dict(code_hash=self.hash,passed=True,
            reconciliation=dict(expected=1,actual_rows=1,actual_unique=1,missing=0,unexpected=0,duplicate_ids=0,passed=True,signal_ids_sha256='a'*64),
            before=before,after=copy.deepcopy(before),unchanged_payloads=True,anomaly_archive_complete=True,unchanged_history_skipped=True,
            runs=[dict(wall_seconds=1,health=dict(by_interval=static_metrics),history=self.history)])
        self.load=dict(code_hash=self.hash,signals=10000,rows=10001,reconciled=True,signal_id_sha256='c'*64,
            runs=[dict(status='SUCCESS',duration_ms=100,wall_seconds=.2,peak_rss_bytes=50000000,
                books=[dict(signal_count=10000,signal_id_sha256='c'*64,counts=dict(triggers=10000,parameters=1))])])
        self.static_path=self.root/'static.json';self.load_path=self.root/'load.json'
        self.write(self.static_path,self.static);self.write(self.load_path,self.load)

    def tearDown(self):
        self.db.close();self.temp.cleanup()

    def write(self,path,value):path.write_text(json.dumps(value),encoding='utf-8')

    def meta(self,key,value):
        self.db.execute('INSERT OR REPLACE INTO metadata VALUES(?,?)',(key,json.dumps(value)));self.db.commit()

    def check(self,created=None):
        with patch.object(gate,'process_started_ms',return_value=self.start-1000 if created is None else created):
            return gate.check_release(self.candidate,self.shadow,self.static_path,[self.load_path],sample_seconds=60,now=self.now)

    def test_valid_evidence_only_allows_final_freeze_never_automatic_cutover(self):
        result=self.check()
        self.assertEqual([],result['blocking_checks'])
        self.assertTrue(result['ready_for_final_freeze'])
        self.assertFalse(result['ready_for_cutover'])
        self.assertFalse(result['production_changed'])
        self.assertIn('FINAL_CONSISTENT_RECONCILIATION',[row['id'] for row in result['remaining_checks']])

    def test_fabricated_24_hour_report_cannot_override_young_os_process(self):
        result=self.check(created=self.now-3600000)
        self.assertIn('real_elapsed_time',result['blocking_checks'])
        self.assertFalse(result['ready_for_final_freeze'])

    def test_raw_sampling_gap_is_not_hidden_by_passed_report(self):
        path=self.shadow/'acceptance/soak-samples.jsonl'
        lines=path.read_text(encoding='utf-8').splitlines()
        path.write_text('\n'.join(lines[:100]+lines[110:])+'\n',encoding='utf-8')
        self.assertIn('raw_observation',self.check()['blocking_checks'])

    def test_hash_change_and_unbound_static_evidence_block_release(self):
        (self.candidate/'monitor/new.py').write_text('changed',encoding='utf-8')
        result=self.check()
        self.assertIn('candidate_code_hash',result['blocking_checks'])
        self.assertIn('static_evidence_code_binding',result['blocking_checks'])

    def test_pending_dirty_and_history_failure_are_read_from_database(self):
        self.meta('shadow_runner_state',dict(pending={'15m':[self.now]}))
        self.db.execute('INSERT INTO signal_export_periods VALUES(2,1)');self.db.commit()
        self.history['anomalies']['error']='archive unavailable'
        self.meta('history_maintenance_health',self.history)
        result=self.check()
        self.assertIn('no_pending_work',result['blocking_checks'])
        self.assertIn('history_maintenance',result['blocking_checks'])

    def test_load_pass_flag_cannot_override_memory_time_or_count_failure(self):
        self.load['passed']=True
        self.load['runs'][0].update(peak_rss_bytes=600*1024*1024,duration_ms=190000)
        self.load['runs'][0]['books'][0]['signal_count']=9999
        self.write(self.load_path,self.load)
        result=self.check()
        self.assertIn('load_acceptance_metrics_0',result['blocking_checks'])

    def test_stale_health_and_shadow_error_are_blocking(self):
        self.health.update(time=self.now-31000,shadow_error={'reason':'read failed'})
        self.write(self.shadow/'runtime/health.json',self.health)
        self.assertIn('fresh_health',self.check()['blocking_checks'])


if __name__=='__main__':unittest.main()
