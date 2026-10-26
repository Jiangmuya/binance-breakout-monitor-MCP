"""Read-only release evidence gate. This tool never switches the production code.

PASSED in a JSON report is insufficient: recompute the soak verdict, check raw
sampling continuity, the live process creation time, current health and the
candidate code fingerprint. Final frozen-file reconciliation remains separate.
"""
import argparse
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parent))
from export_soak import PERIOD_SECONDS
from soak_policy_24h import summarize_soak
from run_shadow import code_hash

HOURS_REQUIRED = 24*3600_000


def number(value,minimum=0):
    return not isinstance(value,bool) and isinstance(value,(int,float)) and math.isfinite(value) and value>=minimum


def read_json(path):
    path = Path(path)
    if path.stat().st_size>64*1024*1024:
        raise ValueError('Evidence JSON exceeds 64 MiB: '+str(path))
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(value,dict):
        raise ValueError('Expected a JSON object: '+str(path))
    return value


def process_started_ms(pid):
    """Read OS process creation time; report timestamps cannot substitute for it."""
    if not isinstance(pid,int) or isinstance(pid,bool) or pid<=0:
        return None
    try:
        if os.name=='nt':
            from ctypes import wintypes
            kernel=ctypes.WinDLL('kernel32',use_last_error=True)
            kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
            kernel.OpenProcess.restype=wintypes.HANDLE
            kernel.CloseHandle.argtypes=[wintypes.HANDLE]
            kernel.GetProcessTimes.argtypes=[wintypes.HANDLE]+[ctypes.POINTER(wintypes.FILETIME)]*4
            handle=kernel.OpenProcess(0x1000,False,pid)
            if not handle:return None
            try:
                fields=[wintypes.FILETIME() for _ in range(4)]
                if not kernel.GetProcessTimes(handle,*[ctypes.byref(field) for field in fields]):return None
                created=(fields[0].dwHighDateTime<<32)|fields[0].dwLowDateTime
                return created//10000-11644473600000
            finally:kernel.CloseHandle(handle)
        stat=Path(f'/proc/{pid}/stat').read_text()
        ticks=int(stat[stat.rfind(')')+2:].split()[19])
        boot=next(int(row.split()[1]) for row in Path('/proc/stat').read_text().splitlines() if row.startswith('btime '))
        return boot*1000+ticks*1000//os.sysconf('SC_CLK_TCK')
    except (OSError,ValueError,IndexError,StopIteration):
        return None


def raw_observation(path,state,now,sample_seconds):
    start=state.get('continuous_start_ms');end=state.get('last_sample_ms')
    if not number(start,1) or not number(end,start):raise ValueError('Invalid continuous segment timestamps')
    known={row['time']:row.get('rss_bytes') for row in state.get('producer_samples',[]) if isinstance(row,dict) and 'time' in row}
    first=last=None;count=0;max_gap=0;unmatched=0;matched=set()
    with Path(path).open(encoding='utf-8') as stream:
        for line in stream:
            if len(line)>2*1024*1024:raise ValueError('Oversized raw sample')
            row=json.loads(line);stamp=row.get('time')
            if not number(stamp,1):raise ValueError('Invalid raw sample timestamp')
            if stamp<start:continue
            if stamp>end:break  # the producer may append after the state was read
            if stamp>now+1000:raise ValueError('Raw sample is in the future')
            if last is not None:
                if stamp<=last:raise ValueError('Raw samples are not strictly increasing')
                max_gap=max(max_gap,stamp-last)
            if first is None:first=stamp
            last=stamp;count+=1
            if not number(row.get('producer_rss_bytes'),1):raise ValueError('Missing raw parent memory sample')
            if stamp in known:
                matched.add(stamp)
                if known[stamp]!=row['producer_rss_bytes']:unmatched+=1
            if row.get('shadow_error') or row.get('read_error'):raise ValueError('Raw observation includes producer/read error')
    gap_limit=sample_seconds*2500
    omitted=sum(stamp not in matched for stamp in known if last is not None and stamp<=last)
    valid=bool(first is not None and last is not None and last-first>=HOURS_REQUIRED and
        first-start<=gap_limit and end-last<=gap_limit and max_gap<=gap_limit and
        count>=HOURS_REQUIRED/gap_limit and not unmatched and not omitted and all(first<=stamp<=end for stamp in known))
    return dict(passed=valid,samples=count,first_ms=first,last_ms=last,max_gap_ms=max_gap,
                allowed_gap_ms=gap_limit,state_rss_mismatches=unmatched,state_samples_missing_from_raw=omitted)


def inspect_database(path):
    db=sqlite3.connect(Path(path).resolve(strict=True).as_uri()+'?mode=ro',uri=True,timeout=10)
    try:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        metadata={key:json.loads(value) for key,value in db.execute('SELECT key,value FROM metadata WHERE key IN '
            "('shadow_runner_state','history_maintenance_health','last_export')")}
        dirty=db.execute('SELECT count(*) FROM signal_export_periods WHERE revision>exported_revision').fetchone()[0]
        return dict(metadata=metadata,dirty_periods=dirty,
                    signal_count=db.execute('SELECT count(*) FROM signals').fetchone()[0])
    finally:db.close()


def clean_history(history):
    errors=[]
    for name in ('anomalies','workbooks'):
        item=history.get(name)
        if not isinstance(item,dict):errors.append(name+': missing maintenance evidence');continue
        result=item.get('result') or {}
        if item.get('error') or item.get('blocked') or item.get('failure_count',0):errors.append(name+': maintenance error/circuit')
        if result.get('pending'):errors.append(name+': pending archive work')
        if name=='anomalies' and (not number(result.get('archived_through_id')) or
                not number(result.get('snapshot_max_id')) or result['archived_through_id']<result['snapshot_max_id']):
            errors.append(name+': incomplete archive watermark')
    return errors


def static_acceptance(report,budget,target):
    errors=[];reconciliation=report.get('reconciliation') or {}
    expected=reconciliation.get('expected')
    if report.get('passed') is not True:errors.append('passed is not true')
    if (reconciliation.get('passed') is not True or not number(expected,1) or
        any(reconciliation.get(key)!=0 for key in ('missing','unexpected','duplicate_ids')) or
        reconciliation.get('actual_rows')!=expected or reconciliation.get('actual_unique')!=expected):
        errors.append('signal ID reconciliation is missing or inconsistent')
    before=report.get('before') or {};after=report.get('after') or {}
    inventory=before.get('inventory') or {}
    if (not before or before!=after or report.get('unchanged_payloads') is not True or
        inventory.get('counts',{}).get('signals')!=expected or
        inventory.get('signal_ids_sha256')!=reconciliation.get('signal_ids_sha256')):
        errors.append('record counts/payload history were not preserved')
    if report.get('anomaly_archive_complete') is not True or report.get('unchanged_history_skipped') is not True:
        errors.append('archive completeness/unchanged-history skipping is unverified')
    runs=report.get('runs') or [];peaks=[];times=[]
    if not runs:errors.append('missing measured runs')
    for run in runs:
        wall=run.get('wall_seconds')
        if not number(wall) or wall*1000>target:errors.append('static wall time exceeds target or is missing')
        else:times.append(wall*1000)
        metrics=run.get('health',{}).get('by_interval',{})
        for interval in PERIOD_SECONDS:
            item=metrics.get(interval,{})
            peak=item.get('peak_rss_bytes');duration=item.get('duration_ms')
            if not number(peak,1) or peak>budget:errors.append(interval+': static memory budget/measurement failure')
            else:peaks.append(peak)
            if not number(duration) or duration>target or item.get('circuit',{}).get('blocked'):
                errors.append(interval+': static duration/circuit failure')
        errors.extend(clean_history(run.get('history') or {}))
    return dict(passed=not errors,errors=sorted(set(errors)),signals=expected,
                peak_rss_bytes=max(peaks,default=None),max_wall_ms=max(times,default=None))


def load_acceptance(report,budget,target):
    errors=[];signals=report.get('signals');runs=report.get('runs') or [];peaks=[];times=[]
    if report.get('passed') is False or report.get('reconciled') is not True or not number(signals,1):
        errors.append('load records were not reconciled')
    if not runs:errors.append('missing measured load runs')
    for run in runs:
        if run.get('status')!='SUCCESS':errors.append('load run did not succeed')
        peak=run.get('peak_rss_bytes');duration=run.get('duration_ms');wall=run.get('wall_seconds')
        if not number(peak,1) or peak>budget:errors.append('load memory budget/measurement failure')
        else:peaks.append(peak)
        if not number(duration) or duration>target or not number(wall) or wall*1000>target:
            errors.append('load duration exceeds 180-second target or is missing')
        else:times.append(max(duration,wall*1000))
        books=run.get('books') or []
        if not books or sum(book.get('signal_count',-1) for book in books)!=signals or \
                sum(book.get('counts',{}).get('triggers',-1) for book in books)!=signals:
            errors.append('load workbook signal counts disagree')
        if len(books)==1 and books[0].get('signal_id_sha256')!=report.get('signal_id_sha256'):
            errors.append('load signal digest disagrees')
        if sum(sum(book.get('counts',{}).values()) for book in books)!=report.get('rows'):
            errors.append('load workbook total row count disagrees')
    return dict(passed=not errors,errors=sorted(set(errors)),signals=signals,
                peak_rss_bytes=max(peaks,default=None),max_wall_ms=max(times,default=None))


def writer_evidence_matches(report,candidate):
    required=('excel_builder.mjs','streaming_xlsx.mjs','package.json','package-lock.json')
    try:
        expected={name:hashlib.sha256((Path(candidate)/name).read_bytes()).hexdigest() for name in required}
        runs=report.get('runs') or []
        return bool(runs) and all(run.get('source_unchanged_during_run') is True and
            all(run.get('provenance',{}).get('source_sha256',{}).get(name)==digest
                for name,digest in expected.items()) for run in runs)
    except OSError:
        return False


def check_release(candidate,shadow,static_path,load_paths,*,budget=512*1024*1024,
                  target=180000,max_age_seconds=30,sample_seconds=5,now=None):
    now=time.time_ns()//1_000_000 if now is None else now
    candidate=Path(candidate).resolve(strict=True);shadow=Path(shadow).resolve(strict=True)
    checks=[];evidence={};binding=[]
    def check(name,passed,detail):checks.append(dict(name=name,passed=bool(passed),detail=detail))
    def load(name,path):
        try:
            value=read_json(path)
            evidence[name]=dict(path=str(Path(path).resolve()),sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest())
            return value
        except (OSError,ValueError) as exc:
            check(name,False,str(exc));return {}
    expected_hash=code_hash(candidate)
    report=load('soak_report',shadow/'acceptance/soak-24h-report.json')
    state=load('soak_state',shadow/'acceptance/soak-state.json')
    health=load('health',shadow/'runtime/health.json')
    descriptor=load('shadow_descriptor',shadow/'shadow-run.json')
    check('candidate_code_hash',all(item.get('code_hash')==expected_hash for item in (report,state,health,descriptor)),
          dict(candidate=expected_hash,observed=[item.get('code_hash') for item in (report,state,health,descriptor)]))
    start=state.get('continuous_start_ms');end=state.get('last_sample_ms');created=process_started_ms(health.get('pid'))
    check('real_elapsed_time',number(start,1) and number(end,start) and number(created,1) and
          start>=created-1000 and end<=now+1000 and end-start>=HOURS_REQUIRED and now-created>=HOURS_REQUIRED,
          dict(continuous_start_ms=start,last_sample_ms=end,os_process_started_ms=created,checked_at_ms=now))
    descriptor_start=descriptor.get('started_ms')
    check('producer_identity',health.get('running') is True and number(created,1) and number(descriptor_start,1) and
          created-1000<=descriptor_start<=created+120000 and health.get('pid')==descriptor.get('pid')==state.get('producer_pid') and
          bool(health.get('instance_id')) and health.get('instance_id')==descriptor.get('instance_id')==state.get('producer_instance'),
          dict(pid=health.get('pid'),instance_id=health.get('instance_id'),running=health.get('running')))
    try:
        recomputed=summarize_soak(state,budget,target)
        check('soak_recomputed',report.get('status')=='PASSED' and recomputed['status']=='PASSED' and
              number(report.get('continuous_hours'),24),dict(reported=report.get('status'),recomputed=recomputed))
    except (TypeError,ValueError,KeyError) as exc:check('soak_recomputed',False,str(exc))
    invalid_exports=[]
    for interval,rows in state.get('exports',{}).items():
        for row in rows:
            stamp=row.get('time');boundary=row.get('boundary')
            if (interval not in PERIOD_SECONDS or not number(stamp,1) or stamp>now+1000 or
                not number(row.get('duration_ms'),1) or not number(row.get('peak_rss_bytes'),1) or row['peak_rss_bytes']>budget or
                (row.get('reason') in ('CANDLE_CLOSE','RECOVERY_COMPLETE') and
                 (not number(boundary,1) or boundary%(PERIOD_SECONDS[interval]*1000) or stamp<boundary or
                  row.get('close_latency_ms')!=stamp-boundary))):
                invalid_exports.append(dict(interval=interval,time=stamp,boundary=boundary))
    check('export_event_measurements',not invalid_exports,invalid_exports[:20])
    try:
        raw=raw_observation(shadow/'acceptance/soak-samples.jsonl',state,now,sample_seconds)
        check('raw_observation',raw['passed'],raw)
    except (OSError,ValueError,TypeError,KeyError) as exc:check('raw_observation',False,str(exc))
    times=[health.get('time'),end,(health.get('source_snapshot') or {}).get('completed_ms')]
    check('fresh_health',all(number(stamp,1) and -1000<=now-stamp<=max_age_seconds*1000 for stamp in times) and
          not health.get('shadow_error'),dict(timestamps=times,shadow_error=health.get('shadow_error'),max_age_seconds=max_age_seconds))
    operational=[]
    for interval,seconds in PERIOD_SECONDS.items():
        item=health.get('export_health',{}).get('by_interval',{}).get(interval,{})
        coverage=item.get('coverage') or {};boundary=item.get('report_boundary')
        due=(now-target)//(seconds*1000)*(seconds*1000)
        if (not number(coverage.get('expected'),1) or coverage.get('completed')!=coverage.get('expected') or coverage.get('missing') or
            not number(boundary,due) or boundary>now or item.get('circuit',{}).get('blocked')):
            operational.append(interval+': incomplete, stale close or blocked export')
    check('current_close_coverage',not operational,operational)
    try:
        database=inspect_database(shadow/'runtime/monitor.sqlite3');metadata=database['metadata']
        pending=(metadata.get('shadow_runner_state') or {}).get('pending') or {}
        latest=metadata.get('last_export') or {}
        pending_files=latest.get('pending_replace') or any(entry.get('pending_archive_moves') or entry.get('pending_replace')
            for entry in latest.get('by_interval',{}).values())
        check('no_pending_work',not any(pending.values()) and not database['dirty_periods'] and not pending_files,
              dict(pending=pending,dirty_periods=database['dirty_periods'],pending_files=bool(pending_files)))
        history_errors=clean_history(metadata.get('history_maintenance_health') or {})
        check('history_maintenance',not history_errors,history_errors)
    except (OSError,ValueError,sqlite3.Error,KeyError) as exc:check('shadow_database',False,str(exc))
    static=load('static_acceptance',static_path)
    static_result=static_acceptance(static,budget,target)
    check('static_acceptance_metrics',static_result['passed'],static_result)
    if static.get('code_hash')!=expected_hash:binding.append(str(Path(static_path).resolve()))
    largest=0
    for index,path in enumerate(load_paths):
        result=load('load_acceptance_'+str(index),path);metrics=load_acceptance(result,budget,target)
        check('load_acceptance_metrics_'+str(index),metrics['passed'],metrics)
        if number(metrics.get('signals')):largest=max(largest,metrics['signals'])
        if result.get('code_hash')!=expected_hash and not writer_evidence_matches(result,candidate):
            binding.append(str(Path(path).resolve()))
    check('amplified_load',largest>=max(10000,2*(static_result.get('signals') or 0)),dict(largest_load_signals=largest))
    check('static_evidence_code_binding',not binding,dict(unbound_or_different_code_reports=binding))
    ready=all(item['passed'] for item in checks)
    remaining=[dict(id='FREEZE_AND_LOCK_SHADOW',detail='Stop the shadow producer cleanly and hold its instance lock before final reconciliation.'),
        dict(id='FINAL_CONSISTENT_RECONCILIATION',detail='Under the same lock, reconcile the frozen shadow SQLite signal IDs with every canonical workbook; require zero missing/unexpected/duplicate IDs.'),
        dict(id='CONTROLLED_CUTOVER_AND_VERIFY',detail='Only after final reconciliation, perform a separate controlled cutover and verify production startup, close coverage and rollback availability.')]
    return dict(status='READY_FOR_FINAL_FREEZE' if ready else 'BLOCKED',ready_for_final_freeze=ready,
        ready_for_cutover=False,production_changed=False,checked_at_ms=now,candidate_code_hash=expected_hash,
        checks=checks,remaining_checks=remaining,evidence=evidence,
        blocking_checks=[item['name'] for item in checks if not item['passed']],
        evidence_limit='Local evidence is cross-checked against the OS process age and raw samples; it is not cryptographically attested. No running database/workbook pair is treated as a consistent final reconciliation.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate',default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument('--shadow',required=True)
    parser.add_argument('--static-acceptance',required=True)
    parser.add_argument('--load-acceptance',action='append',required=True)
    parser.add_argument('--memory-mib',type=int,default=512)
    parser.add_argument('--max-health-age-seconds',type=float,default=30)
    parser.add_argument('--sample-seconds',type=float,default=5)
    args=parser.parse_args()
    if args.memory_mib<=0 or not 0<args.max_health_age_seconds<=120 or not 0<args.sample_seconds<=60:
        parser.error('Invalid memory budget or evidence freshness/cadence')
    try:
        result=check_release(args.candidate,args.shadow,args.static_acceptance,args.load_acceptance,
            budget=args.memory_mib*1024*1024,max_age_seconds=args.max_health_age_seconds,sample_seconds=args.sample_seconds)
    except (OSError,ValueError,TypeError,KeyError) as exc:
        result=dict(status='BLOCKED',ready_for_final_freeze=False,ready_for_cutover=False,production_changed=False,error=str(exc))
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0 if result.get('ready_for_final_freeze') else 2


if __name__=='__main__':
    raise SystemExit(main())
