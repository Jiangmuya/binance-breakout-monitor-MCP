"""User-approved 24-hour policy over unchanged continuous-run evidence.

The producer remains frozen so already collected evidence stays valid. Only the
required elapsed duration differs from export_soak.summarize_soak; the checks for
coverage, latency, memory and restart continuity are identical.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from export_soak import PERIOD_SECONDS
from monitor.maintenance import _write_json

def summarize_soak(state, budget_bytes=512 * 1024 * 1024, target_ms=180_000):
    elapsed = max(0, state.get('last_sample_ms', 0) - state.get('continuous_start_ms', 0))
    enough_time = elapsed >= 24 * 3600_000
    result = {'status': 'OBSERVING', 'continuous_hours': elapsed / 3600_000,
              'required_hours': 24, 'memory_budget_bytes': budget_bytes,
              'close_target_ms': target_ms, 'samples': state.get('samples', 0),
              'code_hash':state.get('code_hash'),
              'producer_rss_bytes': state.get('producer_rss_bytes'), 'by_interval': {},
              'interruptions': state.get('interruptions', []), 'failures': state.get('failures', [])}
    samples = [x['rss_bytes'] for x in state.get('producer_samples', [])
               if isinstance(x.get('rss_bytes'), (int, float))]
    producer_trend = {}
    if len(samples) >= 12:
        width = max(3, len(samples) // 6)
        first, last = statistics.median(samples[:width]), statistics.median(samples[-width:])
        producer_trend = dict(first_median=first, last_median=last, growth_ratio=(last-first)/max(1, first))
    result['producer_memory'] = dict(peak_rss_bytes=max(samples, default=None), trend=producer_trend,
        metrics_complete=bool(samples) and len(samples) == len(state.get('producer_samples', [])))
    failures = bool(state.get('failures')) or (bool(samples) and max(samples) > budget_bytes)
    failures |= producer_trend.get('growth_ratio', 0) > .30
    enough_exports = True
    for interval, seconds in PERIOD_SECONDS.items():
        rows = state.get('exports', {}).get(interval, [])
        peaks = [x['peak_rss_bytes'] for x in rows if isinstance(x.get('peak_rss_bytes'), (int, float))]
        durations = [x['duration_ms'] for x in rows if isinstance(x.get('duration_ms'), (int, float))]
        # A permitted provisional workbook must not fail the gate after the
        # same close is repaired. Count distinct closes, never retry attempts.
        batches = {}
        for row in rows:
            if row.get('boundary') and row.get('reason') in ('CANDLE_CLOSE', 'RECOVERY_COMPLETE'):
                previous = batches.get(row['boundary'])
                coverage = (previous or {}).get('coverage') or {}
                previously_complete = coverage.get('expected',0)>0 and coverage.get('expected')==coverage.get('completed')
                # Deadline means the first complete publication. A later
                # harmless re-publication must not turn an on-time close late.
                if not previously_complete:
                    batches[row['boundary']] = row
        settled = [row for row in batches.values() if
                   row['boundary'] <= state.get('last_sample_ms', 0) - target_ms]
        latency = [x['close_latency_ms'] for x in settled if isinstance(x.get('close_latency_ms'), (int, float))]
        required = max(0, (state.get('last_sample_ms', 0)-target_ms)//(seconds*1000) -
                       state.get('continuous_start_ms', 0)//(seconds*1000))
        observed_required = sum(state.get('continuous_start_ms', 0) < boundary <=
            state.get('last_sample_ms', 0)-target_ms for boundary in batches)
        measurable = len(peaks) == len(rows) == len(durations) and bool(rows)
        enough_exports &= observed_required >= required and measurable
        over_budget = bool(peaks and max(peaks) > budget_bytes)
        late = sum(x > target_ms for x in latency)
        incomplete = sum((x.get('coverage') or {}).get('completed', -1) != (x.get('coverage') or {}).get('expected', -2) for x in settled)
        trend = {}
        if len(rows) >= 12 and durations and peaks:
            width = max(3, len(rows) // 6)
            for field in ('duration_ms', 'peak_rss_bytes'):
                first = [x[field] for x in rows[:width] if isinstance(x.get(field), (int, float))]
                last = [x[field] for x in rows[-width:] if isinstance(x.get(field), (int, float))]
                if first and last:
                    baseline = statistics.median(first)
                    change = statistics.median(last) - baseline
                    trend[field] = {'first_median': baseline, 'last_median': statistics.median(last),
                                    'growth_ratio': change / max(1, baseline)}
        growing = any(x['growth_ratio'] > .30 for x in trend.values())
        failures |= over_budget or late > 0 or incomplete > 0 or growing
        result['by_interval'][interval] = {'exports_observed': len(rows), 'distinct_closes': len(batches),
            'required_closes': required,
            'metrics_complete': measurable, 'peak_rss_bytes': max(peaks, default=None),
            'max_duration_ms': max(durations, default=None), 'late_exports': late,
            'incomplete_exports': incomplete, 'trend': trend, 'growth_review_required': growing}
    if enough_time:
        result['status'] = 'FAILED' if failures else ('PASSED' if enough_exports and result['producer_memory']['metrics_complete'] and state.get('code_hash') else 'INSUFFICIENT_EXPORT_EVIDENCE')
    elif failures:
        result['status'] = 'OBSERVING_WITH_FAILURES'
    return result


def assess(shadow, now=None):
    shadow=Path(shadow).resolve()
    now=time.time_ns()//1000000 if now is None else now
    state_path=shadow/'acceptance/soak-state.json'
    state=json.loads(state_path.read_text(encoding='utf-8'))
    health=json.loads((shadow/'runtime/health.json').read_text(encoding='utf-8'))
    descriptor=json.loads((shadow/'shadow-run.json').read_text(encoding='utf-8'))
    report=summarize_soak(state)
    report.update(policy='USER_APPROVED_24_HOUR',assessed_at_ms=now,
        continuous_start_ms=state.get('continuous_start_ms'),last_sample_ms=state.get('last_sample_ms'),
        producer_original_required_hours=72,producer_original_planned_hours=descriptor.get('requested_hours'),
        evidence_state_sha256=hashlib.sha256(state_path.read_bytes()).hexdigest())
    start=state.get('continuous_start_ms',now);end=state.get('last_sample_ms',now)
    consistent=state.get('code_hash')==health.get('code_hash')==descriptor.get('code_hash') and bool(state.get('code_hash'))
    fresh=0<=now-health.get('time',0)<=120000
    wall_complete=now-start>=24*3600000
    report['evidence_fresh']=fresh
    report['producer_running']=health.get('running')
    report['wall_clock_complete']=wall_complete
    if not consistent or end>now+1000 or start>now+1000:
        report['status']='INVALID_EVIDENCE'
    elif not fresh:
        report['status']='STALE_EVIDENCE'
    elif health.get('shadow_error'):
        report['status']='FAILED'
    elif not health.get('running') and not (shadow/'STOP').exists():
        report['status']='INTERRUPTED'
    if report['status']=='PASSED' and not wall_complete:
        report['status']='OBSERVING'
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shadow',required=True)
    parser.add_argument('--watch',action='store_true')
    parser.add_argument('--poll-seconds',type=float,default=30)
    args=parser.parse_args()
    if args.poll_seconds<=0:parser.error('poll interval must be positive')
    shadow=Path(args.shadow).resolve(strict=True)
    output=shadow/'acceptance/soak-24h-report.json'
    while True:
        report=assess(shadow)
        _write_json(output,report)
        if args.watch and report['continuous_hours']>=24 and report['wall_clock_complete']:
            # Stops only this disposable shadow test, never the production monitor.
            (shadow/'STOP').touch(exist_ok=True)
            report['shadow_stop_requested_ms']=time.time_ns()//1000000
            _write_json(output,report)
            print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
            return
        if not args.watch or report['status'] in ('INVALID_EVIDENCE','STALE_EVIDENCE','INTERRUPTED'):
            print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
            return
        time.sleep(args.poll_seconds)

if __name__=='__main__':main()
