"""Report coverage eligibility; this never changes quantitative signal rules."""
import math
import json
from .core import PERIODS


def close_applicable(info, boundary):
    """A contract listed at/after the close cannot have that closed candle.

    Unknown or invalid listing times remain in the expected set so missing
    metadata never hides an actual data gap. The first partially traded candle
    remains applicable when its close is later than the listing timestamp.
    """
    onboard = info.get('onboardDate')
    if isinstance(onboard, bool) or not isinstance(onboard, (int, float)) or onboard <= 0:
        return True
    if isinstance(onboard, float) and not math.isfinite(onboard):
        return True
    return onboard < boundary


def valid_exact_evaluation(payload, version, symbol, interval, target):
    """Validate persisted proof of processing, without recalculating strategy."""
    if not isinstance(payload, dict):
        return False
    if not isinstance(version,str) or not version or any(payload.get(k)!=v for k,v in
            (('strategy_version',version),('symbol',symbol),('interval',interval))):
        return False
    bar=payload.get('bar'); metrics=payload.get('metrics'); ema=payload.get('ema')
    if not isinstance(bar,dict) or type(bar.get('t')) is not int or bar['t']!=target:
        return False
    if type(bar.get('T')) is not int or bar['T']!=target+PERIODS[interval]-1:
        return False
    def number(value):
        return not isinstance(value,bool) and isinstance(value,(int,float)) and math.isfinite(value)
    if not all(number(bar.get(k)) for k in ('o','h','l','c','v','q')):
        return False
    if not isinstance(metrics,dict) or not isinstance(metrics.get('checks'),list) or len(metrics['checks'])!=6:
        return False
    if not isinstance(ema,list) or len(ema)!=3 or not all(number(x) for x in ema):
        return False
    if not all(k in payload for k in ('readiness','contract_type','seed','count','discovered','processed')):
        return False
    return all(number(payload[k]) for k in ('count','discovered','processed'))


def load_exact_evaluations(db, interval, target):
    """Read one indexed close; corrupt/mismatched evidence is never a proof."""
    result={}
    cursor=db.execute("""SELECT e.version,e.symbol,e.payload FROM evaluations e
        JOIN universe u ON u.symbol=e.symbol LEFT JOIN versions v ON v.version=e.version
        WHERE e.interval=? AND e.t=? AND u.active=1 ORDER BY v.created,e.version""",(interval,target))
    for row in cursor:
        try:
            payload=json.loads(row['payload'])
        except (ValueError,TypeError):
            continue
        if valid_exact_evaluation(payload,row['version'],row['symbol'],interval,target):
            result[row['symbol']]=payload
    return result


def close_processed(last, evaluated, health, target, exact_proof=False):
    """A saved exact evaluation proves processing, not resolution of later errors."""
    return bool(exact_proof or (last is not None and last>=target and evaluated==target and health=='OK'))
