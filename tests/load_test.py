"""Synthetic load benchmark only; never writes to the production sample database."""
import json
import tempfile
import time
from pathlib import Path
from test_monitor import CONFIG,info,bars,signal_bar
from monitor.core import Store,PERIODS

with tempfile.TemporaryDirectory() as directory:
    config=dict(CONFIG,data_dir=directory)
    store=Store(Path(directory)/'synthetic.sqlite3',config)
    symbols=[f'TEST{i}USDT' for i in range(750)]
    store.refresh({'symbols':[info(s) for s in symbols]})
    for s in symbols:
        for interval in PERIODS: store.ingest(s,interval,bars(200,interval),'HISTORICAL')
    started=time.perf_counter()
    for s in symbols:
        for interval in PERIODS:
            b=signal_bar(bars(200,interval)[-1],interval)
            store.ingest(s,interval,[b],'REALTIME',b[6]+10000)
    elapsed=time.perf_counter()-started
    count=store.db.execute('SELECT count(*) FROM signals').fetchone()[0]
    print(json.dumps(dict(dataset='SYNTHETIC_TEST_ONLY',simultaneous_closes=3000,signals=count,
                         seconds=elapsed,events_per_second=3000/elapsed,logical_events_unique=count==3000)))
    assert count==3000
    store.close()
