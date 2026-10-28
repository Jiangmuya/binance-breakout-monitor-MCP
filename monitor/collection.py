"""Bounded, close-aligned WebSocket collection without changing candle evaluation."""
import asyncio
from dataclasses import dataclass, field
from .core import PERIODS, dumps
from .close_applicability import close_applicable

LEAD_MS = {'15m': 2*60000, '1h': 5*60000, '4h': 10*60000, '1d': 15*60000}
CLOSE_GRACE_MS = 120000
POLL_SECONDS = 1.


def close_window(boundary):
    """All intervals closing together use their longest pre-close lead."""
    if boundary % PERIODS['15m']:
        raise ValueError('Collection close must align to 15 minutes')
    intervals = tuple(i for i, step in PERIODS.items() if boundary % step == 0)
    lead = max(LEAD_MS[i] for i in intervals)
    return dict(boundary=boundary, start=boundary-lead,
                deadline=boundary+CLOSE_GRACE_MS, intervals=intervals)


def active_windows(stamp):
    """At most the just-closed batch and the next batch can overlap."""
    current = stamp // PERIODS['15m'] * PERIODS['15m']
    return [window for boundary in (current, current+PERIODS['15m'])
            if (window := close_window(boundary))['start'] <= stamp < window['deadline']]


def next_window_start(stamp):
    current = stamp // PERIODS['15m'] * PERIODS['15m']
    return min(window['start'] for n in range(1, 4)
               if (window := close_window(current+n*PERIODS['15m']))['start'] > stamp)


def window_snapshot(store, window, stamp):
    """Only current universe and the exact close are queried, never all history."""
    from .export import close_coverage
    boundary = window['boundary']
    with store.lock:
        universe = store.universe()
        expected = {(symbol, interval) for symbol, info in universe.items()
                    if close_applicable(info, boundary) for interval in window['intervals']}
        missing = set(expected)
        if stamp >= boundary:
            missing = set()
            for interval in window['intervals']:
                coverage = close_coverage(store, interval, boundary)
                missing.update((symbol, interval) for symbol in coverage['missing'])
    return expected, missing


@dataclass
class Batch:
    window: dict
    streams: set = field(default_factory=set)
    jobs: dict = field(default_factory=dict)
    sequence: int = 0
    expected: int = 0
    missing: int = 0


class CollectionWindows:
    def __init__(self, monitor):
        self.monitor = monitor
        self.batches = {}
        self.finished = set()
        self.lock = asyncio.Lock()
        self.last_error = None
        self.report = dict(mode='CLOSE_WINDOWS', state='INITIALIZING', active_windows=[])

    def _sync_tasks(self):
        self.monitor.ws_tasks = [job[1] for batch in self.batches.values() for job in batch.jobs.values()]

    async def _end(self, boundary):
        batch = self.batches.pop(boundary)
        batch.streams.clear()
        for _, task in batch.jobs.values():
            task.cancel()
        await asyncio.gather(*(task for _, task in batch.jobs.values()), return_exceptions=True)
        for key in batch.jobs:
            self.monitor.client.connections.pop(key, None)
        self.finished.add(boundary)
        self._sync_tasks()

    async def refresh(self):
        async with self.lock:
            if self.monitor.stop.is_set():
                return
            stamp = self.monitor.client.time()
            current = stamp // PERIODS['15m'] * PERIODS['15m']
            self.finished = {boundary for boundary in self.finished if boundary >= current-PERIODS['15m']}
            for window in active_windows(stamp):
                boundary = window['boundary']
                if boundary not in self.finished and boundary not in self.batches:
                    self.batches[boundary] = Batch(window)
            # Start a newly overlapping window before waiting for an older socket to close.
            for boundary, batch in sorted(self.batches.items(), reverse=True):
                expected, missing = await asyncio.to_thread(window_snapshot, self.monitor.store, batch.window, stamp)
                batch.expected, batch.missing = len(expected), len(missing)
                if stamp >= boundary and not missing:
                    await self._end(boundary)
                    continue
                if stamp >= batch.window['deadline']:
                    for pair in missing:
                        self.monitor.repair(*pair)
                    await self._end(boundary)
                    continue
                # A backward server-clock correction must not keep a future window open.
                if stamp < batch.window['start']:
                    await self._end(boundary)
                    self.finished.discard(boundary)
                    continue
                wanted = {f'{symbol.lower()}@kline_{interval}' for symbol, interval in expected}
                batch.streams = wanted
                represented = set()
                for key, (streams, task) in list(batch.jobs.items()):
                    if task.done():
                        # Retrieve exceptions so an unexpected task failure is not silent.
                        if not task.cancelled() and task.exception() is not None:
                            self.monitor.log.error(dumps(dict(event='COLLECTION_STREAM_ERROR',
                                boundary=boundary, error=type(task.exception()).__name__)))
                        batch.jobs.pop(key)
                        self.monitor.client.connections.pop(key, None)
                    elif not (set(streams) & wanted):
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        batch.jobs.pop(key)
                        self.monitor.client.connections.pop(key, None)
                    else:
                        represented.update(streams)
                added = sorted(wanted-represented)
                size = self.monitor.config['streams_per_connection']
                for offset in range(0, len(added), size):
                    streams = added[offset:offset+size]
                    key = f'w{boundary}s{batch.sequence}'
                    batch.sequence += 1
                    task = asyncio.create_task(self.monitor.client.stream(key, streams, self.monitor.queue,
                        self.monitor.repair, self.monitor.store, self.monitor.stop,
                        active_streams=lambda batch=batch: batch.streams, target_close=boundary))
                    batch.jobs[key] = (streams, task)
            self._sync_tasks()
            windows = [dict(batch.window, expected=batch.expected, processed=batch.expected-batch.missing,
                            missing=batch.missing,
                            phase='COLLECTING' if stamp < boundary else 'WAITING_COMMIT')
                       for boundary, batch in sorted(self.batches.items())]
            self.report = dict(mode='CLOSE_WINDOWS', state='ACTIVE' if windows else 'IDLE',
                active_windows=windows, next_start_ms=next_window_start(stamp),
                grace_seconds=CLOSE_GRACE_MS//1000, lead_seconds={i:v//1000 for i,v in LEAD_MS.items()})
            self.monitor.console_stage = '收盘窗口采集中' if windows else '等待下一采集窗口（正常待机）'
            self.last_error = None

    async def loop(self):
        while not self.monitor.stop.is_set():
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = type(exc).__name__
                self.report = {**self.report, 'state':'ERROR', 'error':error}
                if error != self.last_error:
                    self.monitor.log.error(dumps(dict(event='COLLECTION_SCHEDULE_ERROR', error=error)))
                    await asyncio.to_thread(self.monitor.store.anomaly, '', '', 'COLLECTION_SCHEDULE_ERROR', error)
                    self.last_error = error
            try:
                await asyncio.wait_for(self.monitor.stop.wait(), POLL_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def close(self):
        async with self.lock:
            for boundary in list(self.batches):
                await self._end(boundary)
            self.report = {**self.report, 'state':'STOPPED', 'active_windows':[]}
