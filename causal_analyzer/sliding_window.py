"""
滑动时间窗口模块 - 用于实时模式下维护最近 N 分钟的数据
支持：
  - 事件按时间戳自动过期清理
  - 增量更新计数（不需要重放全部历史）
  - trace 级别分组（事件到达时按 trace 聚合）
"""

import time
import asyncio
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Set, Deque
from dataclasses import dataclass, field


@dataclass
class WindowEvent:
    timestamp: float
    trace_id: str
    event_key: str
    raw: dict


@dataclass
class TraceState:
    trace_id: str
    first_seen: float
    last_seen: float
    events: List[WindowEvent] = field(default_factory=list)
    completed: bool = False


class SlidingTimeWindow:
    def __init__(
        self,
        window_seconds: int = 300,
        cleanup_interval: float = 5.0,
        trace_timeout: float = 30.0,
    ):
        self.window_seconds = window_seconds
        self.cleanup_interval = cleanup_interval
        self.trace_timeout = trace_timeout

        self._traces: Dict[str, TraceState] = {}
        self._trace_order: Deque[Tuple[float, str]] = deque()

        self._event_count: Dict[str, int] = defaultdict(int)
        self._trace_event_count: Dict[str, Set[str]] = defaultdict(set)

        self._pair_cooccurrence: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        self._pair_intervals: Dict[Tuple[str, str], List[Tuple[float, str]]] = defaultdict(list)

        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        self._running = False
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    async def _cleanup_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[window] cleanup error: {e}", flush=True)

    async def _cleanup_expired(self) -> None:
        now = time.time()
        cutoff = now - self.window_seconds
        trace_cutoff = now - self.trace_timeout

        expired_trace_ids: Set[str] = set()
        completed_traces: Set[str] = set()

        async with self._lock:
            while self._trace_order and self._trace_order[0][0] < cutoff:
                _, trace_id = self._trace_order.popleft()
                expired_trace_ids.add(trace_id)

            for trace_id, state in list(self._traces.items()):
                if trace_id in expired_trace_ids:
                    self._remove_trace(trace_id)
                elif state.last_seen < trace_cutoff and not state.completed:
                    state.completed = True
                    completed_traces.add(trace_id)
                    self._process_completed_trace(state)

        if expired_trace_ids:
            print(f"[window] expired {len(expired_trace_ids)} traces, cutoff={cutoff:.1f}", flush=True)

    async def add_event(self, raw_entry: dict) -> Optional[TraceState]:
        trace_id = str(raw_entry.get("trace_id", ""))
        if not trace_id:
            return None

        ev_type = str(raw_entry.get("event_type", ""))
        msg = str(raw_entry.get("message", ""))
        event_key = f"{ev_type}: {msg}"
        ts = float(raw_entry.get("timestamp", time.time()))

        async with self._lock:
            if trace_id not in self._traces:
                state = TraceState(
                    trace_id=trace_id,
                    first_seen=ts,
                    last_seen=ts,
                )
                self._traces[trace_id] = state
                self._trace_order.append((ts, trace_id))
            else:
                state = self._traces[trace_id]
                state.last_seen = ts

            event = WindowEvent(
                timestamp=ts,
                trace_id=trace_id,
                event_key=event_key,
                raw=raw_entry,
            )
            state.events.append(event)

            self._event_count[event_key] += 1
            self._trace_event_count[event_key].add(trace_id)

            self._update_pair_counts(trace_id, state.events)

            now = time.time()
            if state.last_seen < now - self.trace_timeout and not state.completed:
                state.completed = True
                self._process_completed_trace(state)

        return state

    def _update_pair_counts(self, trace_id: str, events: List[WindowEvent]) -> None:
        if len(events) < 2:
            return

        last_event = events[-1]
        for prev in events[:-1]:
            if prev.timestamp <= last_event.timestamp:
                pair = (prev.event_key, last_event.event_key)
                self._pair_cooccurrence[pair].add(trace_id)

                if prev.event_key != last_event.event_key:
                    interval = (last_event.timestamp - prev.timestamp) * 1000.0
                    self._pair_intervals[pair].append((interval, trace_id))

    def _process_completed_trace(self, state: TraceState) -> None:
        pass

    def _remove_trace(self, trace_id: str) -> None:
        state = self._traces.pop(trace_id, None)
        if not state:
            return

        event_keys_seen: Set[str] = set()
        for ev in state.events:
            event_keys_seen.add(ev.event_key)
            self._event_count[ev.event_key] = max(0, self._event_count[ev.event_key] - 1)

        for ek in event_keys_seen:
            if trace_id in self._trace_event_count[ek]:
                self._trace_event_count[ek].discard(trace_id)
                if not self._trace_event_count[ek]:
                    del self._trace_event_count[ek]

        pairs_to_update: Set[Tuple[str, str]] = set()
        n = len(state.events)
        for i in range(n):
            for j in range(i + 1, n):
                pair = (state.events[i].event_key, state.events[j].event_key)
                pairs_to_update.add(pair)

        for pair in pairs_to_update:
            if trace_id in self._pair_cooccurrence[pair]:
                self._pair_cooccurrence[pair].discard(trace_id)
                if not self._pair_cooccurrence[pair]:
                    del self._pair_cooccurrence[pair]

            if pair in self._pair_intervals:
                self._pair_intervals[pair] = [
                    (t, tr) for t, tr in self._pair_intervals[pair] if tr != trace_id
                ]
                if not self._pair_intervals[pair]:
                    del self._pair_intervals[pair]

    async def get_stats(self) -> dict:
        async with self._lock:
            return {
                "window_seconds": self.window_seconds,
                "total_traces": len(self._traces),
                "total_events": sum(self._event_count.values()),
                "unique_event_types": len(self._event_count),
                "pair_count": len(self._pair_cooccurrence),
            }

    async def get_causal_pair_stats(self, cause: str, effect: str) -> Optional[dict]:
        pair = (cause, effect)
        async with self._lock:
            total_traces = len(self._traces)
            cause_traces = len(self._trace_event_count.get(cause, set()))
            effect_traces = len(self._trace_event_count.get(effect, set()))
            both_traces = len(self._pair_cooccurrence.get(pair, set()))

            intervals = [t for t, _ in self._pair_intervals.get(pair, [])]
            avg_interval = sum(intervals) / len(intervals) if intervals else 0.0
            std_interval = 0.0
            if len(intervals) >= 2:
                mean = avg_interval
                var = sum((x - mean) ** 2 for x in intervals) / len(intervals)
                std_interval = var ** 0.5

        return {
            "cause": cause,
            "effect": effect,
            "total_traces": total_traces,
            "cause_traces": cause_traces,
            "effect_traces": effect_traces,
            "co_occurrence_traces": both_traces,
            "avg_interval_ms": avg_interval,
            "std_interval_ms": std_interval,
            "intervals_count": len(intervals),
        }

    async def get_active_trace_ids(self) -> List[str]:
        async with self._lock:
            return list(self._traces.keys())

    def __len__(self) -> int:
        return len(self._traces)
