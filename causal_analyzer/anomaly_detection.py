"""
异常检测模块 - 检测因果对置信度的突变
算法：
  1. CUSUM (Cumulative Sum) - 检测均值偏移
  2. 滑动窗口比较 - 检测短期 vs 长期的变化
触发条件：
  - 置信度分数从正常区间 (<50%) 突然跃升至高置信区间 (>80%)
  - 变化幅度 > 阈值（默认 50 分）
  - 连续 N 个采样点都高于阈值（避免单点噪声）
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Set, Callable, Any
from collections import deque


@dataclass
class ConfidenceSample:
    timestamp: float
    score: float
    co_occurrence: int


@dataclass
class AnomalyAlert:
    alert_id: str
    timestamp: float
    cause: str
    effect: str
    old_score: float
    new_score: float
    change_magnitude: float
    alert_type: str
    details: dict
    message: str


@dataclass
class PairMonitorState:
    cause: str
    effect: str
    samples: deque = field(default_factory=lambda: deque(maxlen=120))
    baseline_mean: float = 0.0
    baseline_std: float = 0.0
    baseline_window: deque = field(default_factory=lambda: deque(maxlen=60))
    cusum_pos: float = 0.0
    cusum_neg: float = 0.0
    last_alert_ts: float = 0.0
    consec_breaches: int = 0


class AnomalyDetector:
    def __init__(
        self,
        sample_interval: float = 5.0,
        min_samples_for_baseline: int = 10,
        change_threshold: float = 50.0,
        min_rising_score: float = 80.0,
        alert_cooldown: float = 60.0,
        consec_breaches_required: int = 3,
        cusum_threshold: float = 30.0,
    ):
        self.sample_interval = sample_interval
        self.min_samples_for_baseline = min_samples_for_baseline
        self.change_threshold = change_threshold
        self.min_rising_score = min_rising_score
        self.alert_cooldown = alert_cooldown
        self.consec_breaches_required = consec_breaches_required
        self.cusum_threshold = cusum_threshold

        self._monitors: Dict[Tuple[str, str], PairMonitorState] = {}
        self._alerts: deque = deque(maxlen=1000)
        self._alert_callbacks: List[Callable[[AnomalyAlert], None]] = []

        self._watch_pairs: Set[Tuple[str, str]] = set()
        self._lock = asyncio.Lock()
        self._running = False
        self._sample_task: Optional[asyncio.Task] = None
        self._engine = None

    async def start(self, engine) -> None:
        self._engine = engine
        self._running = True
        self._sample_task = asyncio.create_task(self._sampling_loop())

    async def stop(self) -> None:
        self._running = False
        if self._sample_task:
            self._sample_task.cancel()
            try:
                await self._sample_task
            except asyncio.CancelledError:
                pass
            self._sample_task = None

    def register_alert_callback(self, callback: Callable[[AnomalyAlert], None]) -> None:
        self._alert_callbacks.append(callback)

    async def watch_pair(self, cause: str, effect: str) -> None:
        async with self._lock:
            pair = (cause, effect)
            if pair not in self._watch_pairs:
                self._watch_pairs.add(pair)
                if pair not in self._monitors:
                    self._monitors[pair] = PairMonitorState(cause=cause, effect=effect)

    async def unwatch_pair(self, cause: str, effect: str) -> None:
        async with self._lock:
            pair = (cause, effect)
            self._watch_pairs.discard(pair)

    def get_watched_pairs(self) -> List[Tuple[str, str]]:
        return list(self._watch_pairs)

    async def _sampling_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.sample_interval)
                await self._sample_all_pairs()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[anomaly] sampling error: {e}", flush=True)

    async def _sample_all_pairs(self) -> None:
        pairs = list(self._watch_pairs)
        ts = time.time()
        for cause, effect in pairs:
            try:
                result = await self._engine.infer(cause, effect)
                await self._process_sample(cause, effect, result, ts)
            except Exception as e:
                print(f"[anomaly] sample error for {cause}->{effect}: {e}", flush=True)

    async def _process_sample(
        self,
        cause: str,
        effect: str,
        result,
        timestamp: float,
    ) -> None:
        async with self._lock:
            pair = (cause, effect)
            monitor = self._monitors.get(pair)
            if not monitor:
                return

            sample = ConfidenceSample(
                timestamp=timestamp,
                score=result.confidence_score,
                co_occurrence=result.co_occurrence_traces,
            )
            monitor.samples.append(sample)

            if len(monitor.baseline_window) < self.min_samples_for_baseline:
                monitor.baseline_window.append(sample.score)
            else:
                scores = list(monitor.baseline_window)
                monitor.baseline_mean = sum(scores) / len(scores)
                if len(scores) >= 2:
                    var = sum((s - monitor.baseline_mean) ** 2 for s in scores) / len(scores)
                    monitor.baseline_std = var ** 0.5
                else:
                    monitor.baseline_std = 0.0

                monitor.baseline_window.append(sample.score)

            if len(monitor.baseline_window) >= self.min_samples_for_baseline:
                alert = self._detect_anomaly(monitor, sample)
                if alert:
                    self._alerts.append(alert)
                    for cb in self._alert_callbacks:
                        try:
                            cb(alert)
                        except Exception as e:
                            print(f"[anomaly] callback error: {e}", flush=True)

    def _detect_anomaly(
        self,
        monitor: PairMonitorState,
        sample: ConfidenceSample,
    ) -> Optional[AnomalyAlert]:
        now = time.time()
        if now - monitor.last_alert_ts < self.alert_cooldown:
            return None

        delta = sample.score - monitor.baseline_mean

        k = max(5.0, monitor.baseline_std * 1.5)
        self._update_cusum(monitor, delta, k)

        old_score = monitor.baseline_mean
        new_score = sample.score
        change_magnitude = abs(new_score - old_score)

        is_spike_up = (
            old_score < (self.min_rising_score - 10)
            and new_score >= self.min_rising_score
            and change_magnitude >= self.change_threshold
        )

        is_cusum_breach = (
            monitor.cusum_pos >= self.cusum_threshold
            and new_score >= self.min_rising_score
        )

        if is_spike_up or is_cusum_breach:
            monitor.consec_breaches += 1
        else:
            monitor.consec_breaches = max(0, monitor.consec_breaches - 1)

        if monitor.consec_breaches >= self.consec_breaches_required:
            alert_type = "spike" if is_spike_up else "cusum"
            alert_id = f"alert_{int(now)}_{hash((monitor.cause, monitor.effect, now)) % 1000000}"

            if is_spike_up:
                message = (
                    f"因果置信度突变告警：「{monitor.cause}」→「{monitor.effect}」"
                    f"的置信度从 {old_score:.1f}% 跃升至 {new_score:.1f}%，"
                    f"变化幅度 {change_magnitude:.1f} 分，可能是新型异常模式出现。"
                )
            else:
                message = (
                    f"因果置信度持续偏高告警：「{monitor.cause}」→「{monitor.effect}」"
                    f"的置信度持续 {monitor.consec_breaches} 个采样周期处于高位，"
                    f"当前 {new_score:.1f}%（基线 {old_score:.1f}%），疑似出现持续性问题。"
                )

            alert = AnomalyAlert(
                alert_id=alert_id,
                timestamp=now,
                cause=monitor.cause,
                effect=monitor.effect,
                old_score=round(old_score, 1),
                new_score=round(new_score, 1),
                change_magnitude=round(change_magnitude, 1),
                alert_type=alert_type,
                details={
                    "baseline_mean": round(monitor.baseline_mean, 1),
                    "baseline_std": round(monitor.baseline_std, 1),
                    "cusum_pos": round(monitor.cusum_pos, 1),
                    "cusum_neg": round(monitor.cusum_neg, 1),
                    "consec_breaches": monitor.consec_breaches,
                    "current_co_occurrence": sample.co_occurrence,
                },
                message=message,
            )

            monitor.last_alert_ts = now
            monitor.consec_breaches = 0
            monitor.cusum_pos = 0.0
            monitor.cusum_neg = 0.0
            return alert

        return None

    def _update_cusum(
        self,
        monitor: PairMonitorState,
        delta: float,
        k: float,
    ) -> None:
        monitor.cusum_pos = max(0.0, monitor.cusum_pos + (delta - k))
        monitor.cusum_neg = max(0.0, monitor.cusum_neg - (delta + k))

    async def get_recent_alerts(
        self,
        limit: int = 50,
        since: Optional[float] = None,
    ) -> List[AnomalyAlert]:
        async with self._lock:
            alerts = list(self._alerts)
            if since is not None:
                alerts = [a for a in alerts if a.timestamp >= since]
            return alerts[-limit:]

    async def get_pair_state(
        self,
        cause: str,
        effect: str,
    ) -> Optional[dict]:
        async with self._lock:
            pair = (cause, effect)
            monitor = self._monitors.get(pair)
            if not monitor:
                return None
            recent = list(monitor.samples)[-10:]
            return {
                "cause": cause,
                "effect": effect,
                "watched": pair in self._watch_pairs,
                "samples_collected": len(monitor.samples),
                "baseline_mean": round(monitor.baseline_mean, 1),
                "baseline_std": round(monitor.baseline_std, 1),
                "cusum_pos": round(monitor.cusum_pos, 1),
                "cusum_neg": round(monitor.cusum_neg, 1),
                "consec_breaches": monitor.consec_breaches,
                "recent_scores": [round(s.score, 1) for s in recent],
                "recent_timestamps": [s.timestamp for s in recent],
            }
