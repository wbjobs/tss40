"""
流式增量因果引擎
 - 基于滑动时间窗口，逐事件增量更新
 - 不加载全部历史，只维护窗口内的统计量
 - 置信度计算沿用批处理公式，但数据源来自滑动窗口的增量统计
"""

import math
import asyncio
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict

from .models import CausalInferenceResult
from .sliding_window import SlidingTimeWindow


class StreamingIncrementalEngine:
    def __init__(
        self,
        window_seconds: int = 300,
        min_support: int = 2,
        cleanup_interval: float = 5.0,
    ):
        self.window = SlidingTimeWindow(
            window_seconds=window_seconds,
            cleanup_interval=cleanup_interval,
        )
        self.min_support = min_support

        self._pattern_sketch = None
        self._confidence_cache: Dict[Tuple[str, str], CausalInferenceResult] = {}
        self._cache_lock = asyncio.Lock()

        self._listeners: Dict[str, List] = defaultdict(list)

    async def start(self) -> None:
        await self.window.start()

    async def stop(self) -> None:
        await self.window.stop()

    async def ingest(self, entry: dict) -> None:
        await self.window.add_event(entry)
        async with self._cache_lock:
            self._confidence_cache.clear()

    async def ingest_batch(self, entries: List[dict]) -> None:
        for e in entries:
            await self.window.add_event(e)
        async with self._cache_lock:
            self._confidence_cache.clear()

    async def get_stats(self) -> dict:
        wstats = await self.window.get_stats()
        return {
            "mode": "streaming-incremental",
            "window_seconds": wstats["window_seconds"],
            "active_traces": wstats["total_traces"],
            "events_in_window": wstats["total_events"],
            "unique_event_types": wstats["unique_event_types"],
            "cached_pairs": len(self._confidence_cache),
        }

    async def infer(self, cause: str, effect: str) -> CausalInferenceResult:
        cache_key = (cause, effect)
        async with self._cache_lock:
            if cache_key in self._confidence_cache:
                return self._confidence_cache[cache_key]

        stats = await self.window.get_causal_pair_stats(cause, effect)
        if stats is None:
            result = self._build_empty_result(cause, effect, "无法获取统计数据")
            async with self._cache_lock:
                self._confidence_cache[cache_key] = result
            return result

        total = max(1, stats["total_traces"])
        cause_count = stats["cause_traces"]
        effect_count = stats["effect_traces"]
        co_occurrence = stats["co_occurrence_traces"]
        intervals_count = stats["intervals_count"]

        avg_interval_ms = stats["avg_interval_ms"]
        std_interval_ms = stats["std_interval_ms"]

        support = co_occurrence / total
        confidence = co_occurrence / cause_count if cause_count > 0 else 0.0

        lift = 0.0
        if cause_count > 0 and effect_count > 0:
            p_cause = cause_count / total
            p_effect = effect_count / total
            if p_cause * p_effect > 0:
                lift = support / (p_cause * p_effect)

        interval_score = self._interval_score_from_stats(
            avg_interval_ms, std_interval_ms, intervals_count
        )
        pattern_score = self._estimate_pattern_score(cause, effect)

        base_score = (
            confidence * 0.45
            + min(lift, 5.0) / 5.0 * 0.25
            + pattern_score
            + interval_score * 0.10
        )

        if co_occurrence < self.min_support:
            penalty = max(0.0, 1.0 - (co_occurrence / max(1, self.min_support)) * 0.5)
            base_score *= penalty

        if cause_count == 0 or effect_count == 0:
            base_score = 0.0

        confidence_score = round(base_score * 100, 1)
        confidence_score = max(0.0, min(100.0, confidence_score))

        explanation = self._generate_explanation(
            cause, effect, cause_count, effect_count, co_occurrence, total,
            avg_interval_ms, std_interval_ms,
            confidence, lift, confidence_score,
        )

        cause_only = cause_count - co_occurrence
        effect_only = effect_count - co_occurrence

        result = CausalInferenceResult(
            cause=cause,
            effect=effect,
            confidence_score=confidence_score,
            co_occurrence_traces=co_occurrence,
            total_traces=stats["total_traces"],
            avg_time_interval_ms=round(avg_interval_ms, 1),
            std_time_interval_ms=round(std_interval_ms, 1),
            cause_only_traces=cause_only,
            effect_only_traces=effect_only,
            support=round(support, 4),
            confidence=round(confidence, 4),
            lift=round(lift, 4),
            explanation=explanation,
        )

        async with self._cache_lock:
            self._confidence_cache[cache_key] = result

        return result

    def _estimate_pattern_score(self, cause: str, effect: str) -> float:
        return 0.05

    @staticmethod
    def _interval_score_from_stats(avg: float, std: float, count: int) -> float:
        if count < 2 or avg == 0:
            return 0.0
        cv = std / avg
        return max(0.0, 1.0 - min(cv, 3.0) / 3.0)

    @staticmethod
    def _build_empty_result(cause: str, effect: str, reason: str) -> CausalInferenceResult:
        return CausalInferenceResult(
            cause=cause,
            effect=effect,
            confidence_score=0.0,
            co_occurrence_traces=0,
            total_traces=0,
            avg_time_interval_ms=0.0,
            std_time_interval_ms=0.0,
            cause_only_traces=0,
            effect_only_traces=0,
            support=0.0,
            confidence=0.0,
            lift=0.0,
            explanation=reason,
        )

    @staticmethod
    def _generate_explanation(
        cause: str, effect: str,
        cause_count: int, effect_count: int, co_occurrence: int, total: int,
        avg_interval_ms: float, std_interval_ms: float,
        confidence: float, lift: float, confidence_score: float,
    ) -> str:
        parts = []
        if co_occurrence == 0:
            return (
                f"[实时窗口内] 未发现因果关联：「{cause}」与「{effect}」从未同时出现。"
                f"当前窗口共 {total} 个 trace，「{cause}」出现 {cause_count} 次，"
                f"「{effect}」出现 {effect_count} 次，二者无交集。"
            )

        lead = f"[实时窗口内] 该「{cause}」导致「{effect}」的置信度为 {confidence_score}%"

        if co_occurrence >= 5:
            timing = ""
            if avg_interval_ms > 0:
                timing = f"，且平均时间间隔为 {avg_interval_ms:.0f}ms"
                if std_interval_ms > 0 and std_interval_ms / avg_interval_ms < 0.5:
                    timing += f"（标准差 ±{std_interval_ms:.0f}ms，时序稳定）"
                elif std_interval_ms > 0:
                    timing += f"（标准差 ±{std_interval_ms:.0f}ms）"
            parts.append(f"{lead}，二者在 {co_occurrence} 个 trace 中同时出现{timing}。")
        else:
            parts.append(f"{lead}。但样本量较小（仅 {co_occurrence} 次共现）。")

        if cause_count > 0:
            cond = co_occurrence / cause_count * 100
            parts.append(
                f"条件概率 P(effect|cause) = {cond:.1f}%，"
                f"即当「{cause}」发生时，有 {cond:.1f}% 的概率后续出现「{effect}」。"
            )

        if lift > 1.0:
            strength = "强" if lift > 3.0 else ("中等" if lift > 2.0 else "弱")
            parts.append(f"提升度 Lift = {lift:.2f}（>{strength}关联），说明二者共现并非随机巧合。")
        elif 0 < lift < 1.0:
            parts.append(f"提升度 Lift = {lift:.2f}（<1），二者更可能是抑制关系或偶然共现。")

        if confidence_score >= 80:
            parts.append(
                f"OK: 强因果信号：建议优先排查「{cause}」是否为「{effect}」的根因。"
            )
        elif confidence_score < 30 and co_occurrence > 0:
            parts.append("Warning: 置信度较低，需结合业务上下文进一步排查。")

        return "".join(parts)
