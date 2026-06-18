"""
流式因果推断引擎
 - 使用外部排序后的文件逐 trace 处理，不加载全部数据到内存
 - 使用 Count-Min Sketch / Heavy Hitter Tracker 近似计数
 - 只构建与用户指定 cause / effect 相关的因果子图，不构建完整图
内存占用：O(1) 量级（与日志总量无关）
"""

import math
import os
import sys
import tempfile
from typing import Optional, List, Tuple, Dict, Set
from collections import defaultdict

from .models import CausalInferenceResult, PatternResult
from .sketch import CountMinSketch, HeavyHitterTracker
from .external_sort import external_sort_by_trace, iter_grouped_traces


class StreamingCausalEngine:
    def __init__(
        self,
        cause: str,
        effect: str,
        min_support: int = 2,
        chunk_size_lines: int = 50000,
        tmp_dir: Optional[str] = None,
        verbose: bool = False,
    ):
        self.cause = cause
        self.effect = effect
        self.min_support = min_support
        self.chunk_size_lines = chunk_size_lines
        self.tmp_dir = tmp_dir or tempfile.gettempdir()
        self.verbose = verbose

        self.total_traces = 0
        self.cause_traces: Set[str] = set()
        self.effect_traces: Set[str] = set()
        self.both_traces: Set[str] = set()

        self._cause_only_count = 0
        self._effect_only_count = 0

        self.time_intervals_ms: List[float] = []

        self._trace_cause_ts: Dict[str, float] = {}
        self._trace_effect_ts: Dict[str, float] = {}

        self.sketch_cause = CountMinSketch.for_capacity(100000, error_pct=0.1)
        self.sketch_effect = CountMinSketch.for_capacity(100000, error_pct=0.1)
        self.sketch_both = CountMinSketch.for_capacity(50000, error_pct=0.05)

        self.pattern_sketch = CountMinSketch(epsilon=0.0005, delta=1e-7)
        self.top_patterns = HeavyHitterTracker(k=500)

        self._cleanup_tmp = []
        self.sorted_file: Optional[str] = None

    def load(self, log_file: Optional[str]) -> "StreamingCausalEngine":
        if self.verbose:
            sys.stderr.write("[streaming] Step 1/3: 外部排序中...\n")

        self.sorted_file, tmps = external_sort_by_trace(
            input_file=log_file,
            chunk_size_lines=self.chunk_size_lines,
            tmp_dir=self.tmp_dir,
            delete_tmp=True,
            verbose=self.verbose,
        )
        self._cleanup_tmp = tmps

        if self.verbose:
            sys.stderr.write("[streaming] Step 2/3: 逐 trace 扫描并更新计数...\n")

        for trace_id, events in iter_grouped_traces(self.sorted_file):
            self.total_traces += 1
            self._process_trace(trace_id, events)

            if self.verbose and self.total_traces % 5000 == 0:
                sys.stderr.write(f"  已处理 {self.total_traces} traces, 已发现共现 {len(self.both_traces)} 次...\n")

        if self.verbose:
            sys.stderr.write(
                f"[streaming] Step 3/3: 完成! 共 {self.total_traces} traces, "
                f"cause 出现 {len(self.cause_traces)} 次, effect 出现 {len(self.effect_traces)} 次, "
                f"共现 {len(self.both_traces)} 次\n"
            )

        return self

    def _process_trace(self, trace_id: str, events: List[dict]) -> None:
        event_keys: List[str] = []
        event_timestamps: List[float] = []
        has_cause = False
        has_effect = False
        cause_ts: Optional[float] = None
        effect_ts: Optional[float] = None

        for e in events:
            ev_type = str(e.get("event_type", ""))
            msg = str(e.get("message", ""))
            key = f"{ev_type}: {msg}"
            ts = float(e.get("timestamp", 0))
            event_keys.append(key)
            event_timestamps.append(ts)

            if key == self.cause:
                has_cause = True
                if cause_ts is None:
                    cause_ts = ts
            if key == self.effect:
                has_effect = True
                if effect_ts is None:
                    effect_ts = ts

        if has_cause:
            self.cause_traces.add(trace_id)
            self.sketch_cause.add(self.cause)
            if cause_ts is not None:
                self._trace_cause_ts[trace_id] = cause_ts
        if has_effect:
            self.effect_traces.add(trace_id)
            self.sketch_effect.add(self.effect)
            if effect_ts is not None:
                self._trace_effect_ts[trace_id] = effect_ts

        if has_cause and has_effect:
            self.both_traces.add(trace_id)
            self.sketch_both.add(f"{self.cause}||{self.effect}")
            if cause_ts is not None and effect_ts is not None and cause_ts <= effect_ts:
                interval_ms = (effect_ts - cause_ts) * 1000.0
                self.time_intervals_ms.append(interval_ms)
        elif has_cause and not has_effect:
            self._cause_only_count += 1
        elif has_effect and not has_cause:
            self._effect_only_count += 1

        if event_keys:
            self._update_pattern_sketch(event_keys)

    def _update_pattern_sketch(self, event_keys: List[str]) -> None:
        n = len(event_keys)
        max_win = min(n, 8)
        for start in range(n):
            for length in range(2, min(max_win + 1, n - start + 1)):
                seq = event_keys[start:start + length]
                if self.cause in seq and self.effect in seq:
                    cause_idx = seq.index(self.cause)
                    try:
                        effect_idx = seq.index(self.effect)
                    except ValueError:
                        continue
                    if cause_idx < effect_idx:
                        key = "||".join(seq)
                        self.pattern_sketch.add(key)
                        self.top_patterns.add(key)

    def infer(self) -> CausalInferenceResult:
        total = max(1, self.total_traces)
        cause_count = len(self.cause_traces)
        effect_count = len(self.effect_traces)
        co_occurrence = len(self.both_traces)

        avg_interval_ms = 0.0
        std_interval_ms = 0.0

        if self.time_intervals_ms:
            avg_interval_ms = sum(self.time_intervals_ms) / len(self.time_intervals_ms)
            if len(self.time_intervals_ms) >= 2:
                mean = avg_interval_ms
                variance = sum((x - mean) ** 2 for x in self.time_intervals_ms) / len(self.time_intervals_ms)
                std_interval_ms = math.sqrt(variance)

        support = co_occurrence / total
        confidence = co_occurrence / cause_count if cause_count > 0 else 0.0

        lift = 0.0
        if cause_count > 0 and effect_count > 0:
            p_cause = cause_count / total
            p_effect = effect_count / total
            if p_cause * p_effect > 0:
                lift = support / (p_cause * p_effect)

        relevant_patterns = self._extract_relevant_patterns()

        pattern_score = 0.0
        if relevant_patterns:
            top = relevant_patterns[0]
            pattern_score = min(top.confidence * 0.3 + min(top.lift, 5.0) * 0.1, 0.4)

        interval_score = self._interval_score(self.time_intervals_ms)

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
            cause_count, effect_count, co_occurrence, total,
            avg_interval_ms, std_interval_ms,
            confidence, lift, confidence_score, relevant_patterns,
        )

        cause_only = cause_count - co_occurrence
        effect_only = effect_count - co_occurrence

        return CausalInferenceResult(
            cause=self.cause,
            effect=self.effect,
            confidence_score=confidence_score,
            co_occurrence_traces=co_occurrence,
            total_traces=self.total_traces,
            avg_time_interval_ms=round(avg_interval_ms, 1),
            std_time_interval_ms=round(std_interval_ms, 1),
            cause_only_traces=cause_only,
            effect_only_traces=effect_only,
            support=round(support, 4),
            confidence=round(confidence, 4),
            lift=round(lift, 4),
            explanation=explanation,
        )

    def _extract_relevant_patterns(self) -> List[PatternResult]:
        results: List[PatternResult] = []
        total = max(1, self.total_traces)
        cause_count = len(self.cause_traces)
        effect_count = len(self.effect_traces)

        top_items = self.top_patterns.top(n=100)

        for key, sup in top_items:
            parts = key.split("||")
            if len(parts) < 2:
                continue
            try:
                cause_idx = parts.index(self.cause)
                effect_idx = parts.index(self.effect)
            except ValueError:
                continue
            if cause_idx >= effect_idx:
                continue

            prefix = tuple(parts[:-1])
            last = parts[-1]

            prefix_sup = self.pattern_sketch.query("||".join(prefix)) if len(prefix) >= 1 else cause_count
            last_sup = effect_count if last == self.effect else self.pattern_sketch.query(last)

            conf = sup / prefix_sup if prefix_sup > 0 else 0.0
            lift_v = 0.0
            if prefix_sup > 0 and last_sup > 0:
                lift_v = (sup / total) / ((prefix_sup / total) * (last_sup / total)) if (prefix_sup / total) * (last_sup / total) > 0 else 0.0

            results.append(PatternResult(
                pattern=tuple(parts),
                support=sup,
                confidence=conf,
                lift=lift_v,
            ))

        results.sort(key=lambda r: (r.confidence, r.support), reverse=True)
        return results[:20]

    @staticmethod
    def _interval_score(intervals_ms: List[float]) -> float:
        if len(intervals_ms) < 2:
            return 0.0
        mean = sum(intervals_ms) / len(intervals_ms)
        if mean == 0:
            return 0.0
        variance = sum((x - mean) ** 2 for x in intervals_ms) / len(intervals_ms)
        std = math.sqrt(variance)
        cv = std / mean
        return max(0.0, 1.0 - min(cv, 3.0) / 3.0)

    def _generate_explanation(
        self,
        cause_count: int, effect_count: int, co_occurrence: int, total: int,
        avg_interval_ms: float, std_interval_ms: float,
        confidence: float, lift: float, confidence_score: float,
        relevant_patterns: List[PatternResult],
    ) -> str:
        parts = []
        if co_occurrence == 0:
            return (
                f"未找到因果关联：「{self.cause}」与「{self.effect}」从未在同一 trace 中同时出现。"
                f"在 {total} 个 trace 中，「{self.cause}」出现 {cause_count} 次，"
                f"「{self.effect}」出现 {effect_count} 次，但二者无交集。"
            )

        lead = f"该「{self.cause}」导致「{self.effect}」的置信度为 {confidence_score}%"

        if co_occurrence >= 5:
            timing_part = ""
            if avg_interval_ms > 0:
                timing_part = f"，且平均时间间隔为 {avg_interval_ms:.0f}ms"
                if std_interval_ms > 0 and std_interval_ms / avg_interval_ms < 0.5:
                    timing_part += f"（标准差 ±{std_interval_ms:.0f}ms，时序稳定）"
                elif std_interval_ms > 0:
                    timing_part += f"（标准差 ±{std_interval_ms:.0f}ms）"
            parts.append(f"{lead}，因为二者在 {co_occurrence} 个 trace 中同时出现{timing_part}。")
        else:
            parts.append(f"{lead}。但样本量较小（仅 {co_occurrence} 次共现），结论需谨慎对待。")

        if cause_count > 0:
            cond = co_occurrence / cause_count * 100
            parts.append(
                f"条件概率 P(effect|cause) = {cond:.1f}%，"
                f"即当「{self.cause}」发生时，有 {cond:.1f}% 的概率后续出现「{self.effect}」。"
            )

        if lift > 1.0:
            strength = "强" if lift > 3.0 else ("中等" if lift > 2.0 else "弱")
            parts.append(f"提升度 Lift = {lift:.2f}（>{strength}关联），说明二者共现并非随机巧合。")
        elif 0 < lift < 1.0:
            parts.append(
                f"提升度 Lift = {lift:.2f}（<1），说明「{self.cause}」的出现反而降低了「{self.effect}」出现的概率。"
            )

        if relevant_patterns and len(relevant_patterns[0].pattern) > 2:
            p = relevant_patterns[0]
            chain = " -> ".join(p.pattern)
            parts.append(
                f"发现包含该因果对的频繁序列模式 [{chain}] "
                f"（支持度 {p.support}，置信度 {p.confidence * 100:.1f}%），进一步佐证了因果链路的存在。"
            )

        if confidence_score < 30 and co_occurrence > 0:
            parts.append(
                "Warning: 综合置信度较低，可能原因：共现次数不足、时间间隔波动过大、"
                "或存在更强的中间混淆变量。建议结合业务上下文进一步排查。"
            )

        if confidence_score >= 80:
            parts.append(
                f"OK: 强因果信号：建议优先排查「{self.cause}」是否为「{self.effect}」的根因，"
                "可通过注入实验或灰度发布进一步验证。"
            )

        return "".join(parts)

    def cleanup(self) -> None:
        if self.sorted_file and os.path.exists(self.sorted_file):
            try:
                os.remove(self.sorted_file)
            except OSError:
                pass
        for p in self._cleanup_tmp:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False
