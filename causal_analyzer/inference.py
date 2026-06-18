import math
from typing import List, Tuple, Set
from .models import CausalInferenceResult, PatternResult
from .causal_graph import CausalGraph
from .pattern_mining import PrefixSpan


class CausalInferenceEngine:
    def __init__(
        self,
        graph: CausalGraph,
        min_support: int = 2,
        max_pattern_len: int = 10,
    ):
        self.graph = graph
        self.min_support = min_support
        self.max_pattern_len = max_pattern_len
        self._patterns: List[PatternResult] = []
        self._mined = False

    def _ensure_mined(self) -> None:
        if not self._mined:
            miner = PrefixSpan(
                min_support=max(1, self.min_support),
                max_pattern_len=self.max_pattern_len,
            )
            self._patterns = miner.mine(self.graph)
            self._mined = True

    def infer(self, cause: str, effect: str) -> CausalInferenceResult:
        self._ensure_mined()

        total_traces = self.graph.total_traces
        cause_traces = self.graph.get_traces_with_event(cause)
        effect_traces = self.graph.get_traces_with_event(effect)
        both_traces = cause_traces & effect_traces

        cause_count = len(cause_traces)
        effect_count = len(effect_traces)
        co_occurrence = len(both_traces)
        cause_only = cause_count - co_occurrence
        effect_only = effect_count - co_occurrence

        edge = self.graph.get_edge(cause, effect)

        intervals_ms: List[float] = []
        avg_interval_ms = 0.0
        std_interval_ms = 0.0

        if edge and edge.source_trace_pairs:
            valid_pairs: List[Tuple[str, float, float]] = []
            seen_traces: Set[str] = set()
            for trace_id, t1, t2 in edge.source_trace_pairs:
                if trace_id in both_traces and trace_id not in seen_traces:
                    seen_traces.add(trace_id)
                    valid_pairs.append((trace_id, t1, t2))
                    interval = (t2 - t1) * 1000.0
                    intervals_ms.append(interval)

            if intervals_ms:
                avg_interval_ms = sum(intervals_ms) / len(intervals_ms)
                if len(intervals_ms) >= 2:
                    mean = avg_interval_ms
                    variance = sum((x - mean) ** 2 for x in intervals_ms) / len(intervals_ms)
                    std_interval_ms = math.sqrt(variance)

        support = co_occurrence / total_traces if total_traces > 0 else 0.0
        confidence = co_occurrence / cause_count if cause_count > 0 else 0.0

        lift = 0.0
        if cause_count > 0 and effect_count > 0 and total_traces > 0:
            p_cause = cause_count / total_traces
            p_effect = effect_count / total_traces
            if p_cause * p_effect > 0:
                lift = support / (p_cause * p_effect)

        miner = PrefixSpan()
        relevant_patterns = miner.find_patterns_containing(
            self._patterns, cause, effect
        )

        pattern_score = 0.0
        if relevant_patterns:
            top_pattern = relevant_patterns[0]
            pattern_score = min(top_pattern.confidence * 0.3 + min(top_pattern.lift, 5.0) * 0.1, 0.4)

        interval_score = self._calculate_interval_score(intervals_ms)

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
            cause=cause,
            effect=effect,
            co_occurrence=co_occurrence,
            cause_count=cause_count,
            effect_count=effect_count,
            total_traces=total_traces,
            avg_interval_ms=avg_interval_ms,
            std_interval_ms=std_interval_ms,
            confidence=confidence,
            lift=lift,
            confidence_score=confidence_score,
            relevant_patterns=relevant_patterns,
        )

        return CausalInferenceResult(
            cause=cause,
            effect=effect,
            confidence_score=confidence_score,
            co_occurrence_traces=co_occurrence,
            total_traces=total_traces,
            avg_time_interval_ms=round(avg_interval_ms, 1),
            std_time_interval_ms=round(std_interval_ms, 1),
            cause_only_traces=cause_only,
            effect_only_traces=effect_only,
            support=round(support, 4),
            confidence=round(confidence, 4),
            lift=round(lift, 4),
            explanation=explanation,
        )

    @staticmethod
    def _calculate_interval_score(intervals_ms: List[float]) -> float:
        if len(intervals_ms) < 2:
            return 0.0

        mean = sum(intervals_ms) / len(intervals_ms)
        if mean == 0:
            return 0.0

        variance = sum((x - mean) ** 2 for x in intervals_ms) / len(intervals_ms)
        std = math.sqrt(variance)
        cv = std / mean if mean > 0 else float("inf")

        score = max(0.0, 1.0 - min(cv, 3.0) / 3.0)
        return score

    @staticmethod
    def _generate_explanation(
        cause: str,
        effect: str,
        co_occurrence: int,
        cause_count: int,
        effect_count: int,
        total_traces: int,
        avg_interval_ms: float,
        std_interval_ms: float,
        confidence: float,
        lift: float,
        confidence_score: float,
        relevant_patterns: List[PatternResult],
    ) -> str:
        parts = []

        if co_occurrence == 0:
            return (
                f"未找到因果关联：「{cause}」与「{effect}」从未在同一 trace 中同时出现。"
                f"在 {total_traces} 个 trace 中，「{cause}」出现 {cause_count} 次，"
                f"「{effect}」出现 {effect_count} 次，但二者无交集。"
            )

        lead = f"该「{cause}」导致「{effect}」的置信度为 {confidence_score}%"

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
            parts.append(
                f"{lead}。但样本量较小（仅 {co_occurrence} 次共现），结论需谨慎对待。"
            )

        if cause_count > 0:
            conditional = co_occurrence / cause_count * 100
            parts.append(
                f"条件概率 P(effect|cause) = {conditional:.1f}%，"
                f"即当「{cause}」发生时，有 {conditional:.1f}% 的概率后续出现「{effect}」。"
            )

        if lift > 1.0:
            strength = "强" if lift > 3.0 else "中等" if lift > 2.0 else "弱"
            parts.append(
                f"提升度 Lift = {lift:.2f}（>{strength}关联），说明二者共现并非随机巧合。"
            )
        elif 0 < lift < 1.0:
            parts.append(
                f"提升度 Lift = {lift:.2f}（<1），说明「{cause}」的出现反而降低了「{effect}」出现的概率，"
                f"更可能是抑制关系或偶然共现。"
            )

        if relevant_patterns and len(relevant_patterns[0].pattern) > 2:
            pattern = relevant_patterns[0]
            chain = " → ".join(pattern.pattern)
            parts.append(
                f"发现包含该因果对的频繁序列模式 [{chain}] "
                f"（支持度 {pattern.support}，置信度 {pattern.confidence * 100:.1f}%），"
                f"进一步佐证了因果链路的存在。"
            )

        if confidence_score < 30 and co_occurrence > 0:
            parts.append(
                "⚠️ 综合置信度较低，可能原因：共现次数不足、时间间隔波动过大、"
                "或存在更强的中间混淆变量。建议结合业务上下文进一步排查。"
            )

        if confidence_score >= 80:
            parts.append(
                "✅ 强因果信号：建议优先排查「{cause}」是否为「{effect}」的根因，"
                "可通过注入实验或灰度发布进一步验证。".format(cause=cause, effect=effect)
            )

        return "".join(parts)

    def get_top_causal_pairs(self, top_n: int = 20) -> List[Tuple[str, str, float]]:
        self._ensure_mined()

        pairs: List[Tuple[str, str, float]] = []
        for pattern in self._patterns:
            if len(pattern.pattern) == 2:
                cause, effect = pattern.pattern
                score = (
                    pattern.confidence * 0.5
                    + min(pattern.lift, 5.0) / 5.0 * 0.3
                    + min(pattern.support, 100) / 100 * 0.2
                )
                pairs.append((cause, effect, round(score * 100, 1)))

        pairs.sort(key=lambda x: x[2], reverse=True)
        return pairs[:top_n]
