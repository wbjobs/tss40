from typing import List, Dict, Tuple, Set
from collections import defaultdict
from .models import PatternResult
from .causal_graph import CausalGraph


class PrefixSpan:
    def __init__(self, min_support: int = 2, max_pattern_len: int = 10):
        self.min_support = min_support
        self.max_pattern_len = max_pattern_len
        self.patterns: List[Tuple[Tuple[str, ...], int]] = []

    def mine(self, graph: CausalGraph) -> List[PatternResult]:
        sequences = graph.get_all_pattern_sequences()
        seq_data = [list(seq[0]) for seq in sequences]
        total = len(seq_data)

        self._prefix_span(
            prefix=(),
            seq_db=seq_data,
            min_sup=self.min_support,
            max_len=self.max_pattern_len,
        )

        pattern_support: Dict[Tuple[str, ...], int] = {}
        for pattern, sup in self.patterns:
            pattern_support[pattern] = pattern_support.get(pattern, 0) + sup

        individual_support: Dict[str, int] = {}
        for (item,), sup in [(p, s) for p, s in self.patterns if len(p) == 1]:
            individual_support[item] = individual_support.get(item, 0) + sup

        results: List[PatternResult] = []
        for pattern, sup in pattern_support.items():
            if len(pattern) >= 2:
                prefix_part = pattern[:-1]
                last_item = pattern[-1]

                prefix_sup = pattern_support.get(prefix_part, 0)
                if prefix_sup == 0:
                    prefix_sup = individual_support.get(prefix_part[0], 0) if len(prefix_part) == 1 else 0

                confidence = sup / prefix_sup if prefix_sup > 0 else 0.0
                last_sup = individual_support.get(last_item, 0)
                lift = 0.0
                if last_sup > 0 and prefix_sup > 0:
                    lift = (sup / total) / ((prefix_sup / total) * (last_sup / total))

                results.append(PatternResult(
                    pattern=pattern,
                    support=sup,
                    confidence=confidence,
                    lift=lift,
                ))

        results.sort(key=lambda r: (r.support, r.confidence), reverse=True)
        return results

    def _prefix_span(
        self,
        prefix: Tuple[str, ...],
        seq_db: List[List[str]],
        min_sup: int,
        max_len: int,
    ) -> None:
        if len(prefix) > 0:
            support = len(seq_db)
            if support >= min_sup:
                self.patterns.append((prefix, support))

        if len(prefix) >= max_len:
            return

        frequent_items = self._find_frequent_items(seq_db, min_sup)

        for item, support in frequent_items:
            projected_db = self._project_database(seq_db, item)
            if len(projected_db) >= min_sup:
                new_prefix = prefix + (item,)
                self._prefix_span(
                    prefix=new_prefix,
                    seq_db=projected_db,
                    min_sup=min_sup,
                    max_len=max_len,
                )

    @staticmethod
    def _find_frequent_items(
        seq_db: List[List[str]],
        min_sup: int,
    ) -> List[Tuple[str, int]]:
        item_count: Dict[str, Set[int]] = defaultdict(set)

        for seq_idx, sequence in enumerate(seq_db):
            for item in sequence:
                item_count[item].add(seq_idx)

        frequent = []
        for item, seq_indices in item_count.items():
            support = len(seq_indices)
            if support >= min_sup:
                frequent.append((item, support))

        frequent.sort(key=lambda x: x[1], reverse=True)
        return frequent

    @staticmethod
    def _project_database(seq_db: List[List[str]], item: str) -> List[List[str]]:
        projected = []
        for sequence in seq_db:
            try:
                idx = sequence.index(item)
                suffix = sequence[idx + 1:]
                if suffix:
                    projected.append(suffix)
            except ValueError:
                continue
        return projected

    def find_patterns_containing(
        self,
        patterns: List[PatternResult],
        cause: str,
        effect: str,
    ) -> List[PatternResult]:
        result = []
        for p in patterns:
            try:
                cause_idx = p.pattern.index(cause)
                effect_idx = p.pattern.index(effect)
                if cause_idx < effect_idx:
                    result.append(p)
            except ValueError:
                continue
        result.sort(key=lambda r: (r.confidence, r.support), reverse=True)
        return result
