from typing import Dict, List, Tuple, Set
from collections import defaultdict
from .models import LogEntry, TraceSequence, EventNode, CausalEdge


class CausalGraph:
    def __init__(self):
        self.nodes: Dict[str, EventNode] = {}
        self.edges: Dict[Tuple[str, str], CausalEdge] = {}
        self.total_traces: int = 0
        self.trace_event_sequences: Dict[str, List[str]] = {}
        self.trace_event_timestamps: Dict[str, List[float]] = {}

    def build_from_traces(self, sequences: List[TraceSequence]) -> None:
        self.total_traces = len(sequences)

        for seq in sequences:
            trace_id = seq.trace_id
            event_keys: List[str] = []
            event_timestamps: List[float] = []

            for event in seq.events:
                key = event.event_key
                event_keys.append(key)
                event_timestamps.append(event.timestamp)

                if key not in self.nodes:
                    event_type, message = self._split_key(key)
                    self.nodes[key] = EventNode(
                        key=key,
                        event_type=event_type,
                        message=message,
                    )

                node = self.nodes[key]
                node.count += 1
                node.trace_ids.add(trace_id)
                node.timestamps.append(event.timestamp)

            self.trace_event_sequences[trace_id] = event_keys
            self.trace_event_timestamps[trace_id] = event_timestamps

            self._build_edges_for_trace(trace_id, event_keys, event_timestamps)

    def _build_edges_for_trace(
        self,
        trace_id: str,
        event_keys: List[str],
        timestamps: List[float],
    ) -> None:
        n = len(event_keys)
        for i in range(n):
            for j in range(i + 1, n):
                source = event_keys[i]
                target = event_keys[j]
                t_source = timestamps[i]
                t_target = timestamps[j]
                interval = t_target - t_source

                edge_key = (source, target)
                if edge_key not in self.edges:
                    self.edges[edge_key] = CausalEdge(source=source, target=target)

                edge = self.edges[edge_key]
                if trace_id not in set(p[0] for p in edge.source_trace_pairs):
                    edge.co_occurrence_traces += 1
                edge.time_intervals.append(interval)
                edge.source_trace_pairs.append((trace_id, t_source, t_target))

    def get_traces_with_event(self, event_key: str) -> Set[str]:
        if event_key not in self.nodes:
            return set()
        return self.nodes[event_key].trace_ids

    def get_traces_with_both(self, cause: str, effect: str) -> Set[str]:
        cause_traces = self.get_traces_with_event(cause)
        effect_traces = self.get_traces_with_event(effect)
        return cause_traces & effect_traces

    def get_edge(self, source: str, target: str) -> CausalEdge:
        return self.edges.get((source, target))

    def get_all_pattern_sequences(self) -> List[Tuple[Tuple[str, ...], str]]:
        sequences: List[Tuple[Tuple[str, ...], str]] = []
        for trace_id, events in self.trace_event_sequences.items():
            sequences.append((tuple(events), trace_id))
        return sequences

    @staticmethod
    def _split_key(key: str) -> Tuple[str, str]:
        parts = key.split(": ", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return key, ""
