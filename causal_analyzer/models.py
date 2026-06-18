from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from collections import defaultdict


@dataclass
class LogEntry:
    timestamp: float
    service_name: str
    trace_id: str
    span_id: str
    parent_span_id: str
    event_type: str
    message: str

    @property
    def event_key(self) -> str:
        return f"{self.event_type}: {self.message}"

    @classmethod
    def from_json(cls, data: dict) -> "LogEntry":
        return cls(
            timestamp=float(data.get("timestamp", 0)),
            service_name=str(data.get("service_name", "")),
            trace_id=str(data.get("trace_id", "")),
            span_id=str(data.get("span_id", "")),
            parent_span_id=str(data.get("parent_span_id", "")),
            event_type=str(data.get("event_type", "")),
            message=str(data.get("message", "")),
        )


@dataclass
class EventNode:
    key: str
    event_type: str
    message: str
    count: int = 0
    trace_ids: set = field(default_factory=set)
    timestamps: List[float] = field(default_factory=list)


@dataclass
class CausalEdge:
    source: str
    target: str
    co_occurrence_traces: int = 0
    time_intervals: List[float] = field(default_factory=list)
    source_trace_pairs: List[Tuple[str, float, float]] = field(default_factory=list)

    @property
    def avg_interval(self) -> float:
        if not self.time_intervals:
            return 0.0
        return sum(self.time_intervals) / len(self.time_intervals)

    @property
    def std_interval(self) -> float:
        if len(self.time_intervals) < 2:
            return 0.0
        mean = self.avg_interval
        variance = sum((x - mean) ** 2 for x in self.time_intervals) / len(self.time_intervals)
        return variance ** 0.5


@dataclass
class TraceSequence:
    trace_id: str
    events: List[LogEntry] = field(default_factory=list)

    def sort_by_time(self) -> None:
        self.events.sort(key=lambda e: e.timestamp)


@dataclass
class PatternResult:
    pattern: Tuple[str, ...]
    support: int = 0
    confidence: float = 0.0
    lift: float = 0.0


@dataclass
class CausalInferenceResult:
    cause: str
    effect: str
    confidence_score: float
    co_occurrence_traces: int
    total_traces: int
    avg_time_interval_ms: float
    std_time_interval_ms: float
    cause_only_traces: int
    effect_only_traces: int
    support: float
    confidence: float
    lift: float
    explanation: str = ""
