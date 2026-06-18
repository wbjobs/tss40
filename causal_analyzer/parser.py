import json
import sys
from typing import List, Iterator, Optional
from .models import LogEntry, TraceSequence
from collections import defaultdict


class LogParser:
    @staticmethod
    def parse_line(line: str) -> Optional[LogEntry]:
        line = line.strip()
        if not line:
            return None
        try:
            data = json.loads(line)
            return LogEntry.from_json(data)
        except (json.JSONDecodeError, ValueError):
            return None

    @staticmethod
    def parse_file(filepath: str) -> List[LogEntry]:
        entries: List[LogEntry] = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                entry = LogParser.parse_line(line)
                if entry:
                    entries.append(entry)
        return entries

    @staticmethod
    def parse_stdin() -> List[LogEntry]:
        entries: List[LogEntry] = []
        for line in sys.stdin:
            entry = LogParser.parse_line(line)
            if entry:
                entries.append(entry)
        return entries

    @staticmethod
    def parse_iter(filepath: Optional[str] = None) -> Iterator[LogEntry]:
        if filepath:
            source = open(filepath, "r", encoding="utf-8")
        else:
            source = sys.stdin

        try:
            for line in source:
                entry = LogParser.parse_line(line)
                if entry:
                    yield entry
        finally:
            if filepath:
                source.close()

    @staticmethod
    def group_by_trace(entries: List[LogEntry]) -> List[TraceSequence]:
        trace_map: dict = defaultdict(list)
        for entry in entries:
            if entry.trace_id:
                trace_map[entry.trace_id].append(entry)

        sequences: List[TraceSequence] = []
        for trace_id, events in trace_map.items():
            seq = TraceSequence(trace_id=trace_id, events=events)
            seq.sort_by_time()
            sequences.append(seq)

        sequences.sort(key=lambda s: s.trace_id)
        return sequences
