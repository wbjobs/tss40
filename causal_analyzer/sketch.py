"""
Count-Min Sketch - 概率近似频率计数器
用于在有限内存下估算大量键的出现频率，替代 HashMap
误差保证: 估计值 >= 真实值，且 P(估计值 - 真实值 > eps * N) < delta
"""

import math
import hashlib
import array
import heapq
from typing import Tuple, List, Dict, Any


class CountMinSketch:
    def __init__(self, epsilon: float = 0.001, delta: float = 1e-6, depth: int = None, width: int = None):
        if depth and width:
            self.depth = depth
            self.width = width
        else:
            self.depth = int(math.ceil(math.log(1.0 / delta)))
            self.width = int(math.ceil(math.e / epsilon))

        self.tables: List[array.array] = [
            array.array("L", [0] * self.width) for _ in range(self.depth)
        ]
        self.hash_seeds: List[int] = list(range(self.depth))
        self._total = 0

    @classmethod
    def for_capacity(cls, expected_unique: int, error_pct: float = 0.01) -> "CountMinSketch":
        eps = error_pct / 100.0
        return cls(epsilon=eps, delta=1e-7)

    def _hash(self, item: str, seed: int) -> int:
        h = hashlib.md5(f"{seed}:{item}".encode("utf-8")).digest()
        return int.from_bytes(h[:8], "little") % self.width

    def add(self, item: str, count: int = 1) -> None:
        self._total += count
        for i in range(self.depth):
            idx = self._hash(item, self.hash_seeds[i])
            self.tables[i][idx] = min(self.tables[i][idx] + count, 2**64 - 1)

    def query(self, item: str) -> int:
        min_count = None
        for i in range(self.depth):
            idx = self._hash(item, self.hash_seeds[i])
            val = self.tables[i][idx]
            if min_count is None or val < min_count:
                min_count = val
        return min_count if min_count is not None else 0

    def __contains__(self, item: str) -> bool:
        return self.query(item) > 0

    def __len__(self) -> int:
        return self._total

    @property
    def total_count(self) -> int:
        return self._total

    @property
    def memory_bytes(self) -> int:
        return self.depth * self.width * 8


class HeavyHitterTracker:
    def __init__(self, k: int = 1000, sketch_epsilon: float = 0.001, sketch_delta: float = 1e-6):
        self.k = k
        self.sketch = CountMinSketch(epsilon=sketch_epsilon, delta=sketch_delta)
        self._heap: List[Tuple[int, str]] = []
        self._heap_set: set = set()

    def add(self, item: str, count: int = 1) -> None:
        self.sketch.add(item, count)
        est = self.sketch.query(item)

        if item in self._heap_set:
            for i, (_, existing_item) in enumerate(self._heap):
                if existing_item == item:
                    self._heap[i] = (est, item)
                    heapq.heapify(self._heap)
                    break
        else:
            if len(self._heap) < self.k:
                heapq.heappush(self._heap, (est, item))
                self._heap_set.add(item)
            elif est > self._heap[0][0]:
                old_count, old_item = heapq.heapreplace(self._heap, (est, item))
                self._heap_set.discard(old_item)
                self._heap_set.add(item)

    def top(self, n: int = None) -> List[Tuple[str, int]]:
        n = n or self.k
        items = sorted(self._heap, key=lambda x: x[0], reverse=True)
        return [(item, count) for count, item in items[:n]]

    def estimate(self, item: str) -> int:
        return self.sketch.query(item)

    def __contains__(self, item: str) -> bool:
        return self.sketch.query(item) > 0
