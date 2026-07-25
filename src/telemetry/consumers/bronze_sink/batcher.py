"""Size/age-bounded buffer of raw records, kept per (topic, partition)."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

TP = tuple[str, int]  # (topic, partition)


@dataclass(frozen=True)
class RawRecord:
    offset: int
    timestamp_ms: int
    line: bytes  # one serialized JSON line


@dataclass
class _Buffer:
    records: list[RawRecord] = field(default_factory=list)
    size_bytes: int = 0


class MicroBatcher:
    def __init__(
        self,
        max_records: int = 5000,
        max_bytes: int = 5_000_000,
        max_age_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.max_age_s = max_age_s
        self._clock = clock
        self._buffers: dict[TP, _Buffer] = {}
        self._oldest: float | None = None

    def add(self, tp: TP, record: RawRecord) -> None:
        buf = self._buffers.setdefault(tp, _Buffer())
        buf.records.append(record)
        buf.size_bytes += len(record.line)
        if self._oldest is None:
            self._oldest = self._clock()

    @property
    def record_count(self) -> int:
        return sum(len(b.records) for b in self._buffers.values())

    @property
    def size_bytes(self) -> int:
        return sum(b.size_bytes for b in self._buffers.values())

    def should_flush(self) -> bool:
        if self._oldest is None:
            return False
        return (
            self.record_count >= self.max_records
            or self.size_bytes >= self.max_bytes
            or self._clock() - self._oldest >= self.max_age_s
        )

    def drain(self, only: set[TP] | None = None) -> dict[TP, list[RawRecord]]:
        """Remove and return buffered records (all, or only some partitions)."""
        keys = [tp for tp in self._buffers if only is None or tp in only]
        drained = {tp: self._buffers.pop(tp).records for tp in keys}
        if not self._buffers:
            self._oldest = None
        return drained

    def discard(self, tps: set[TP]) -> None:
        for tp in tps:
            self._buffers.pop(tp, None)
        if not self._buffers:
            self._oldest = None
