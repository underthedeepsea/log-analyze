from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from logrisk.incremental_sources import SourceCursor


REJECTION_CODES = frozenset({"record_decode_failed", "record_shape_invalid"})


@dataclass(frozen=True)
class KafkaCursorBounds:
    """One immutable offset contract shared by all records from an iterator."""

    initial_positions: Mapping[int, int]
    target: Mapping[int, int]
    identity: str


@dataclass(frozen=True)
class KafkaCursorDelta:
    bounds: KafkaCursorBounds
    partition: int
    next_offset: int


class KafkaCursorAccumulator:
    def __init__(self, delta: KafkaCursorDelta, current: SourceCursor) -> None:
        from logrisk.streaming_observability.contracts import offset_vector
        from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError

        self.bounds = delta.bounds
        self.positions = dict(self.bounds.initial_positions)
        if current.kind and (
            current.kind != "kafka"
            or offset_vector(current.value.get("partitions")) != self.positions
            or offset_vector(current.value.get("high_water")) != self.bounds.target
            or current.value.get("topic_identity") != self.bounds.identity
        ):
            raise KafkaBoundaryError("checkpoint_conflict")

    def apply(self, delta: KafkaCursorDelta) -> None:
        from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError

        previous = self.positions.get(delta.partition)
        if (
            delta.bounds is not self.bounds or previous is None
            or not previous < delta.next_offset <= self.bounds.target[delta.partition]
        ):
            raise KafkaBoundaryError("checkpoint_conflict")
        self.positions[delta.partition] = delta.next_offset

    def checkpoint(self) -> SourceCursor:
        from logrisk.incremental_sources import SourceCursor

        return SourceCursor("kafka", {
            "partitions": {str(key): value for key, value in self.positions.items()},
            "high_water": {str(key): value for key, value in self.bounds.target.items()},
            "topic_identity": self.bounds.identity,
        })
