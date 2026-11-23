from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Iterator

from logrisk.incremental_sources import KafkaIncrementalSource
from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError, classify
from logrisk.streaming_observability.repository import ObservabilityRepository, assignment_key, digest


def assignment_lock_key(configuration: dict[str, str]) -> str:
    return "assignment:" + assignment_key(configuration)


@contextmanager
def legacy_execution(
    repository: ObservabilityRepository, source: KafkaIncrementalSource,
) -> Iterator[Callable[[], None]]:
    configuration = source.configuration
    # A first-time managed binding must take the same name lock. Once bound,
    # matching legacy tasks require proof of their physical Kafka destination.
    with repository.control_lock(assignment_lock_key(configuration)) as check_owner:
        repository.require_legacy_writer(configuration)
        if repository.managed_assignment_exists(configuration):
            adapter = source.adapters.get(configuration["adapter_id"])
            read_identity = getattr(getattr(adapter, "offset_reader", None), "read_identity", None)
            if not callable(read_identity):
                raise KafkaBoundaryError("topic_identity_unavailable", "无法验证旧批次与连续来源是否为同一消费组")
            try:
                identity = read_identity(configuration)
            except Exception as exc:
                raise KafkaBoundaryError(classify(exc), "无法验证旧批次与连续来源是否为同一消费组") from None
            broker_key = digest([identity, configuration["consumer_group"]])
            if repository.managed_assignment_exists(configuration, broker_key=broker_key):
                raise KafkaBoundaryError("group_frontier_changed", "该 Broker Topic 与消费组已绑定到连续来源")
        yield check_owner
