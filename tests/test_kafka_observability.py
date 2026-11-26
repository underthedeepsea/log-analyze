from __future__ import annotations

import hashlib
import json
import shutil
import threading
import traceback
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from itertools import product
from pathlib import Path
from types import SimpleNamespace

import pytest

from logrisk.application.api import ApiFacade
from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import KafkaIncrementalSource
from logrisk.kafka_adapter import KafkaPythonConsumerAdapter
from logrisk.large_file_pipeline import run_incremental_pipeline
from logrisk.node_risk import NodeRiskService
from logrisk.normalizer import LOG_CLOUD_PROFILE
from logrisk.runtime.identity import RequestIdentity
from logrisk.streaming_observability.contracts import aggregate_gaps, durable_rate, reading, states
from logrisk.streaming_observability.continuous import ContinuousKafka
from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError, KafkaOffsetReader, KafkaOffsets, _metadata_request, classify
from logrisk.streaming_observability.repository import ObservabilityError, ObservabilityRepository
from logrisk.streaming_observability.sampler import KafkaSampler
from logrisk.streaming_observability.service import ObservabilityService, result_coverage
from logrisk.streaming_observability.worker import WorkerObservation
from logrisk.streaming_state import StreamingStateRepository
from pipeline.database_migrate import _source_tables


ROOT = Path(__file__).resolve().parents[1]
BASE = "/api/streaming/observability/sources"
SETTINGS = {
    "adapter_id": "kafka-python", "topic": "fixture-logs", "consumer_group": "fixture-group",
    "bootstrap_env": "LOGRISK_OBSERVABILITY_FAKE_BOOTSTRAP", "cluster_id": "fixture",
}


class Clock:
    def __init__(self) -> None:
        self.time = datetime(2026, 11, 17, 6, 30, tzinfo=timezone.utc)

    def __call__(self) -> str:
        return self.time.isoformat()

    def advance(self, seconds: int) -> None:
        self.time += timedelta(seconds=seconds)


def test_precise_zero_unknown_and_incomparable_offsets():
    now = Clock()()
    fields = ("broker_end", "delivered", "durable", "group_committed", "batch_target", "result_frontier")
    row = {
        key: reading(value, epoch="a", collector_id="fixture", observed_at=now)
        for key, value in zip(fields, (2**53 + 5, 2**53 + 3, 2**53 + 1, 2**53 + 2, 2**53 + 1, None))
    }
    facts = aggregate_gaps([row], 2, exclusive=True)
    assert facts["undurable_offset_gap"]["value"] == "4"
    assert facts["undurable_offset_gap"]["coverage"] == "lower_bound"
    assert facts["batch_remaining_offset_gap"]["value"] == "0"
    assert facts["unconfirmed_commit_offset_gap"]["value"] is None
    assert facts["unconfirmed_commit_offset_gap"]["raw_signed_difference"] == "-1"
    row["group_committed"] = reading(None, epoch="a", collector_id="fixture", reason_code="never_committed")
    assert aggregate_gaps([row], 1, exclusive=True)["group_offset_gap"]["value"] is None
    row["group_committed"] = reading(0, epoch="b", collector_id="fixture", observed_at=now)
    assert aggregate_gaps([row], 1, exclusive=True)["group_offset_gap"]["reason_code"] == "epoch_mismatch"
    row["group_committed"] = reading(0, epoch="a", collector_id="fixture", observed_at=now)
    assert aggregate_gaps([row], 1, exclusive=False)["unconfirmed_commit_offset_gap"]["reason_code"] == "group_writer_unverified"


def test_result_coverage_is_distinct_from_publication_age():
    clock = Clock()
    earlier = clock()
    cursor = {"kind": "kafka", "value": {"partitions": {"0": 9}, "topic_identity": "same"}}
    task = {"task_id": "batch-two", "cursor": cursor}
    evidence = {"task_id": "batch-two", "frontier": cursor, "published_at": earlier}
    clock.advance(10000)
    assert result_coverage(task, {}, evidence, clock())["status"] == "covered"
    earlier_batch = {"kind": "kafka", "value": {"partitions": {"0": 7}, "topic_identity": "same"}}
    evidence.update(task_id="batch-one", frontier=earlier_batch, first_durable_commit_at=clock())
    clock.advance(120)
    assert result_coverage(task, {}, evidence, clock())["status"] == "pending"
    clock.advance(1)
    result = result_coverage(task, {}, evidence, clock())
    assert result["status"] == "delayed"
    assert result["wait_seconds"] == 121


class FakeBroker:
    def __init__(self, clock) -> None:
        self.clock = clock
        self.identity = "fixture-topic-uuid"
        self.end = {0: 2}
        self.beginning = {0: 0}
        self.group = {0: None}
        self.sparse = set()
        self.values = {}
        self.failed = False
        self.fail_on_commit = None
        self.commit_hook = None
        self.callbacks = []
        self.consumer_calls = 0
        self.commit_calls = []
        self.yielded = []
        self.read_calls = 0

    def read(self, __, *, require_beginning=False):
        self.read_calls += 1
        return KafkaOffsets(self.identity, True, tuple(self.end), len(self.end), dict(self.beginning),
                            dict(self.end), dict(self.group), self.clock(), self.clock())

    def read_identity(self, __):
        return self.identity

    def consumer(self, **kwargs):
        assert kwargs["allow_auto_create_topics"] is False
        assert kwargs["auto_offset_reset"] == "none"
        self.consumer_calls += 1
        broker = self

        class Consumer:
            def __init__(self):
                self.positions = {}

            def assign(self, partitions):
                self.positions = dict.fromkeys(partitions, None)

            def seek(self, partition, position):
                self.positions[partition] = position

            def poll(self, **kwargs):
                if broker.callbacks:
                    broker.callbacks.pop(0)()
                records = {}
                for partition, start in self.positions.items():
                    number = partition.partition
                    end = broker.end[number]
                    records[partition] = [
                        SimpleNamespace(offset=at, value=broker.values.get((number, at), json.dumps({
                            "node": "node-a", "component": "kernel", "timestamp": "2026-11-17T06:00:00+00:00",
                            "message": f"NVRM: Xid (0000:65:00): 79, GPU has fallen off the bus {at}",
                        }).encode()))
                        for at in range(start, min(end, start + 10)) if (number, at) not in broker.sparse
                    ]
                    broker.yielded.extend((number, record.offset) for record in records[partition])
                    self.positions[partition] = min(end, start + 10)
                return records

            def position(self, partition):
                return self.positions[partition]

            def commit(self, *, offsets, timeout_ms):
                values = {partition.partition: data.offset for partition, data in offsets.items()}
                broker.commit_calls.append(values)
                if broker.commit_hook is not None:
                    broker.commit_hook(values)
                if broker.failed or broker.fail_on_commit == len(broker.commit_calls):
                    broker.failed = False
                    raise TimeoutError("dummy-private-data")
                broker.group.update(values)

            def close(self, *, autocommit):
                assert not autocommit

        return Consumer()


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv(SETTINGS["bootstrap_env"], "fixture.invalid:9092")
    database = SQLiteDatabase(tmp_path / "test.sqlite3")
    streaming = StreamingStateRepository(database)
    clock = Clock()
    observations = ObservabilityRepository(database, clock)
    broker = FakeBroker(clock)
    adapter = KafkaPythonConsumerAdapter(broker.consumer, offset_reader=broker)
    risks = NodeRiskService(database, ROOT / "configs" / "node_risk.yaml", clock=clock)
    class Classified:
        def match(self, window):
            return {
                "domain": "hardware", "category": "gpu", "risk_type": "gpu.fallen_off_bus",
                "severity": "critical", "base_score": 95, "confidence": 1.0,
                "semantic_rule_id": "test-xid", "semantic_rule_version": 1,
                "semantic_fields": {}, "dedup": {"key_fields": ["cluster", "node_id", "risk_type"]},
            }

    def source_factory(configuration, **scope):
        return KafkaIncrementalSource(configuration, adapters={adapter.adapter_id: adapter}, **scope)

    def run_task(task_id, configuration, *, source):
        return run_incremental_pipeline(
            input_job_id=task_id, source=source, source_name="kafka://fixture-logs",
            config_path=ROOT / "configs" / "drain3_recommended.ini", rules_path=ROOT / "configs" / "risk_rules.yaml",
            state_dir=tmp_path / "runtime", worker_count=1, stream_batch_records=1,
            streaming_repository=streaming, resume_task_id=task_id, node_risks=risks, risk_semantics=Classified(),
        )

    def config_snapshot():
        return {"path": ROOT / "configs" / "drain3_recommended.ini", "config_id": "baseline", "version": 1}

    worker = ContinuousKafka(observations, streaming, source_factory=source_factory,
                             run_task=run_task, config_snapshot=config_snapshot)
    return SimpleNamespace(database=database, state=streaming, repo=observations, clock=clock, broker=broker,
                           source=source_factory(SETTINGS), worker=worker)


def setup(runtime, initial="earliest"):
    source_id = runtime.repo.register(runtime.source.descriptor(), initial_offset=initial, exclusive_group=True)
    runtime.repo.set_enabled(source_id, True)
    return source_id


def test_new_batch_uses_new_target_but_recovery_retains_its_original_target(runtime):
    source_id = setup(runtime)
    runtime.broker.failed = True
    with pytest.raises(KafkaBoundaryError, match="无法安全验证"):
        runtime.worker.step(source_id)
    active_id = runtime.repo.get(source_id)["active_task_id"]
    failed = runtime.state.get_task(active_id)
    assert failed["status"] == "failed"
    assert failed["records_processed"] == 1
    assert failed["pending_external_commit"]["value"]["partitions"] == {"0": 1}
    assert failed["kafka_batch"]["target"] == {"0": 2}
    assert runtime.broker.group == {0: None}
    runtime.broker.end = {0: 3}
    assert runtime.worker.step(source_id) == active_id
    first = runtime.state.get_task(active_id)
    assert first["records_processed"] == 2
    assert first["cursor"]["value"]["high_water"] == {"0": 2}
    assert first["result_reference"]["task_id"] == active_id
    second_id = runtime.worker.step(source_id)
    second = runtime.state.get_task(second_id)
    assert second_id != active_id
    assert second["kafka_batch"]["start"] == {"0": 2}
    assert second["kafka_batch"]["target"] == {"0": 3}
    assert second["records_processed"] == 1
    assert second["result_reference"]["task_id"] == second_id
    assert second["kafka_batch"]["miner_generation"] == first["miner_generation"]
    assert runtime.broker.commit_calls == [{0: 1}, {0: 1}, {0: 2}, {0: 3}]
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == 3
    assert runtime.worker.step(source_id) is None
    assert runtime.repo.get(source_id)["completed_task_id"] == second_id


def test_empty_and_sparse_batches_preserve_target_and_publish_coverage(runtime):
    runtime.broker.sparse = {(0, 0), (0, 1)}
    source_id = setup(runtime)
    batch_id = runtime.worker.step(source_id)
    task = runtime.state.get_task(batch_id)
    assert task["records_processed"] == 0
    assert task["kafka_batch"]["target"] == {"0": 2}
    assert task["cursor"]["value"]["partitions"] == {"0": 2}
    assert task["status"] == "completed"
    assert runtime.broker.group == {0: 2}
    assert ObservabilityService(runtime.repo, kafka_enabled=True).detail(source_id)["state"]["result"]["status"] == "covered"
    latest_id = runtime.repo.get(source_id)["completed_task_id"]
    assert latest_id == batch_id
    assert runtime.worker.step(source_id) is None


def test_recovery_replays_completed_effects_idempotently(runtime):
    source = setup(runtime)
    runtime.broker.fail_on_commit = 2
    with pytest.raises(KafkaBoundaryError):
        runtime.worker.step(source)
    task_id = runtime.repo.get(source)["active_task_id"]
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == 1
    assert runtime.worker.step(source) == task_id
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == 2
    assert runtime.state.get_task(task_id)["records_processed"] == 2


def test_new_topic_epoch_does_not_claim_old_result_covers_it(runtime):
    source_id = setup(runtime)
    runtime.worker.step(source_id)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    assert service.detail(source_id)["state"]["result"]["status"] == "covered"
    runtime.broker.identity = "fixture-topic-recreated"
    assert KafkaSampler(runtime.repo, runtime.broker).sample(source_id)
    snapshot = service.detail(source_id)
    assert snapshot["source_epoch"] == runtime.broker.identity
    assert service.sources(cursor=None, limit=10)["sources"][0]["source_epoch"] == snapshot["source_epoch"]
    assert snapshot["state"]["progress"] == {"status": "incomparable", "reason_code": "topic_identity_changed"}
    assert snapshot["state"]["result"] == {"status": "unknown", "reason_code": "topic_identity_changed"}
    assert snapshot["derived"]["undurable_offset_gap"]["value"] is None


def test_stop_restart_keeps_original_target_and_rejects_old_owner(runtime):
    source_id = setup(runtime)
    def interrupt():
        active = runtime.repo.get(source_id)["active_task_id"]
        assert runtime.state.get_task(active)["status"] == "running"
        assert runtime.state.interrupt_running_tasks() == 0
        with pytest.raises(ObservabilityError, match="已有运行中"):
            runtime.worker.step(source_id)
        runtime.repo.set_enabled(source_id, False)
        runtime.repo.set_enabled(source_id, True)

    runtime.broker.callbacks.append(interrupt)
    assert runtime.worker.step(source_id) is None
    active = runtime.repo.get(source_id)["active_task_id"]
    task = runtime.state.get_task(active)
    assert task["status"] == "interrupted"
    assert task["records_processed"] == 0
    assert runtime.broker.commit_calls == []
    runtime.broker.end = {0: 4}
    assert runtime.worker.step(source_id) == active
    assert runtime.state.get_task(active)["kafka_batch"]["target"] == {"0": 2}


def test_recreated_topic_during_poll_is_rejected_before_durable_write(runtime):
    source_id = setup(runtime)
    runtime.broker.callbacks.append(lambda: setattr(runtime.broker, "identity", "new-topic"))
    with pytest.raises(KafkaBoundaryError) as caught:
        runtime.worker.step(source_id)
    assert caught.value.code == "topic_identity_changed"
    active = runtime.state.get_task(runtime.repo.get(source_id)["active_task_id"])
    assert active["records_processed"] == 0
    assert not runtime.state.list_commits(active["task_id"])
    assert not runtime.broker.commit_calls


@pytest.mark.parametrize(
    ("change", "code"),
    [
        (lambda b: b.group.update({0: 9}), "group_frontier_changed"),
        (lambda b: (b.beginning.update({0: 3}), b.end.update({0: 5})), "offset_out_of_range"),
        (lambda b: setattr(b, "identity", "recreated"), "topic_identity_changed"),
        (lambda b: (b.beginning.update({1: 0}), b.end.update({1: 2}), b.group.update({1: None})), "partition_set_changed"),
    ],
)
def test_continuous_rejects_group_retention_topic_and_partition_conflicts(runtime, change, code):
    source_id = setup(runtime)
    assert runtime.worker.step(source_id)
    change(runtime.broker)
    with pytest.raises(KafkaBoundaryError) as exc:
        runtime.worker.step(source_id)
    assert code == exc.value.code
    assert runtime.repo.get(source_id)["block_reason"] == code
    assert runtime.repo.get(source_id)["active_task_id"] is None


def test_explicit_committed_start_continues_existing_group_and_fences_legacy(runtime):
    old = runtime.state.create_or_load(descriptor=runtime.source.descriptor(), config_hash="old")
    runtime.state.mark_failed(old["task_id"], "legacy fixture terminal")
    runtime.broker.group = {0: 1}
    with pytest.raises(ObservabilityError, match="committed"):
        runtime.repo.register(runtime.source.descriptor(), initial_offset="earliest", exclusive_group=True)
    source_id = setup(runtime, "committed")
    assert runtime.state.get_task(old["task_id"])["status"] == "failed"
    with pytest.raises(ObservabilityError, match="已登记为连续来源"):
        runtime.repo.require_legacy_writer(SETTINGS)
    new = runtime.state.get_task(runtime.worker.step(source_id))
    assert new["kafka_batch"]["start"] == {"0": 1}
    assert new["records_processed"] == 1
    assert runtime.broker.group == {0: 2}


def test_sampler_fencing_and_api_stale_pagination_never_touch_broker(runtime):
    source_id = setup(runtime)
    runtime.broker.end.update({1: 0})
    runtime.broker.beginning.update({1: 0})
    runtime.broker.group.update({1: None})
    token = runtime.repo.observer_claim(source_id, lease_seconds=30)
    assert runtime.repo.observer_claim(source_id) is None
    runtime.clock.advance(31)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    assert sampler.sample(source_id)
    assert not runtime.repo.observer_publish(source_id, token, {"status": "cannot_overwrite"})
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    identity_config = SimpleNamespace(enabled=True)
    facade = ApiFacade(SimpleNamespace(runtime_config=SimpleNamespace(identity=identity_config),
                                     streaming_observability=lambda: service), version="1.45.0")
    actor = RequestIdentity("operator", (), "fixture", True, "trusted_proxy", "127.0.0.1")
    path = BASE + "/" + source_id
    assert facade.dispatch_read(path).status == 403
    detail = facade.dispatch_read(path, identity=actor).body
    assert detail["worker"]["last_heartbeat_at"] is None
    assert facade.dispatch_read(path + "/partitions", {"limit": "101"}, identity=actor).status == 400
    first = facade.dispatch_read(path + "/partitions", {
        "limit": "1", "snapshot_id": detail["snapshot_id"],
    }, actor).body
    second = facade.dispatch_read(path + "/partitions", {
        "limit": "1", "snapshot_id": detail["snapshot_id"], "cursor": first["next_cursor"],
    }, actor).body
    assert second["partitions"][0]["broker_end"]["value"] == "0"
    assert second["partitions"][0]["group_committed"]["reason_code"] == "never_committed"
    runtime.clock.advance(46)
    assert facade.dispatch_read(path + "/partitions", {
        "limit": "1", "snapshot_id": detail["snapshot_id"], "cursor": first["next_cursor"],
    }, actor).body["code"] == "snapshot_expired"
    expired = facade.dispatch_read(path, identity=actor).body
    assert expired["sampled_at"] == detail["sampled_at"]
    assert expired["state"]["source"]["status"] == "stale"
    assert runtime.broker.read_calls == 1
    assert runtime.broker.consumer_calls == 0


def test_sampler_uses_metadata_list_offsets_and_offset_fetch_only(monkeypatch):
    monkeypatch.setenv(SETTINGS["bootstrap_env"], "fake.invalid:9092")
    calls = []

    class Admin:
        def __init__(self, **kwargs):
            calls.append("admin_init")

        def list_consumer_group_offsets(self, group, *, partitions):
            calls.append("group")
            assert group == SETTINGS["consumer_group"]
            return {partitions[0]: SimpleNamespace(offset=-1)}

        def close(self):
            calls.append("admin_close")

    class Consumer:
        def __init__(self, **kwargs):
            assert kwargs["group_id"] is None
            assert not kwargs["allow_auto_create_topics"]
            calls.append("reader_init")

        def end_offsets(self, partitions):
            calls.append("end")
            return {partitions[0]: 12}

        def close(self, *, autocommit):
            assert not autocommit
            calls.append("reader_close")

    def metadata(admin, topic):
        assert topic == SETTINGS["topic"]
        calls.append("metadata")
        return "identity", {0}

    reader = KafkaOffsetReader(consumer_factory=Consumer, admin_factory=Admin, metadata_reader=metadata)
    measured = reader.read(SETTINGS)
    assert measured.end == {0: 12}
    assert measured.group == {0: None}
    assert calls == ["admin_init", "metadata", "reader_init", "end", "group", "metadata", "reader_close", "admin_close"]


def test_actual_worker_events_and_heartbeat_are_fenced_separately_from_task_status(runtime, monkeypatch):
    import threading

    task = runtime.state.create_or_load(descriptor=runtime.source.descriptor(), config_hash="legacy")
    task_id = task["task_id"]
    lease = runtime.state.claim_task(task_id)["lease_token"]
    published = threading.Event()
    publish = runtime.repo.worker_publish

    def observe(task_id, worker_epoch, snapshot):
        saved = publish(task_id, worker_epoch, snapshot)
        if saved:
            published.set()
        return saved

    monkeypatch.setattr(runtime.repo, "worker_publish", observe)
    activity = WorkerObservation(runtime.repo, task_id, lease)
    activity.event("poll")
    activity.event("yield", {"partition": 0, "next_offset": 15})
    with activity:
        assert published.wait(2)
        detail = ObservabilityService(runtime.repo, kafka_enabled=True).detail("legacy_" + task_id)
        assert detail["state"]["worker"]["status"] == "running"
        assert detail["worker"]["last_poll_returned_at"] == runtime.clock()
        assert detail["worker"]["last_record_received_at"] == runtime.clock()
    activity.thread.join(2)
    runtime.state.finish_claim(task_id, lease, error="fixture interrupt", interrupted=True)
    runtime.state.claim_task(task_id)
    assert not runtime.repo.worker_publish(task_id, lease, {"last_heartbeat_at": runtime.clock()})
    detail = ObservabilityService(runtime.repo, kafka_enabled=True).detail("legacy_" + task_id)
    assert detail["state"]["worker"]["status"] == "unknown"
    assert detail["worker"]["last_poll_returned_at"] is None


def test_rapid_restart_excludes_saved_heartbeat_and_fences_the_real_old_worker(runtime, monkeypatch):
    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    polling, release_poll = threading.Event(), threading.Event()
    first_heartbeat, next_heartbeat = threading.Event(), threading.Event()
    release_heartbeat, stale_heartbeat = threading.Event(), threading.Event()
    errors, accepted = [], []
    original_init = WorkerObservation.__init__
    original_publish = ObservabilityRepository.worker_publish

    def quick_heartbeat(self, repository, task_id, epoch, interval=15):
        repository.clock = runtime.clock
        original_init(self, repository, task_id, epoch, interval=0.02)

    def capture_publish(self, task_id, lease, sample):
        if first_heartbeat.is_set():
            next_heartbeat.set()
            if not release_heartbeat.wait(3):
                raise RuntimeError("fixture did not release the pending heartbeat")
            accepted.append(original_publish(self, task_id, lease, sample))
            stale_heartbeat.set()
            return accepted[-1]
        result = original_publish(self, task_id, lease, sample)
        if result:
            first_heartbeat.set()
        return result

    def wait_in_poll():
        polling.set()
        if not release_poll.wait(5):
            raise RuntimeError("fixture did not release poll")

    def run():
        try:
            runtime.worker.step(source_id)
        except Exception as exc:
            errors.append(exc)

    monkeypatch.setattr(WorkerObservation, "__init__", quick_heartbeat)
    monkeypatch.setattr(ObservabilityRepository, "worker_publish", capture_publish)
    runtime.broker.callbacks.append(wait_in_poll)
    owner = threading.Thread(target=run, daemon=True)
    owner.start()
    try:
        assert polling.wait(3) and first_heartbeat.wait(2) and next_heartbeat.wait(2)
        previous = service.detail(source_id)
        assert previous["state"]["worker"]["status"] == "running"
        assert previous["worker"]["last_heartbeat_at"] == runtime.clock()
        task_id = previous["task"]["task_id"]
        epoch = runtime.repo.get(source_id)["control_epoch"]
        runtime.repo.set_enabled(source_id, False)
        assert service.detail(source_id)["state"]["worker"]["status"] == "stopped"
        runtime.repo.set_enabled(source_id, True)
        assert runtime.repo.get(source_id)["control_epoch"] == epoch + 2
        current = service.detail(source_id)
        assert owner.is_alive()
        assert current["task"]["status"] == "running"
        assert current["state"]["worker"] == {"status": "unknown", "reason_code": "process_unverified"}
        assert current["worker"]["last_heartbeat_at"] is None
        assert current["worker"]["worker_epoch"] is None
        with runtime.database.connect() as connection:
            assert connection.execute(
                "SELECT observed_at FROM kafka_worker_observations WHERE task_id=?", (task_id,),
            ).fetchone()[0] == runtime.clock()
        runtime.clock.advance(1)
        release_heartbeat.set()
        assert stale_heartbeat.wait(2)
        assert accepted == [False]
        assert service.detail(source_id)["worker"]["last_heartbeat_at"] is None
    finally:
        release_heartbeat.set()
        release_poll.set()
        owner.join(4)
    assert not owner.is_alive() and not errors
    assert runtime.state.get_task(task_id)["status"] == "interrupted"
    assert runtime.broker.commit_calls == []
    monkeypatch.setattr(ObservabilityRepository, "worker_publish", original_publish)
    assert runtime.worker.step(source_id) == task_id
    assert runtime.broker.commit_calls == [{0: 1}, {0: 2}]


def test_metadata_v10_fixture_scopes_topic_and_disables_topic_creation():
    request = _metadata_request("fixture-logs")
    assert request.to_object() == {
        "topics": [{"topic_id": bytes(16), "name": "fixture-logs", "tags": {}}],
        "allow_auto_create": False, "cluster_operations": False, "topic_operations": False, "tags": {},
    }
    assert request.encode().hex() == "02000000000000000000000000000000000d666978747572652d6c6f67730000000000"
    response = request.RESPONSE_TYPE(
        0, [(0, "fixture", 9092, None, {})], "cluster", 0,
        [(0, "fixture-logs", bytes(range(16)), False, [(0, 0, 0, 1, [0], [0], [], {})], 0, {})], 0, {},
    )
    result = request.RESPONSE_TYPE.decode(response.encode()).to_object()
    assert result["cluster_id"] == "cluster"
    assert result["topics"][0]["topic_id"] == bytes(range(16))
    assert result["topics"][0]["partitions"][0]["partition"] == 0


def test_sqlite_migration_projects_legacy_source_and_remains_topologically_exportable(tmp_path):
    old_migrations = tmp_path / "before"
    old_migrations.mkdir()
    for path in (ROOT / "database" / "migrations").glob("*.sql"):
        if path.name < "0033":
            shutil.copy(path, old_migrations / path.name)
    db_path = tmp_path / "fixture.sqlite3"
    database = SQLiteDatabase(db_path, migrations_dir=old_migrations)
    descriptor = KafkaIncrementalSource({**SETTINGS, "source_profile": LOG_CLOUD_PROFILE}).descriptor().to_dict()
    task = {"task_id": "old-task", "source": descriptor, "status": "completed"}
    at = Clock()()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at)"
            " VALUES (?, 'kafka', ?, 'test', 'completed', 'COMPLETED', '{}', ?, ?, ?)",
            ("old-task", json.dumps(descriptor), json.dumps(task), at, at),
        )
        connection.execute("INSERT INTO streaming_task_events VALUES ('old-task',1,'task_completed','{}',?)", (at,))
    database = SQLiteDatabase(db_path)
    repository = ObservabilityRepository(database)
    legacy = repository.get("legacy_old-task")
    assert json.loads(legacy["descriptor_json"])["configuration"]["source_profile"] == LOG_CLOUD_PROFILE
    assert repository.events("legacy_old-task", cursor=None, limit=1)["events"][0]["event_type"] == "task_completed"
    with database.connect() as connection:
        ordered = _source_tables(connection)
        assert ordered.index("kafka_stream_sources") < ordered.index("streaming_tasks") < ordered.index("kafka_worker_observations")
        assert ordered.index("streaming_tasks") < ordered.index("streaming_task_events")
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_managed_owner_rejects_same_physical_group_under_bootstrap_alias(runtime):
    original = setup(runtime)
    assert runtime.worker.step(original)
    alternate = KafkaIncrementalSource({**SETTINGS, "bootstrap_env": "FIXTURE_BOOTSTRAP_ALIAS"})
    alias = runtime.repo.register(alternate.descriptor(), initial_offset="committed", exclusive_group=True)
    runtime.repo.set_enabled(alias, True)
    runtime.broker.end[0] += 1
    with pytest.raises(KafkaBoundaryError, match="另一连续来源"):
        runtime.worker.step(alias)
    assert runtime.repo.get(alias)["active_task_id"] is None


def test_standalone_django_share_observability_contract_and_workspace_routes(runtime, tmp_path, monkeypatch):
    from urllib.request import urlopen

    import django
    from django.test import Client, override_settings

    from pipeline.dashboard_server import build_server

    monkeypatch.setenv("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
    django.setup()
    from logrisk_django.service_factory import clear_cached_container

    monkeypatch.setattr(KafkaOffsetReader, "read", lambda *args, **kwargs: pytest.fail("GET attempted Kafka access"))
    source_id = setup(runtime)
    KafkaSampler(runtime.repo, runtime.broker).sample(source_id)
    server = build_server(
        "127.0.0.1", 0, database_path=runtime.database.path, database_provider="sqlite",
        state_root=tmp_path, output_root=tmp_path / "output", kafka_enabled=False,
        ollama_checker=lambda: pytest.fail("GET attempted model access"),
    )
    runner = threading.Thread(target=server.serve_forever, daemon=True)
    runner.start()
    paths = (BASE, BASE + "/" + source_id, BASE + "/" + source_id + "/events")
    host = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with override_settings(LOGRISK={
            "project_root": str(ROOT), "state_root": str(tmp_path),
            "output_root": str(tmp_path / "django_output"), "database_provider": "sqlite",
            "database_path": str(runtime.database.path),
            "airflow_base_url": "http://127.0.0.1:18080", "airflow_dag_id": "fixture",
        }):
            clear_cached_container()
            client = Client()
            for path in paths:
                with urlopen(host + path, timeout=3) as response:
                    expected = json.load(response)
                django_response = client.get(path)
                assert django_response.status_code == 200
                actual = django_response.json()
                actual.pop("served_at", None)
                expected.pop("served_at", None)
                assert actual == expected
            for path in ("/", "/streaming", "/streaming-tasks", "/offline-analysis"):
                with urlopen(host + path, timeout=3) as response:
                    assert response.status == 200
                    assert "text/html" in response.headers["Content-Type"]
                django_response = client.get(path)
                assert django_response.status_code == 200
                assert "text/html" in django_response["Content-Type"]
                django_response.close()
    finally:
        clear_cached_container()
        server.shutdown()
        runner.join(timeout=3)
        server.server_close()


def test_scheduler_keeps_live_waiting_evidence_and_exits_without_poll(runtime, monkeypatch):
    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    waiting = threading.Event()
    proceed = threading.Event()
    pulse_after_wait = threading.Event()
    published = runtime.repo.scheduler_heartbeat
    errors = []

    def beat(source_id, token, phase):
        ok = published(source_id, token, phase)
        if ok and phase == "awaiting_batch" and runtime.clock.time.second == 20:
            pulse_after_wait.set()
        return ok

    monkeypatch.setattr(runtime.repo, "scheduler_heartbeat", beat)
    def pause(_):
        waiting.set()
        if not proceed.wait(3):
            raise RuntimeError("fixture did not release scheduler")

    def run():
        try:
            runtime.worker.run(source_id, wait=pause, heartbeat_interval=0.01)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert waiting.wait(3)
        detail = service.detail(source_id)
        assert detail["task"]["status"] == "completed"
        assert detail["state"]["worker"] == {"status": "waiting", "reason_code": "scheduler_waiting"}
        scheduler = detail["worker"]["scheduler"]
        assert scheduler["phase"] == "awaiting_batch"
        assert detail["worker"]["last_poll_returned_at"] is None
        runtime.clock.advance(20)
        assert pulse_after_wait.wait(2)
        assert service.detail(source_id)["worker"]["scheduler"]["last_heartbeat_at"] != scheduler["last_heartbeat_at"]
        with pytest.raises(ObservabilityError, match="已有运行中"):
            runtime.worker.run(source_id, once=True)
        runtime.repo.set_enabled(source_id, False)
        assert service.detail(source_id)["state"]["worker"]["status"] == "stopped"
    finally:
        proceed.set()
        thread.join(4)
    assert not thread.is_alive()
    assert not errors
    assert service.detail(source_id)["worker"]["scheduler"]["state"] == "inactive"
    runtime.repo.set_enabled(source_id, True)
    runtime.worker.run(source_id, once=True)
    assert service.detail(source_id)["state"]["worker"]["status"] == "completed"


def test_fenced_stale_scheduler_is_not_current_worker(runtime):
    source_id = setup(runtime)
    epoch = runtime.repo.get(source_id)["control_epoch"]
    token = runtime.repo.start_scheduler(source_id, epoch)
    runtime.clock.advance(46)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    assert service.detail(source_id)["state"]["worker"] == {"status": "stale", "reason_code": "scheduler_heartbeat_expired"}
    newer = runtime.repo.start_scheduler(source_id, epoch)
    assert not runtime.repo.scheduler_heartbeat(source_id, token, "awaiting_batch")
    runtime.repo.finish_scheduler(source_id, token)
    assert service.detail(source_id)["worker"]["scheduler"]["owner_epoch"] == newer


def test_sampler_shared_cadence_backoff_and_two_success_recovery(runtime, monkeypatch):
    source_id = setup(runtime)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    another = KafkaSampler(ObservabilityRepository(runtime.database, runtime.clock), runtime.broker)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    assert sampler.sample(source_id)
    assert not another.sample(source_id)
    assert runtime.broker.read_calls == 1
    original = runtime.broker.read
    failures = []

    def fail(*args, **kwargs):
        failures.append(runtime.clock())
        raise TimeoutError("PRIVATE-FIXTURE-DETAIL")

    monkeypatch.setattr(runtime.broker, "read", fail)
    runtime.clock.advance(15)
    for delay in (15, 30, 60):
        assert another.sample(source_id)
        assert service.detail(source_id)["state"]["source"] == {"status": "error", "reason_code": "timeout"}
        assert runtime.repo.observer_schedule(source_id) == ("error", delay)
        runtime.clock.advance(delay - 1)
        assert not sampler.sample(source_id)
        runtime.clock.advance(1)
    assert len(failures) == 3
    monkeypatch.setattr(runtime.broker, "read", original)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"] == {"status": "recovering", "reason_code": "sample_recovery_pending"}
    assert not another.sample(source_id)
    runtime.clock.advance(15)
    assert another.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "available"


@pytest.mark.parametrize(
    ("gap", "change", "second_status"),
    [
        (45, "none", "available"),
        (60, "none", "recovering"),
        (15, "epoch", "recovering"),
        (15, "out_of_order", "recovering"),
        (15, "same_timestamp", "recovering"),
    ],
)
def test_sampler_recovery_requires_two_current_ordered_successes_in_the_same_epoch(
    runtime, monkeypatch, gap, change, second_status,
):
    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    original = runtime.broker.read

    def fail(*args, **kwargs):
        raise TimeoutError("fixture timeout")

    monkeypatch.setattr(runtime.broker, "read", fail)
    assert sampler.sample(source_id)
    runtime.clock.advance(15)
    monkeypatch.setattr(runtime.broker, "read", original)
    assert sampler.sample(source_id)
    first = service.detail(source_id)
    assert first["state"]["source"]["status"] == "recovering"
    runtime.clock.advance(gap)
    if gap > 45:
        assert service.detail(source_id)["state"]["source"]["status"] == "stale"
    if change == "epoch":
        runtime.broker.identity = "new-epoch"
    if change in {"out_of_order", "same_timestamp"}:
        rewind = int(change == "out_of_order")
        measured_at = (datetime.fromisoformat(first["sampled_at"]) - timedelta(seconds=rewind)).isoformat()
        monkeypatch.setattr(runtime.broker, "read", lambda *args, **kwargs: replace(
            original(*args, **kwargs), end_at=measured_at, group_at=measured_at,
        ))
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == second_status
    assert not sampler.sample(source_id)
    runtime.clock.advance(15)
    monkeypatch.setattr(runtime.broker, "read", original)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "available"


def test_sampler_future_observation_does_not_become_a_recovery_confirmation(runtime, monkeypatch):
    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    original = runtime.broker.read
    future = (runtime.clock.time + timedelta(seconds=5)).isoformat()
    monkeypatch.setattr(runtime.broker, "read", lambda *args, **kwargs: replace(
        original(*args, **kwargs), end_at=future, group_at=future,
    ))
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "stale"
    runtime.clock.advance(15)
    monkeypatch.setattr(runtime.broker, "read", original)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "recovering"
    runtime.clock.advance(15)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "available"


@pytest.mark.parametrize(("field", "delta"), [
    ("group_at", 10), ("group_at", -46), ("group_at", None),
    ("end_at", None),
])
def test_invalid_broker_component_time_cannot_confirm_recovery(runtime, monkeypatch, field, delta):
    source_id = setup(runtime)
    runtime.broker.group = {0: 0}
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    original = runtime.broker.read

    def fail(*args, **kwargs):
        raise TimeoutError("fixture timeout")

    def invalid_time(*args, **kwargs):
        measured_at = None if delta is None else (runtime.clock.time + timedelta(seconds=delta)).isoformat()
        return replace(original(*args, **kwargs), **{field: measured_at})

    monkeypatch.setattr(runtime.broker, "read", fail)
    assert sampler.sample(source_id)
    monkeypatch.setattr(runtime.broker, "read", invalid_time)
    for _ in range(2):
        runtime.clock.advance(15)
        assert sampler.sample(source_id)
        detail = service.detail(source_id)
        assert detail["state"]["source"] == {"status": "stale", "reason_code": "sample_expired"}
        partition = service.partitions(
            source_id, snapshot_id=detail["snapshot_id"], cursor=None, limit=1,
        )["partitions"][0]
        metric = "broker_end" if field == "end_at" else "group_committed"
        assert partition[metric]["availability"] == "stale"

    runtime.broker.group = {0: None}
    monkeypatch.setattr(runtime.broker, "read", original)
    for expected in ("recovering", "available"):
        runtime.clock.advance(15)
        assert sampler.sample(source_id)
        detail = service.detail(source_id)
        assert detail["state"]["source"]["status"] == expected
    row = service.partitions(source_id, snapshot_id=detail["snapshot_id"], cursor=None, limit=1)["partitions"][0]
    assert row["group_committed"]["value"] is None
    assert row["group_committed"]["reason_code"] == "never_committed"


@pytest.mark.parametrize(("group_age", "second_status"), [(30, "available"), (31, "recovering")])
def test_recovery_expires_when_previous_group_time_expires_before_end_time(
    runtime, monkeypatch, group_age, second_status,
):
    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    original = runtime.broker.read

    def fail(*args, **kwargs):
        raise TimeoutError("fixture timeout")

    monkeypatch.setattr(runtime.broker, "read", fail)
    assert sampler.sample(source_id)
    runtime.clock.advance(15)
    monkeypatch.setattr(runtime.broker, "read", lambda *args, **kwargs: replace(
        original(*args, **kwargs), group_at=(runtime.clock.time - timedelta(seconds=group_age)).isoformat(),
    ))
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "recovering"
    runtime.clock.advance(15)
    before = service.detail(source_id)
    assert before["state"]["source"]["status"] == ("recovering" if group_age == 30 else "stale")
    monkeypatch.setattr(runtime.broker, "read", original)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == second_status
    runtime.clock.advance(15)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "available"


@pytest.mark.parametrize("rewind", (0, 1))
def test_recovery_requires_group_observation_time_to_advance(runtime, monkeypatch, rewind):
    source_id = setup(runtime)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    original = runtime.broker.read

    def fail(*args, **kwargs):
        raise TimeoutError("fixture timeout")

    monkeypatch.setattr(runtime.broker, "read", fail)
    assert sampler.sample(source_id)
    runtime.clock.advance(15)
    monkeypatch.setattr(runtime.broker, "read", original)
    assert sampler.sample(source_id)
    first_group_at = runtime.clock.time
    runtime.clock.advance(15)
    monkeypatch.setattr(runtime.broker, "read", lambda *args, **kwargs: replace(
        original(*args, **kwargs), group_at=(first_group_at - timedelta(seconds=rewind)).isoformat(),
    ))
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "recovering"


def test_available_source_expires_when_group_observation_expires(runtime, monkeypatch):
    source_id = setup(runtime)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    original = runtime.broker.read
    monkeypatch.setattr(runtime.broker, "read", lambda *args, **kwargs: replace(
        original(*args, **kwargs), group_at=(runtime.clock.time - timedelta(seconds=30)).isoformat(),
    ))
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "available"
    runtime.clock.advance(16)
    assert service.detail(source_id)["state"]["source"] == {"status": "stale", "reason_code": "sample_expired"}


def test_source_state_checks_all_sampling_times_before_available_or_recovering():
    clock = Clock()
    fresh = ((clock.time - timedelta(seconds=45)).isoformat(), clock())
    invalid = (None, (clock.time - timedelta(seconds=46)).isoformat(), (clock.time + timedelta(seconds=1)).isoformat())
    for times in product((*fresh, *invalid), repeat=3):
        sample = dict(zip(("sampled_at", "end_at", "group_at"), times))
        for pending in (False, True):
            axes, _ = states(
                source={"mode": "continuous_bounded", "enabled": True}, worker={}, task=None,
                sample={**sample, "status": "known", "recovery_pending": pending},
                derived={"durable_records_per_second": {"value": None}}, coverage={"status": "unknown"}, now=clock(),
            )
            expected = ("recovering" if pending else "available") if all(time in fresh for time in times) else "stale"
            assert axes["source"]["status"] == expected, (sample, pending)


def test_group_query_error_takes_precedence_over_missing_time_during_recovery(runtime, monkeypatch):
    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    read = runtime.broker.read

    def failed_group_query(*args, **kwargs):
        return replace(read(*args, **kwargs), group={}, group_at=None, group_error="authorization")

    monkeypatch.setattr(runtime.broker, "read", failed_group_query)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"] == {"status": "error", "reason_code": "authorization"}

    monkeypatch.setattr(runtime.broker, "read", read)
    runtime.clock.advance(15)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"]["status"] == "recovering"

    monkeypatch.setattr(runtime.broker, "read", failed_group_query)
    runtime.clock.advance(15)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["state"]["source"] == {"status": "error", "reason_code": "authorization"}

    monkeypatch.setattr(runtime.broker, "read", read)
    for expected in ("recovering", "available"):
        runtime.clock.advance(15)
        assert sampler.sample(source_id)
        assert service.detail(source_id)["state"]["source"]["status"] == expected


def test_physical_alias_legacy_cannot_consume_commit_or_duplicate_effects_while_continuous_runs(runtime, monkeypatch):
    source_id = setup(runtime)
    alias = {**SETTINGS, "bootstrap_env": "SYNTHETIC_LEGACY_ALIAS_ENV"}
    monkeypatch.setenv(alias["bootstrap_env"], "fixture.invalid:9092")
    old_source = runtime.worker.source_factory(alias)
    config_hash = hashlib.sha256((ROOT / "configs" / "drain3_recommended.ini").read_bytes()).hexdigest()
    original = runtime.state.create_or_load(descriptor=old_source.descriptor(), config_hash=config_hash)
    entered = threading.Event()
    release = threading.Event()
    errors = []

    def hold_poll():
        entered.set()
        if not release.wait(3):
            raise RuntimeError("fixture did not release Broker")

    def continuous():
        try:
            runtime.worker.step(source_id)
        except Exception as exc:
            errors.append(exc)

    runtime.broker.callbacks.append(hold_poll)
    thread = threading.Thread(target=continuous, daemon=True)
    thread.start()
    try:
        assert entered.wait(3)
        with pytest.raises(KafkaBoundaryError) as caught:
            runtime.worker.run_task(original["task_id"], alias, source=old_source)
        assert caught.value.code == "group_frontier_changed"
        assert runtime.broker.consumer_calls == 1
        assert runtime.broker.commit_calls == []
        assert runtime.broker.group == {0: None}
    finally:
        release.set()
        thread.join(4)
    assert not thread.is_alive()
    assert not errors
    with pytest.raises(KafkaBoundaryError, match="已绑定到连续来源"):
        runtime.worker.run_task(original["task_id"], alias, source=old_source)
    assert runtime.broker.group == {0: 2}
    assert runtime.broker.commit_calls == [{0: 1}, {0: 2}]
    assert not runtime.state.list_commits(original["task_id"])
    with runtime.database.connect() as connection:
        assert connection.execute("SELECT SUM(occurrence_count) FROM node_risk_ingestions").fetchone()[0] == 2


def test_large_backlog_publishes_multiple_bounded_result_frontiers(runtime):
    source_id = setup(runtime)
    runtime.broker.end = {0: 12}
    runtime.worker.max_batch_offsets = 5
    for start, target in ((0, 5), (5, 10), (10, 12)):
        task_id = runtime.worker.step(source_id)
        task = runtime.state.get_task(task_id)
        assert task["kafka_batch"]["start"] == {"0": start}
        assert task["kafka_batch"]["target"] == {"0": target}
        assert task["records_processed"] == target - start
        detail = ObservabilityService(runtime.repo, kafka_enabled=True).detail(source_id)
        assert detail["result"]["task_id"] == task_id
        assert detail["result"]["status"] == "covered"
    assert runtime.worker.step(source_id) is None


def test_attempt_deadline_keeps_original_target_and_reports_real_coverage_delay(runtime, monkeypatch):
    monkeypatch.setattr("logrisk.streaming_state.utc_now", runtime.clock)
    runtime.worker.monotonic = lambda: runtime.clock.time.timestamp()
    source_id = setup(runtime)
    runtime.broker.end = {0: 12}
    runtime.broker.callbacks.extend([lambda: None, lambda: runtime.clock.advance(121)])
    with pytest.raises(KafkaBoundaryError) as caught:
        runtime.worker.step(source_id)
    assert caught.value.code == "batch_time_budget_exceeded"
    active = runtime.repo.get(source_id)["active_task_id"]
    original = runtime.state.get_task(active)
    assert original["status"] == "interrupted"
    assert original["kafka_batch"]["target"] == {"0": 12}
    assert original["records_processed"] == 10
    assert not original.get("result_reference")
    snapshot = ObservabilityService(runtime.repo, kafka_enabled=True).detail(source_id)
    assert snapshot["result"]["status"] == "delayed"
    assert snapshot["result"]["wait_seconds"] == 121
    runtime.broker.end = {0: 13}
    assert runtime.worker.step(source_id) == active
    assert runtime.state.get_task(active)["cursor"]["value"]["partitions"] == {"0": 12}


def test_commit_timeout_points_to_submit_and_adapter_error_does_not_leak(runtime):
    source_id = setup(runtime)
    runtime.broker.failed = True
    with pytest.raises(KafkaBoundaryError) as caught:
        runtime.worker.step(source_id)
    assert "dummy-private-data" not in "".join(traceback.format_exception(caught.value))
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    result = service.detail(source_id)
    assert result["state"]["progress"] == {"status": "commit_failed", "reason_code": "external_commit_failed"}
    assert result["next_action"] == "inspect_pending_commit"
    assert result["source"]["block_stage"] == "group_commit"
    assert result["source"]["block_reason"] == "timeout"


def test_slow_broker_commit_only_serializes_its_own_control_transition(runtime, monkeypatch):
    import fcntl

    source_id = setup(runtime)
    other = runtime.worker.source_factory({**SETTINGS, "topic": "other-logs"})
    other_id = runtime.repo.register(other.descriptor(), initial_offset="earliest", exclusive_group=True)
    control = ObservabilityRepository(runtime.database, runtime.clock)
    epoch = control.get(source_id)["control_epoch"]
    ack_started = threading.Event()
    ack_release = threading.Event()
    stop_attempted = threading.Event()
    stop_done = threading.Event()
    other_done = threading.Event()
    commit_errors, control_errors = [], []
    original_flock = fcntl.flock

    def observed_flock(file, operation):
        if threading.current_thread() is stop_thread:
            stop_attempted.set()
        return original_flock(file, operation)

    monkeypatch.setattr(fcntl, "flock", observed_flock)

    def commit(values):
        ack_started.set()
        assert ack_release.wait(8)
        runtime.broker.group.update(values)
        raise TimeoutError("ack lost after application")

    runtime.broker.commit_hook = commit

    def run():
        try:
            runtime.worker.step(source_id)
        except Exception as exc:
            commit_errors.append(exc)

    def stop():
        try:
            control.set_enabled(source_id, False)
        except Exception as exc:
            control_errors.append(exc)
        finally:
            stop_done.set()

    def unrelated_write():
        try:
            control.set_enabled(other_id, True)
        except Exception as exc:
            control_errors.append(exc)
        finally:
            other_done.set()

    worker_thread = threading.Thread(target=run, daemon=True)
    stop_thread = threading.Thread(target=stop, daemon=True)
    other_thread = threading.Thread(target=unrelated_write, daemon=True)
    worker_thread.start()
    try:
        assert ack_started.wait(5)
        stop_thread.start()
        assert stop_attempted.wait(2)
        other_thread.start()
        assert other_done.wait(2)
        assert not control_errors
        assert control.get(other_id)["enabled"]
        assert not stop_done.is_set()
        row = control.get(source_id)
        assert row["enabled"] and row["control_epoch"] == epoch
        task_id = row["active_task_id"]
        task = runtime.state.get_task(task_id)
        assert task["pending_external_commit"]["value"]["partitions"] == {"0": 1}
    finally:
        ack_release.set()
        for thread in (worker_thread, stop_thread, other_thread):
            if thread.ident is not None:
                thread.join(6)
    assert not any(thread.is_alive() for thread in (worker_thread, stop_thread, other_thread))
    assert not control_errors
    assert len(commit_errors) == 1 and commit_errors[0].code == "timeout"
    stopped = control.get(source_id)
    assert not stopped["enabled"] and stopped["control_epoch"] == epoch + 1
    assert stopped["active_task_id"] == task_id
    assert runtime.state.get_task(task_id)["pending_external_commit"] == task["pending_external_commit"]
    assert runtime.worker.step(source_id) is None
    assert runtime.broker.commit_calls == [{0: 1}]

    runtime.broker.commit_hook = None
    runtime.broker.end = {0: 3}
    control.set_enabled(source_id, True)
    assert runtime.worker.step(source_id) == task_id
    completed = runtime.state.get_task(task_id)
    assert completed["kafka_batch"]["target"] == {"0": 2}
    assert completed["records_processed"] == 2
    assert completed["pending_external_commit"] is None
    assert runtime.broker.commit_calls == [{0: 1}, {0: 2}]


def test_control_epoch_change_before_commit_gate_fences_worker(runtime, monkeypatch):
    source_id = setup(runtime)
    initial_epoch = runtime.repo.get(source_id)["control_epoch"]
    original_gate = runtime.repo.commit_control

    @contextmanager
    def stop_and_restart_before_commit(id):
        runtime.repo.set_enabled(id, False)
        runtime.repo.set_enabled(id, True)
        with original_gate(id) as current:
            yield current

    monkeypatch.setattr(runtime.repo, "commit_control", stop_and_restart_before_commit)
    assert runtime.worker.step(source_id) is None
    assert not runtime.broker.commit_calls
    row = runtime.repo.get(source_id)
    assert row["enabled"] and row["control_epoch"] == initial_epoch + 2
    pending = runtime.state.get_task(row["active_task_id"])
    assert pending["status"] == "interrupted"
    assert pending["pending_external_commit"]["value"]["partitions"] == {"0": 1}

    monkeypatch.setattr(runtime.repo, "commit_control", original_gate)
    assert runtime.worker.step(source_id) == row["active_task_id"]
    assert runtime.broker.commit_calls == [{0: 1}, {0: 2}]


def test_unknown_source_control_does_not_create_a_lock_file(runtime):
    directory = runtime.database.path.parent
    locks = list(directory.glob(".*.kafka-*.lock"))
    with pytest.raises(ObservabilityError) as caught:
        runtime.repo.set_enabled("unknown-source", False)
    assert caught.value.code == "source_not_found"
    assert list(directory.glob(".*.kafka-*.lock")) == locks


@pytest.mark.parametrize("error_code", ["timeout", "broker_unavailable"])
def test_continuous_scheduler_automatically_recovers_pending_commit(runtime, error_code):
    from kafka.errors import NetworkExceptionError

    source_id = setup(runtime)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    failure = {"timeout": TimeoutError, "broker_unavailable": NetworkExceptionError}[error_code]
    phases, completed = [], []
    task_id = None

    def fail_first_commit(_):
        if len(runtime.broker.commit_calls) == 1:
            raise failure()

    runtime.broker.commit_hook = fail_first_commit

    def wait(seconds):
        nonlocal task_id
        runtime.clock.advance(seconds)
        snapshot = service.detail(source_id)
        phase = snapshot["worker"]["scheduler"]["phase"]
        phases.append(phase)
        if phase == "retry_backoff":
            assert snapshot["state"]["worker"] == {"status": "waiting", "reason_code": "scheduler_retry_backoff"}
            assert snapshot["source"]["block_reason"] == error_code
            task_id = snapshot["task"]["task_id"]
            original = runtime.state.get_task(task_id)
            assert original["kafka_batch"]["target"] == {"0": 2}
            assert original["pending_external_commit"]["value"]["partitions"] == {"0": 1}
            runtime.broker.end = {0: 3}
        else:
            runtime.repo.set_enabled(source_id, False)

    runtime.worker.run(source_id, report=completed.append, wait=wait)
    assert phases == ["retry_backoff"] * 5 + ["awaiting_batch"]
    assert completed == [task_id]
    assert runtime.state.get_task(task_id)["records_processed"] == 2
    assert runtime.broker.commit_calls == [{0: 1}, {0: 1}, {0: 2}]
    assert runtime.repo.get(source_id)["scheduler_token"] is None


def test_transient_backoff_caps_and_resets_after_healthy_pass(runtime, monkeypatch):
    source_id = setup(runtime)
    outcomes = iter(["broker_unavailable"] * 6 + [None, "timeout"])
    pauses = []

    def step(*args, **kwargs):
        pauses.append(0)
        code = next(outcomes)
        if code:
            raise KafkaBoundaryError(code)

    def wait(seconds):
        pauses[-1] += seconds
        if len(pauses) == 8 and pauses[-1] == 5:
            runtime.repo.set_enabled(source_id, False)

    monkeypatch.setattr(runtime.worker, "step", step)
    runtime.worker.run(source_id, wait=wait)
    assert pauses == [5, 10, 20, 40, 60, 60, 30, 5]


@pytest.mark.parametrize(("once", "error_code"), [
    (True, "timeout"), (False, "authentication"), (False, "authorization"),
    (False, "topic_identity_changed"), (False, "group_frontier_changed"),
])
def test_worker_fails_closed_for_single_attempt_or_structural_failure(runtime, once, error_code):
    source_id = setup(runtime)

    def fail(_):
        raise KafkaBoundaryError(error_code)

    runtime.broker.commit_hook = fail
    with pytest.raises(KafkaBoundaryError) as caught:
        runtime.worker.run(source_id, once=once, wait=lambda _: pytest.fail("unexpected retry"))
    assert caught.value.code == error_code
    assert len(runtime.broker.commit_calls) == 1
    assert runtime.repo.get(source_id)["scheduler_token"] is None


def test_classifies_protocol_timeout_and_connection_failure():
    from kafka.errors import InvalidSessionTimeoutError, InvalidTransactionTimeoutError, KafkaTimeoutError, RequestTimedOutError

    assert classify(RequestTimedOutError()) == "timeout"
    assert classify(KafkaTimeoutError()) == "timeout"
    assert classify(TimeoutError()) == "timeout"
    assert classify(ConnectionResetError()) == "broker_unavailable"
    assert classify(InvalidSessionTimeoutError()) == "consumer_failed"
    assert classify(InvalidTransactionTimeoutError()) == "consumer_failed"
    assert classify(OSError()) == "consumer_failed"


def test_sampler_durable_rate_uses_the_database_observation_interval(runtime, monkeypatch):
    source_id = setup(runtime)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    real_facts = runtime.repo.facts
    task = {"task_id": "fixture-task", "records_processed": 200, "kafka_batch": {"identity": runtime.broker.identity}}
    worker = {"worker_epoch": "fixture-worker"}

    def facts(id):
        source, previous, _, _, _, evidence = real_facts(id)
        return source, previous, task, {}, worker, evidence

    def read(*args, **kwargs):
        sample = real_read(*args, **kwargs)
        runtime.clock.advance(db_delay)
        return sample

    real_read = runtime.broker.read
    db_delay = 0
    monkeypatch.setattr(runtime.repo, "facts", facts)
    monkeypatch.setattr(runtime.broker, "read", read)
    assert sampler.sample(source_id)
    runtime.clock.advance(15)
    db_delay = 4
    task["records_processed"] += 190
    assert sampler.sample(source_id)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    derived = service.detail(source_id)["derived"]["durable_records_per_second"]
    assert derived == {"value": 10.0, "unit": "records/s", "reason_code": None}
    sample = real_facts(source_id)[1]
    baseline = sample["previous"]
    assert durable_rate(sample, baseline, runtime.clock()) == derived
    for field, bad in [
        ("durable_at", None), ("durable_at", "not-a-time"), ("durable_at", sample["durable_at"]),
        ("durable_task_id", "another-task"), ("durable_epoch", "another-topic"),
        ("worker_epoch", "another-worker"), ("partitions", [1]), ("records_processed", 999),
    ]:
        invalid = durable_rate(sample, {**baseline, field: bad}, runtime.clock())
        assert invalid["reason_code"] == "baseline_required" and invalid["value"] is None
    runtime.clock.advance(46)
    assert service.detail(source_id)["derived"]["durable_records_per_second"]["value"] is None


def test_durable_rate_uses_pinned_snapshot_time_and_hides_after_worker_reclaim(runtime, monkeypatch):
    source_id = setup(runtime)
    row = runtime.repo.get(source_id)
    runtime.source.control_epoch = row["control_epoch"]
    adapter = runtime.source.adapters[runtime.source.configuration["adapter_id"]]
    task_id = runtime.worker._prepare(row, runtime.source, adapter)["task_id"]
    lease = runtime.state.claim_task(task_id, kafka_control_epoch=row["control_epoch"])["lease_token"]
    assert runtime.repo.worker_publish(task_id, lease, {"worker_epoch": lease, "last_heartbeat_at": runtime.clock()})
    first_snapshot_at = runtime.clock()

    def update_count(count):
        with runtime.database.transaction() as connection:
            raw = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()[0]
            task = json.loads(raw)
            task["records_processed"] = count
            connection.execute("UPDATE streaming_tasks SET task_json=? WHERE task_id=?", (json.dumps(task), task_id))

    actual_snapshot = runtime.repo.observer_snapshot
    read_calls = 0

    def slow_snapshot(connection, id):
        nonlocal read_calls
        read_calls += 1
        if read_calls == 2:
            # The transaction has selected the source, task and worker. A later
            # concurrent write must not affect the timestamp assigned to its snapshot.
            runtime.clock.advance(10)
            update_count(100)
        return actual_snapshot(connection, id)

    monkeypatch.setattr(runtime.repo, "observer_snapshot", slow_snapshot)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    assert sampler.sample(source_id)
    runtime.clock.advance(15)
    update_count(250)
    assert sampler.sample(source_id)
    _, sample, *_ = runtime.repo.facts(source_id)
    assert sample["previous"]["durable_at"] == first_snapshot_at
    assert sample["previous"]["records_processed"] == 0
    assert sample["durable_at"] == runtime.clock()

    def displayed_rate():
        return ObservabilityService(runtime.repo, kafka_enabled=True).detail(source_id)["derived"]["durable_records_per_second"]

    assert displayed_rate()["value"] == 10.0
    runtime.state.finish_claim(task_id, lease, error="test interrupted", interrupted=True)
    assert displayed_rate()["value"] == 10.0
    claim = runtime.state.claim_task(task_id, kafka_control_epoch=row["control_epoch"])
    assert claim["lease_token"] != lease
    assert displayed_rate() == {"value": None, "unit": "records/s", "reason_code": "baseline_required"}


def test_durable_rate_rejects_wide_first_snapshot_select_without_discarding_counts(runtime, monkeypatch):
    source_id = setup(runtime)
    row = runtime.repo.get(source_id)
    runtime.source.control_epoch = row["control_epoch"]
    adapter = runtime.source.adapters[runtime.source.configuration["adapter_id"]]
    task_id = runtime.worker._prepare(row, runtime.source, adapter)["task_id"]
    lease = runtime.state.claim_task(task_id, kafka_control_epoch=row["control_epoch"])["lease_token"]
    assert runtime.repo.worker_publish(task_id, lease, {"worker_epoch": lease, "last_heartbeat_at": runtime.clock()})

    def update_count(count):
        with runtime.database.transaction() as connection:
            raw = connection.execute("SELECT task_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()[0]
            task = json.loads(raw)
            task["records_processed"] = count
            connection.execute("UPDATE streaming_tasks SET task_json=? WHERE task_id=?", (json.dumps(task), task_id))

    original_get = runtime.repo.get
    first_sample_at = runtime.clock()
    source_reads = 0

    def delayed_get(id, connection=None, *, lock=False):
        nonlocal source_reads
        source = original_get(id, connection, lock=lock)
        if connection is not None and not lock:
            source_reads += 1
            if source_reads == 2:
                runtime.clock.advance(10)
                update_count(100)
        return source

    monkeypatch.setattr(runtime.repo, "get", delayed_get)
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    assert sampler.sample(source_id)
    _, first, *_ = runtime.repo.facts(source_id)
    assert first["durable_at"] is None
    assert first["records_processed"] == 0
    assert first["status"] == "known" and first["end_at"] == first["group_at"] == first["sampled_at"] == first_sample_at
    assert service.detail(source_id)["state"]["source"] == {"status": "available", "reason_code": None}

    runtime.clock.advance(15)
    update_count(250)
    assert sampler.sample(source_id)
    _, second, *_ = runtime.repo.facts(source_id)
    assert second["durable_at"] == runtime.clock()
    assert second["records_processed"] == 250
    assert second["previous"]["durable_at"] is None
    assert service.detail(source_id)["derived"]["durable_records_per_second"] == {
        "value": None, "unit": "records/s", "reason_code": "baseline_required",
    }

    runtime.clock.advance(15)
    update_count(400)
    assert sampler.sample(source_id)
    assert service.detail(source_id)["derived"]["durable_records_per_second"]["value"] == 10.0


@pytest.mark.parametrize(("before", "after"), [
    ("unavailable", "2026-11-17T06:30:00+00:00"),
    ("2026-11-17T06:30:00+00:00", "unavailable"),
    ("2026-11-17T06:30:01+00:00", "2026-11-17T06:30:00+00:00"),
])
def test_facts_rejects_malformed_or_reversed_snapshot_bracket(runtime, monkeypatch, before, after):
    source_id = setup(runtime)
    monkeypatch.setattr(runtime.repo, "clock", iter((before, after)).__next__)
    *_, evidence = runtime.repo.facts(source_id)
    assert evidence["durable_observed_at"] is None


def test_durable_rate_hides_when_new_batch_is_queued_before_next_sample(runtime):
    source_id = setup(runtime)
    runtime.broker.end = {0: 4}
    runtime.worker.max_batch_offsets = 3
    sampler = KafkaSampler(runtime.repo, runtime.broker)
    runtime.broker.callbacks.append(lambda: sampler.sample(source_id))

    def sample_in_commit(values):
        if values == {0: 3}:
            runtime.clock.advance(15)
            assert sampler.sample(source_id)

    runtime.broker.commit_hook = sample_in_commit
    previous_task = runtime.worker.step(source_id)
    service = ObservabilityService(runtime.repo, kafka_enabled=True)
    assert service.detail(source_id)["derived"]["durable_records_per_second"]["value"] == 0.2

    row = runtime.repo.get(source_id)
    runtime.source.control_epoch = row["control_epoch"]
    adapter = runtime.source.adapters[runtime.source.configuration["adapter_id"]]
    next_task = runtime.worker._prepare(row, runtime.source, adapter)
    assert next_task["task_id"] != previous_task and next_task["status"] == "queued"
    assert service.detail(source_id)["derived"]["durable_records_per_second"] == {
        "value": None, "unit": "records/s", "reason_code": "baseline_required",
    }


def test_sensitive_metadata_failure_after_claim_is_not_exposed_in_task_api(runtime, monkeypatch):
    source_id = setup(runtime)
    canary = "PRIVATE-DUMMY-BROKER-AND-CREDENTIAL-CANARY"
    original = runtime.broker.read
    seen = []

    def throwing(configuration, **kwargs):
        seen.append(True)
        if len(seen) == 2:
            raise TimeoutError(canary)
        return original(configuration, **kwargs)

    monkeypatch.setattr(runtime.broker, "read", throwing)
    with pytest.raises(KafkaBoundaryError) as caught:
        runtime.worker.step(source_id)
    assert caught.value.code == "timeout"
    facade = ApiFacade(SimpleNamespace(
        runtime_config=SimpleNamespace(identity=SimpleNamespace(enabled=False)),
        streaming_state=runtime.state,
    ), version="1.45.0")
    assert canary not in json.dumps(facade.dispatch_read("/api/streaming/tasks").body)
    assert canary not in "".join(traceback.format_exception(caught.value))
    active = runtime.state.get_task(runtime.repo.get(source_id)["active_task_id"])
    assert active["error_details"]["error_code"] == "timeout"


@pytest.mark.parametrize("boundary", ("admin_init", "metadata_start", "metadata_end", "consumer_init", "beginning", "end", "group"))
def test_read_only_adapter_classifies_transport_failures_without_sensitive_text(boundary, monkeypatch):
    canary = "PRIVATE-TRANSPORT-FIXTURE"
    monkeypatch.setenv(SETTINGS["bootstrap_env"], "fixture.invalid:9092")
    metadata_calls = []

    def raise_at(point):
        if point == boundary:
            raise TimeoutError(canary)

    class Admin:
        def __init__(self, **kwargs):
            raise_at("admin_init")

        def list_consumer_group_offsets(self, group, *, partitions):
            raise_at("group")
            return {}

        def close(self):
            pass

    class Consumer:
        def __init__(self, **kwargs):
            raise_at("consumer_init")

        def beginning_offsets(self, partitions):
            raise_at("beginning")
            return {partitions[0]: 0}

        def end_offsets(self, partitions):
            raise_at("end")
            return {partitions[0]: 1}

        def close(self, *, autocommit):
            pass

    def metadata(*args):
        raise_at("metadata_end" if metadata_calls else "metadata_start")
        metadata_calls.append(True)
        return "identity", {0}

    reader = KafkaOffsetReader(consumer_factory=Consumer, admin_factory=Admin, metadata_reader=metadata)
    with pytest.raises(KafkaBoundaryError) as caught:
        reader.read(SETTINGS, require_beginning=True)
    assert caught.value.code == "timeout"
    assert canary not in "".join(traceback.format_exception(caught.value))


def test_scheduler_drains_saturated_batches_until_a_bounded_idle_check(runtime):
    source_id = setup(runtime)
    runtime.worker.max_batch_offsets = 2
    runtime.broker.end = {0: 6}
    completed = []

    def stop_at_idle(seconds):
        assert seconds == 1 and len(completed) == 3
        assert runtime.broker.group == {0: 6}
        runtime.repo.set_enabled(source_id, False)

    runtime.worker.run(source_id, report=completed.append, wait=stop_at_idle)
    assert [runtime.state.get_task(task)["records_processed"] for task in completed] == [2, 2, 2]


def test_unusable_kafka_payload_is_audited_before_broker_commit_and_later_input_continues(runtime):
    source_id = setup(runtime)
    runtime.broker.end = {0: 3}
    runtime.broker.values[0, 0] = b'{"secret":' + b"[" * 2000 + b'"fixture-private-value"' + b"]" * 2000 + b"}"
    runtime.broker.values[0, 1] = b'{"node":{"fixture-private-value":true},"message":"bad shape"}'
    checks = []

    def inspect_durability(offsets):
        task_id = runtime.repo.get(source_id)["active_task_id"]
        task = runtime.state.get_task(task_id)
        entries = runtime.repo.events(source_id, cursor=None, limit=20)["events"]
        dispositions = [pos for row in entries for pos in row.get("rejected_positions", [])]
        checks.append((offsets[0], task["records_rejected"], dispositions))
        assert "fixture-private-value" not in repr(entries)

    runtime.broker.commit_hook = inspect_durability
    task_id = runtime.worker.step(source_id)
    assert checks[0] == (1, 1, [{"partition": 0, "offset": "0", "reason_code": "record_decode_failed"}])
    assert checks[1][0:2] == (2, 2)
    assert checks[1][2][0] == {"partition": 0, "offset": "1", "reason_code": "record_shape_invalid"}
    task = runtime.state.get_task(task_id)
    assert task["status"] == "completed" and task["records_processed"] == 1
    assert task["records_rejected"] == 2 and runtime.broker.group == {0: 3}


def test_managed_record_deltas_reuse_one_vector_and_materialize_independent_checkpoints(runtime):
    from logrisk.incremental_sources import SourceCursor
    from logrisk.kafka_records import KafkaCursorAccumulator, KafkaCursorDelta

    runtime.broker.end = {partition: 0 for partition in range(256)}
    runtime.broker.end[19] = 3
    runtime.broker.beginning = dict.fromkeys(runtime.broker.end, 0)
    runtime.broker.group = dict.fromkeys(runtime.broker.end)
    source_id = setup(runtime)
    captured = []
    original = runtime.worker.run_task

    def check_deltas(task_id, configuration, *, source):
        position = SourceCursor.empty()
        items = list(source.read(position))
        deltas = [item.next_cursor for item in items]
        assert len(deltas) == 3 and all(isinstance(item, KafkaCursorDelta) for item in deltas)
        assert all(item.bounds is deltas[0].bounds for item in deltas)
        assert [item.next_offset for item in deltas] == [1, 2, 3]
        assert deltas[0].bounds.initial_positions[19] == 0
        accumulator = KafkaCursorAccumulator(deltas[0], position)
        for item in deltas:
            accumulator.apply(item)
            captured.append(accumulator.checkpoint())
        return original(task_id, configuration, source=source)

    runtime.worker.run_task = check_deltas
    runtime.worker.step(source_id)
    assert [item.value["partitions"]["19"] for item in captured] == [1, 2, 3]
    assert all(len(item.value["partitions"]) == 256 for item in captured)
