from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from logrisk.application.container import ApplicationConfig, build_application_container
from logrisk.database import DatabaseError, create_database
from logrisk.database_config import DatabaseConnectionSettings, resolve_database_runtime
from logrisk.incremental_sources import KafkaIncrementalSource, parse_kafka_enabled
from logrisk.kafka_adapter import KAFKA_PYTHON_ADAPTER_ID
from logrisk.streaming_observability.continuous import ContinuousKafka
from logrisk.streaming_observability.kafka_offsets import KafkaBoundaryError, KafkaOffsetReader
from logrisk.streaming_observability.repository import ObservabilityError, ObservabilityRepository, json_text
from logrisk.streaming_observability.sampler import KafkaSampler


def arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka 登记、只读采样与连续有界批次")
    parser.add_argument("--database", type=Path, help="SQLite 数据库路径")
    parser.add_argument("--database-provider", choices=("sqlite", "postgres"))
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("register", help="登记来源，不连接 Broker")
    register.add_argument("--topic", required=True)
    register.add_argument("--group", required=True)
    register.add_argument("--cluster-id", required=True)
    register.add_argument("--bootstrap-env", required=True)
    register.add_argument("--environment", default="production")
    register.add_argument("--source-profile")
    register.add_argument("--initial-offset", choices=("earliest", "latest", "committed"), required=True)
    register.add_argument("--exclusive-group", action="store_true", required=True)
    commands.add_parser("start", help="允许后续 Worker 调度该来源").add_argument("source_id")
    commands.add_parser("stop", help="禁止后续处理并保留当前批次").add_argument("source_id")
    run = commands.add_parser("run", help="启动连续批次 Worker")
    run.add_argument("source_id")
    run.add_argument("--once", action="store_true")
    sample = commands.add_parser("sample", help="显式执行独立只读采样")
    sample.add_argument("source_id")
    sample.add_argument("--watch", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    root = Path(__file__).resolve().parents[3]
    database_path = Path(args.database or os.getenv("LOGRISK_DB_PATH") or root / "state" / "logrisk.sqlite3")
    state = (args.state_root or database_path.parent).resolve()
    # The library may log raw bootstrap configuration at DEBUG. The command's
    # diagnostic output uses application classifications without endpoints.
    logging.getLogger("kafka").setLevel(logging.CRITICAL + 1)
    try:
        runtime = resolve_database_runtime(
            provider=args.database_provider, database_path=database_path,
            settings=DatabaseConnectionSettings(state / "database_connection.json"),
        )
        if args.command == "run":
            if not parse_kafka_enabled(os.getenv("LOGRISK_KAFKA_ENABLED")):
                raise ObservabilityError("worker_disabled", "启动 Worker 需显式设置 LOGRISK_KAFKA_ENABLED=1")
            container = build_application_container(ApplicationConfig(
                project_root=root, state_root=state, output_root=args.output_root or root / "output",
                database_provider=runtime.provider, database_path=runtime.sqlite_path, database_url=runtime.database_url,
                kafka_enabled=True, feature_jobs_auto_start=False, interrupt_feature_jobs=False, migrate_database=False,
            ))
            repository = ObservabilityRepository(container.database)
            worker = ContinuousKafka(
                repository, container.streaming_state, source_factory=container.kafka_source,
                run_task=container.run_kafka_task, config_snapshot=container.drain_quality.configs.active_snapshot,
            )
            worker.run(args.source_id, once=args.once, report=lambda task_id: print(
                json_text({"source_id": args.source_id, "task_id": task_id}), flush=True,
            ))
            return 0

        database = create_database(
            provider=runtime.provider, sqlite_path=runtime.sqlite_path, state_root=state,
            database_url=runtime.database_url, migrate=args.command == "register" and runtime.provider == "sqlite",
        )
        repository = ObservabilityRepository(database)
        if args.command == "register":
            configuration = {
                "adapter_id": KAFKA_PYTHON_ADAPTER_ID, "topic": args.topic, "consumer_group": args.group,
                "cluster_id": args.cluster_id, "bootstrap_env": args.bootstrap_env,
            }
            if args.source_profile:
                configuration["source_profile"] = args.source_profile
            descriptor = KafkaIncrementalSource(configuration, environment=args.environment).descriptor()
            source_id = repository.register(
                descriptor, initial_offset=args.initial_offset, exclusive_group=args.exclusive_group,
            )
            print(json_text({"source_id": source_id, "enabled": bool(repository.get(source_id)["enabled"])}))
        elif args.command == "sample":
            sampler = KafkaSampler(repository, KafkaOffsetReader())
            while True:
                published = sampler.sample(args.source_id)
                status, delay = repository.observer_schedule(args.source_id)
                print(json_text({"source_id": args.source_id, "snapshot_published": published, "status": status}), flush=True)
                if not args.watch:
                    break
                time.sleep(delay)
        else:
            repository.set_enabled(args.source_id, args.command == "start")
            print(json_text({"source_id": args.source_id, "enabled": args.command == "start"}))
    except KeyboardInterrupt:
        return 130
    except (ObservabilityError, DatabaseError) as exc:
        print(json_text({"code": getattr(exc, "code", "database_unavailable"), "error": str(exc)}), file=sys.stderr)
        return 1
    except KafkaBoundaryError as exc:
        print(json_text({"code": exc.code, "error": str(exc)}), file=sys.stderr)
        return 1
    except Exception:
        print(json_text({"code": "operation_failed", "error": "Kafka 操作失败，请检查来源状态与受控运行环境"}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
