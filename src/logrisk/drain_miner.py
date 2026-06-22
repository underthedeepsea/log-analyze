from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, Tuple

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig


class Drain3ShardManager:
    def __init__(self, config_path: str | Path, state_dir: str | Path):
        self.config_path = Path(config_path)
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._miners: Dict[Tuple[str, str, str], TemplateMiner] = {}

    def _load_config(self) -> TemplateMinerConfig:
        config = TemplateMinerConfig()
        config.load(str(self.config_path))
        # Some drain3 versions may load this value as string through ConfigParser.
        try:
            config.parameter_extraction_cache_capacity = int(config.parameter_extraction_cache_capacity)
        except Exception:
            pass
        return config

    def get_miner(self, cluster: str, source_type: str, component: str) -> TemplateMiner:
        key = (cluster, source_type, component)
        if key in self._miners:
            return self._miners[key]
        safe = "__".join(x.replace("/", "_").replace(" ", "_") for x in key)
        state_file = self.state_dir / f"{safe}.bin"
        miner = TemplateMiner(
            persistence_handler=FilePersistence(str(state_file)),
            config=self._load_config(),
        )
        self._miners[key] = miner
        return miner


def stable_template_hash(cluster: str, source_type: str, component: str, template: str) -> str:
    raw = f"{cluster}|{source_type}|{component}|{template}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def mine_template_event(record: Dict[str, Any], shard_manager: Drain3ShardManager) -> Dict[str, Any]:
    cluster = str(record.get("cluster") or "default")
    source_type = str(record.get("source_type") or "unknown")
    component = str(record.get("component") or "unknown")
    message_core = str(record["message_core"])

    miner = shard_manager.get_miner(cluster, source_type, component)
    result = miner.add_log_message(message_core)
    template = result["template_mined"]
    template_hash = stable_template_hash(cluster, source_type, component, template)

    extracted = miner.extract_parameters(template, message_core, exact_matching=True)
    if extracted is None:
        extracted = miner.extract_parameters(template, message_core, exact_matching=False)

    parameters = [
        {"type": p.mask_name, "value": p.value}
        for p in (extracted or [])
    ]

    return {
        "event_id": record.get("raw_log_id"),
        "timestamp": record.get("timestamp"),
        "cluster": cluster,
        "node": record.get("node"),
        "namespace": record.get("namespace"),
        "pod": record.get("pod"),
        "container": record.get("container"),
        "source_type": source_type,
        "component": component,
        "severity": record.get("severity"),
        "template_hash": template_hash,
        "template": template,
        "parameters": parameters,
        "message_core": message_core,
        "raw_sample": record.get("raw_log"),
        "change_type": result.get("change_type"),
    }


def mine_template_events(records: list[Dict[str, Any]], config_path: str | Path, state_dir: str | Path) -> list[Dict[str, Any]]:
    manager = Drain3ShardManager(config_path=config_path, state_dir=state_dir)
    return [mine_template_event(r, manager) for r in records]
