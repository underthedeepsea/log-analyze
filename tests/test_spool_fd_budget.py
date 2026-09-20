from __future__ import annotations

import json

from logrisk.partition_spool import spool_normalized_records


def test_spool_writer_lru_reopens_in_append_mode_without_truncation(tmp_path):
    records = []
    for cycle in range(2):
        for index in range(20):
            records.append({"message": f"row {cycle}-{index}", "node": f"node-{index}", "component": "kernel"})

    manifest = spool_normalized_records(records, spool_dir=tmp_path, max_open_files=3)

    assert sum(item["record_count"] for item in manifest["partitions"]) == 40
    assert all(len((tmp_path / item["path"]).read_text(encoding="utf-8").splitlines()) == 2 for item in manifest["partitions"])
    assert manifest["max_open_files"] == 3
