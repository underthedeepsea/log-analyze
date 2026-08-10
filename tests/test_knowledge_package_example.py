from __future__ import annotations

from pathlib import Path


def test_tracked_example_package_contains_all_asset_types_and_builds(tmp_path: Path) -> None:
    from logrisk.knowledge_packages.archive import build_archive

    source = Path("examples/knowledge_packages/linux_node_baseline")
    manifest = (source / "manifest.json").read_text(encoding="utf-8")
    for asset_type in ("drain3_profile", "semantic_dictionary", "feature_prompt", "risk_semantics", "approved_rule_candidates", "gold_dataset"):
        assert f'"type": "{asset_type}"' in manifest
    inspection = build_archive(source, tmp_path / "linux-node-baseline.logrisk-package.zip")
    assert inspection.manifest.package_id == "linux-node-baseline"
    assert len(inspection.manifest.assets) == 6
    assert inspection.expanded_bytes > 0


def test_example_gold_dataset_contains_only_sanitized_evidence() -> None:
    content = Path("examples/knowledge_packages/linux_node_baseline/assets/gold_dataset.jsonl").read_text(encoding="utf-8")
    assert "raw_sample" not in content
    assert "samples" not in content
    assert "kernel registration failed <NUM>" in content
