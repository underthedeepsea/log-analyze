from __future__ import annotations

import pytest
from logrisk.miner_generations import prepare_generation, seal_generation


def test_committed_generation_is_immutable_and_uncommitted_generation_is_ignored(tmp_path):
    first = prepare_generation(tmp_path,None)
    (first / "miner.bin").write_bytes(b"committed")
    manifest = seal_generation(first)
    orphan = prepare_generation(tmp_path,manifest)
    (orphan / "miner.bin").write_bytes(b"uncommitted")
    resumed = prepare_generation(tmp_path,manifest)
    assert (resumed / "miner.bin").read_bytes() == b"committed"
    (first / "miner.bin").write_bytes(b"corrupt")
    with pytest.raises(ValueError,match="mismatch"):
        prepare_generation(tmp_path,manifest)
