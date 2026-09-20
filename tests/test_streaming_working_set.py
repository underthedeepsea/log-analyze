from __future__ import annotations

import inspect

from logrisk.large_file_pipeline import _run_checkpointed_source_batches


def test_checkpoint_pipeline_persists_batches_instead_of_accumulating_all_windows():
    source = inspect.getsource(_run_checkpointed_source_batches)
    assert "all_windows.extend" not in source
    assert "iter_committed_windows" in source
