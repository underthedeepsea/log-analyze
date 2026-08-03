"""Airflow deployment import surface; implementation remains in the installable LOGRISK package."""

from logrisk.airflow_tasks import (
    drain_partition,
    extract_feature_batch,
    finalize_job,
    list_drain_partitions,
    list_feature_batches,
    merge_templates,
    prepare_job,
    preprocess_input,
    preprocess_uploaded_input,
    score_and_reuse,
    validate_candidates,
)

__all__ = [
    "drain_partition", "extract_feature_batch", "finalize_job", "list_drain_partitions",
    "list_feature_batches", "merge_templates", "prepare_job", "preprocess_input", "preprocess_uploaded_input",
    "score_and_reuse", "validate_candidates",
]
