"""Copy this DAG entrypoint into Airflow's configured DAG directory."""

from integrations.airflow.dags.logrisk_input_preprocess import dag

__all__ = ["dag"]
