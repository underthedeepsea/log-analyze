"""Place this importable DAG entry in the Airflow 2.3.2 DAG search path."""
from integrations.airflow.dags.logrisk_analysis import dag


__all__ = ["dag"]
