"""Merge these values into the host Django settings; secrets remain in environment variables."""
from __future__ import annotations

import os


INSTALLED_APPS += ["logrisk_django"]

LOGRISK = {
    "project_root": os.environ["LOGRISK_PROJECT_ROOT"],
    "state_root": os.environ["LOGRISK_STATE_ROOT"],
    "output_root": os.environ["LOGRISK_OUTPUT_ROOT"],
    "database_provider": "postgres",
    "database_url_env": "LOGRISK_DATABASE_URL",
    "shared_root": os.environ["LOGRISK_SHARED_ROOT"],
    "airflow_base_url": os.environ["LOGRISK_AIRFLOW_URL"],
    "airflow_dag_id": "logrisk_analysis",
    "airflow_authorization_env": "LOGRISK_AIRFLOW_TOKEN",
    "identity_resolver": "company.pacas.LogriskIdentityResolver",
    "write_roles": ["logrisk:operator"],
}
