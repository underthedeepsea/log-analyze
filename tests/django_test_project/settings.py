SECRET_KEY = "logrisk-test-only"
INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "logrisk_django.apps.LogriskDjangoAppConfig",
]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
ROOT_URLCONF = "tests.django_test_project.urls"
USE_TZ = True
MIDDLEWARE = []
LOGRISK = {
    "project_root": ".",
    "state_root": "/tmp/logrisk-django-test-state",
    "output_root": "/tmp/logrisk-django-test-output",
    "database_provider": "sqlite",
    "shared_root": "/tmp/logrisk-django-test-shared",
    "airflow_base_url": "http://127.0.0.1:18080",
    "airflow_dag_id": "logrisk_analysis",
    "airflow_authorization_env": "LOGRISK_AIRFLOW_AUTHORIZATION",
    "write_roles": ["logrisk:operator"],
}
