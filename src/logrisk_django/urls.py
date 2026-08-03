from __future__ import annotations

from django.urls import path

from logrisk_django.views.api import core_read
from logrisk_django.views.jobs import create_job


app_name = "logrisk_django"
urlpatterns = [
    path("api/jobs", create_job),
    path("api/health", core_read, {"endpoint": "health"}),
    path("api/runtime/readiness", core_read, {"endpoint": "runtime/readiness"}),
    path("api/ai-harness/model-profiles", core_read, {"endpoint": "ai-harness/model-profiles"}),
    path("api/ai-harness/prompts", core_read, {"endpoint": "ai-harness/prompts"}),
    path("api/rule-governance/rules", core_read, {"endpoint": "rule-governance/rules"}),
    path("api/release-readiness", core_read, {"endpoint": "release-readiness"}),
]
