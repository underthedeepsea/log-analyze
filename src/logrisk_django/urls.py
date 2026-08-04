from __future__ import annotations

from django.urls import path

from logrisk_django.views.api import core_read
from logrisk_django.views.benchmark import cancel_run, compare, evaluate_gate, runs, suites
from logrisk_django.views.drain import annotation_review, annotations, configs, datasets, eval_runs, tune_runs
from logrisk_django.views.dictionary import dictionary_action, dictionary_candidate, dictionary_collection, dictionary_detail, dictionary_test
from logrisk_django.views.frontend import frontend
from logrisk_django.views.governance import (
    export_approved,
    rule_action,
    rule_detail,
    rule_review_queue,
    update_feature,
    validate_release,
)
from logrisk_django.views.harness import (
    connections,
    model_profiles,
    prompt_detail,
    test_connection,
    update_connection,
)
from logrisk_django.views.jobs import (
    create_job,
    input_orchestration_action,
    input_orchestration_detail,
    job_detail,
    job_events,
    orchestration_action,
)
from logrisk_django.views.settings import (
    retention_maintenance,
    save_database_candidate,
    save_retention_policy,
    test_database_candidate,
)
from logrisk_django.views.semantics import (
    import_semantic,
    semantic_action,
    semantic_collection,
    semantic_detail,
    semantic_versions,
    test_semantic,
    validate_semantic,
)
from logrisk_django.views.uploads import (
    analyze_upload,
    append_upload_chunk,
    complete_upload,
    create_upload,
    input_job_progress,
    input_job_result,
)


app_name = "logrisk_django"
urlpatterns = [
    path("api/uploads", create_upload),
    path("api/uploads/<str:upload_id>/chunks/<int:index>", append_upload_chunk),
    path("api/uploads/<str:upload_id>/complete", complete_upload),
    path("api/inputs/analyze-upload", analyze_upload),
    path("api/input-jobs/<str:input_job_id>", input_job_progress),
    path("api/input-jobs/<str:input_job_id>/result", input_job_result),
    path("api/jobs", create_job),
    path("api/jobs/<str:job_id>", job_detail),
    path("api/jobs/<str:job_id>/events", job_events),
    path("api/orchestration/runs/<str:orchestration_run_id>/<str:action>", orchestration_action),
    path("api/input-orchestration/runs/<str:input_orchestration_run_id>", input_orchestration_detail),
    path("api/input-orchestration/runs/<str:input_orchestration_run_id>/<str:action>", input_orchestration_action),
    path("api/benchmark-center/suites", suites),
    path("api/benchmark-center/runs", runs),
    path("api/benchmark-center/runs/<str:run_id>/cancel", cancel_run),
    path("api/benchmark-center/comparisons", compare),
    path("api/benchmark-center/gates/evaluate", evaluate_gate),
    path("api/drain-quality/datasets", datasets),
    path("api/drain-quality/annotations", annotations),
    path("api/drain-quality/annotations/<str:annotation_id>/review", annotation_review),
    path("api/drain-quality/eval-runs", eval_runs),
    path("api/drain-quality/configs", configs),
    path("api/drain-quality/tune-runs", tune_runs),
    path("api/semantic/dictionaries", dictionary_collection),
    path("api/semantic/dictionaries/<str:dictionary_id>", dictionary_detail),
    path("api/semantic/dictionaries/<str:dictionary_id>/candidates", dictionary_candidate),
    path("api/semantic/dictionaries/<str:dictionary_id>/<str:action>", dictionary_action),
    path("api/semantic/test", dictionary_test),
    path("api/jobs/<str:job_id>/features/<str:candidate_id>", update_feature),
    path("api/jobs/<str:job_id>/export", export_approved),
    path("api/release-readiness/validate", validate_release),
    path("api/rule-governance/review-queue", rule_review_queue),
    path("api/rule-governance/rules/<str:rule_id>", rule_detail),
    path("api/rule-governance/rules/<str:rule_id>/<str:action>", rule_action),
    path("api/semantics", semantic_collection),
    path("api/semantics/validate", validate_semantic),
    path("api/semantics/test", test_semantic),
    path("api/semantics/import", import_semantic),
    path("api/semantics/effective", core_read, {"endpoint": "semantics/effective"}),
    path("api/semantics/export", core_read, {"endpoint": "semantics/export"}),
    path("api/semantics/unclassified", core_read, {"endpoint": "semantics/unclassified"}),
    path("api/semantics/<str:rule_id>/versions", semantic_versions),
    path("api/semantics/<str:rule_id>", semantic_detail),
    path("api/semantics/<str:rule_id>/<str:action>", semantic_action),
    path("api/runtime/retention/policy", save_retention_policy),
    path("api/runtime/retention/<str:mode>", retention_maintenance),
    path("api/system/database/config", save_database_candidate),
    path("api/system/database/test", test_database_candidate),
    path("api/ai-harness/connections", connections),
    path("api/ai-harness/connections/<str:connection_id>", update_connection),
    path("api/ai-harness/connections/<str:connection_id>/test", test_connection),
    path("api/ai-harness/model-profiles", model_profiles),
    path("api/ai-harness/prompts/<str:prompt_id>", prompt_detail),
    path("api/health", core_read, {"endpoint": "health"}),
    path("api/runtime/readiness", core_read, {"endpoint": "runtime/readiness"}),
    path("api/runtime/health", core_read, {"endpoint": "runtime/health"}),
    path("api/runtime/airflow", core_read, {"endpoint": "runtime/airflow"}),
    path("api/runtime/tasks", core_read, {"endpoint": "runtime/tasks"}),
    path("api/runtime/storage", core_read, {"endpoint": "runtime/storage"}),
    path("api/runtime/retention", core_read, {"endpoint": "runtime/retention"}),
    path("api/runtime/audits", core_read, {"endpoint": "runtime/audits"}),
    path("api/system/database", core_read, {"endpoint": "system/database"}),
    path("api/ai-harness/prompts", core_read, {"endpoint": "ai-harness/prompts"}),
    path("api/rule-governance/rules", core_read, {"endpoint": "rule-governance/rules"}),
    path("api/benchmark-center/overview", core_read, {"endpoint": "benchmark-center/overview"}),
    path("api/benchmark-center/trends", core_read, {"endpoint": "benchmark-center/trends"}),
    path("api/benchmark-center/leaderboard", core_read, {"endpoint": "benchmark-center/leaderboard"}),
    path("api/release-readiness", core_read, {"endpoint": "release-readiness"}),
    path("<path:path>", frontend),
]
