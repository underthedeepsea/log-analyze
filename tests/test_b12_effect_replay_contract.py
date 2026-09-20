from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.multi_source.repository import MultiSourceRepository
from logrisk.multi_source.service import MultiSourceService


def entity(component, template_hash):
    return {
        "cluster": "prod", "entity_type": "node", "entity_id": "node-a",
        "window_start": "2026-01-01T00:00:00+00:00",
        "window_end": "2026-01-01T00:00:10+00:00",
        "risk_score": 90, "risk_level": "critical",
        "top_templates": [{
            "cluster": "prod", "node": "node-a", "component": component,
            "source_type": component, "template_hash": template_hash,
            "window_start": "2026-01-01T00:00:00+00:00",
            "window_end": "2026-01-01T00:00:10+00:00", "count": 1,
        }],
    }


def test_nonempty_multisource_replay_keeps_stable_observations_edges_and_payload(tmp_path):
    repository = MultiSourceRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    service = MultiSourceService(repository, aliases={}, rules=[{
        "rule_id": "b12-replay", "version": 1, "enabled": True,
        "source_pairs": [["kernel", "kubelet"]], "max_gap_seconds": 60,
        "min_risk_score": 1, "min_count": 1, "confidence": 1.0,
    }])
    payload = [entity("kernel", "kernel-h"), entity("kubelet", "kubelet-h")]
    first = service.ingest_risk_entities(payload, source_job_id="job")
    assert first["observations"] == 2
    assert first["correlations"] == 1
    with repository.database.connect() as connection:
        before_observations = [dict(row) for row in connection.execute(
            "SELECT observation_id,source_family,template_hash,occurrence_count,entity_keys_json,relations_json "
            "FROM multi_source_observations ORDER BY observation_id"
        ).fetchall()]
        before_edges = [dict(row) for row in connection.execute(
            "SELECT correlation_id,rule_id,cluster,primary_entity_key,source_families_json,edges_json "
            "FROM multi_source_correlations ORDER BY correlation_id"
        ).fetchall()]
    replay = service.ingest_risk_entities(payload, source_job_id="job")
    assert replay["observations"] == 2
    assert replay["correlations"] == 1
    with repository.database.connect() as connection:
        after_observations = [dict(row) for row in connection.execute(
            "SELECT observation_id,source_family,template_hash,occurrence_count,entity_keys_json,relations_json "
            "FROM multi_source_observations ORDER BY observation_id"
        ).fetchall()]
        after_edges = [dict(row) for row in connection.execute(
            "SELECT correlation_id,rule_id,cluster,primary_entity_key,source_families_json,edges_json "
            "FROM multi_source_correlations ORDER BY correlation_id"
        ).fetchall()]
    assert after_observations == before_observations
    assert after_edges == before_edges
