from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from logrisk.database import Database
from logrisk.server_risk.contracts import ServerRiskError, decode, digest, json_text, typed_fields

PAGE_ROWS = 250
MAX_SCOPE_ROWS = 10000
MAX_OVERVIEW_WINDOWS = 10_000
OVERVIEW_GROUP_SIZE = 128
CommitVerifications = dict[tuple[str, str, str | None], bool]


def identity_filter(column: str, cluster: str, node_id: str, provider: str) -> tuple[str, list[str]]:
    """Match canonical identity fields without relying on database JSON extensions."""
    tokens = []
    for key, value in (("cluster", cluster), ("node_id", node_id)):
        token = json_text(key) + ":" + json_text(value)
        tokens.append(token)
    function = "strpos" if provider == "postgres" else "instr"
    return function + "(" + column + ",?)>0 AND " + function + "(" + column + ",?)>0", tokens


@dataclass(frozen=True)
class _IndexedWindow:
    order: int
    row: dict[str, Any]
    window: dict[str, Any]
    physical: str | None
    node: tuple[str, str] | None


class _OverviewWindows:
    """Bounded index that belongs to one workbench overview request."""

    def __init__(self, scoped_physical_keys: set[str] | None = None) -> None:
        self.by_node: dict[tuple[str, str], list[_IndexedWindow]] = defaultdict(list)
        self.by_physical: dict[str, list[_IndexedWindow]] = defaultdict(list)
        self.by_commit: dict[tuple[str, str], list[_IndexedWindow]] = defaultdict(list)
        self.commits: dict[tuple[str, str], dict[str, Any]] = {}
        self.scoped_physical_keys = scoped_physical_keys

    def select(self, node: tuple[str, str], physical_keys: set[str] | None) -> list[_IndexedWindow]:
        scoped = self.by_node.get(node, [])
        if not physical_keys:
            return scoped
        # Use the database traversal order so the last matching physical row
        # has the same precedence as with the individual-host projection.
        selected = {item.order: item for item in scoped}
        for key in physical_keys:
            selected.update((item.order, item) for item in self.by_physical.get(key, ()))
        return [selected[order] for order in sorted(selected)]

    def verify(self, key: tuple[str, str], row: dict[str, Any]) -> bool:
        from logrisk.streaming_state import _payload_hash

        commit = self.by_commit[key]
        try:
            summary = decode(row["summary_json"])
            expected = summary.get("template_count")
            return (
                bool(row["payload_hash"])
                and (expected is None or expected == len(commit))
                and all(item.row["item_index"] == index for index, item in enumerate(commit))
                and _payload_hash(decode(row["cursor_json"]), [item.window for item in commit], summary) == row["payload_hash"]
            )
        except (ValueError, TypeError, KeyError, AttributeError):
            return False


class ServerRiskRepository:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.assessment = None

    def _recorded_node_keys(self) -> set[tuple[str, str]]:
        with self.database.connect() as conn:
            rows = conn.execute("SELECT DISTINCT cluster,node_id FROM node_risk_events ORDER BY cluster,node_id").fetchall()
            observations = conn.execute("SELECT sanitized_payload_json FROM server_risk_observations").fetchall()
        keys = {(row["cluster"], row["node_id"]) for row in rows}
        for row in observations:
            identity = decode(row["sanitized_payload_json"])["identity"]
            keys.add((identity["cluster"], identity["node_id"]))
        return keys

    @staticmethod
    def _node_rows(keys: set[tuple[str, str]]) -> list[dict[str, str]]:
        if len(keys) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "节点查询超过资源上限")
        return [{"cluster": c, "node_id": n, "label": n} for c, n in sorted(keys)]

    def nodes(self) -> list[dict[str, str]]:
        keys = self._recorded_node_keys()
        for row in self._window_pages():
            window = decode(row["window_json"])
            if window.get("entity_type") == "node" and window.get("cluster") and window.get("entity_id"):
                keys.add((window["cluster"], window["entity_id"]))
        return self._node_rows(keys)

    def latest_risk_times(self) -> dict[tuple[str, str], str]:
        result: dict[tuple[str, str], str] = {}
        with self.database.connect() as conn:
            events = conn.execute("SELECT cluster,node_id,MAX(last_seen) AS latest FROM node_risk_events WHERE derivation_status='current' GROUP BY cluster,node_id").fetchall()
            observations = conn.execute("SELECT sanitized_payload_json,interval_end FROM server_risk_observations WHERE kind='adverse'").fetchall()
        for row in events:
            result[(row["cluster"], row["node_id"])] = row["latest"]
        for row in observations:
            identity = decode(row["sanitized_payload_json"])["identity"]
            key = (identity["cluster"], identity["node_id"])
            result[key] = max(result.get(key, ""), row["interval_end"])
        for row in self._window_pages():
            window = decode(row["window_json"])
            if window.get("entity_type") == "node" and window.get("cluster") and window.get("entity_id") and window.get("last_seen") and isinstance(window.get("risk_semantic"), dict):
                key = (window["cluster"], window["entity_id"])
                result[key] = max(result.get(key, ""), window["last_seen"])
        return result

    def snapshot(self, cluster: str, node: str) -> Any:
        return ReadRiskSnapshot(self, cluster, node)

    def overview_snapshots(self) -> Iterator[tuple[dict[str, str], ReadRiskSnapshot]]:
        """Share one ordered window scan and commit verification across all hosts."""
        index = self._index_overview_windows()
        verifications: CommitVerifications = {}
        if index is None:
            nodes = self.nodes()
            for start in range(0, len(nodes), OVERVIEW_GROUP_SIZE):
                yield from self._overview_group(nodes[start:start + OVERVIEW_GROUP_SIZE], verifications)
            return
        keys = self._recorded_node_keys()
        keys.update((cluster, entity) for cluster, entity in index.by_node if cluster and entity)
        for node in self._node_rows(keys):
            yield node, ReadRiskSnapshot(
                self, node["cluster"], node["node_id"], window_index=index, commit_verifications=verifications,
            )

    def _overview_group(
        self, nodes: list[dict[str, str]], verifications: CommitVerifications,
    ) -> Iterator[tuple[dict[str, str], ReadRiskSnapshot]]:
        scopes = [(node["cluster"], node["node_id"]) for node in nodes]
        filter_by_node = " OR ".join("(e.cluster=? AND e.node_id=?)" for _ in scopes)
        params = [value for scope in scopes for value in scope]
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT i.physical_key FROM node_risk_ingestions i JOIN node_risk_events e ON e.event_id=i.event_id "
                "WHERE i.is_current=1 AND e.derivation_status='current' AND i.physical_key IS NOT NULL AND i.physical_key<>'' "
                f"AND ({filter_by_node}) LIMIT ?", (*params, MAX_OVERVIEW_WINDOWS + 1),
            ).fetchall()
        index = None
        if len(rows) <= MAX_OVERVIEW_WINDOWS:
            index = self._index_overview_windows(
                nodes=set(scopes), physical_keys={row["physical_key"] for row in rows},
            )
        if index is None and len(nodes) > 1:
            middle = len(nodes) // 2
            yield from self._overview_group(nodes[:middle], verifications)
            yield from self._overview_group(nodes[middle:], verifications)
            return
        for node in nodes:
            yield node, ReadRiskSnapshot(
                self, node["cluster"], node["node_id"], window_index=index, commit_verifications=verifications,
            )

    def _index_overview_windows(
        self, *, nodes: set[tuple[str, str]] | None = None, physical_keys: set[str] | None = None,
    ) -> _OverviewWindows | None:
        index = _OverviewWindows(physical_keys)
        retained = 0
        for order, row in enumerate(self._window_pages()):
            window = decode(row["window_json"])
            cluster, entity = window.get("cluster"), window.get("entity_id")
            node = (cluster, entity) if window.get("entity_type") == "node" and isinstance(cluster, str) and isinstance(entity, str) else None
            outside_group = nodes is not None and node not in nodes
            if outside_group and not physical_keys:
                continue
            physical = self._physical(row, window)
            if outside_group and physical not in (physical_keys or ()):
                continue
            if retained >= MAX_OVERVIEW_WINDOWS:
                return None
            retained += 1
            commit_key = (row["task_id"], row["window_id"])
            if commit_key not in index.commits:
                index.commits[commit_key] = {key: row[key] for key in ("cursor_json", "summary_json", "payload_hash")}
            item = _IndexedWindow(order, {
                "task_id": row["task_id"], "window_id": row["window_id"], "item_index": row["item_index"],
            }, window, physical, node)
            if nodes is None:
                index.by_commit[commit_key].append(item)
            if node is not None:
                index.by_node[node].append(item)
            if physical:
                index.by_physical[physical].append(item)
        return index

    def require_node(self, cluster: str, node_id: str) -> None:
        clause, parameters = identity_filter("sanitized_payload_json", cluster, node_id, self.database.provider)
        with self.database.connect() as conn:
            event = conn.execute("SELECT 1 FROM node_risk_events WHERE cluster=? AND node_id=? LIMIT 1", (cluster, node_id)).fetchone()
            observed = conn.execute("SELECT 1 FROM server_risk_observations WHERE " + clause + " LIMIT 1", parameters).fetchone()
        if event or observed:
            return
        if any(decode(row["window_json"]).get("cluster") == cluster and decode(row["window_json"]).get("entity_type") == "node" and decode(row["window_json"]).get("entity_id") == node_id for row in self._window_pages()):
            return
        raise ServerRiskError("node_not_found", "节点不存在于指定集群", 404)

    def observations(self, cluster: str, node_id: str, connection: Any = None) -> list[dict[str, Any]]:
        clause, parameters = identity_filter("o.sanitized_payload_json", cluster, node_id, self.database.provider)
        query = "SELECT o.* FROM server_risk_observations o LEFT JOIN server_risk_projection_state p ON p.input_namespace='fact:' || o.source_fact_key WHERE (p.last_fact_ref IS NULL OR p.last_fact_ref=o.observation_id) AND " + clause + " ORDER BY o.interval_start,o.source_fact_key LIMIT 10001"
        if connection is None:
            with self.database.connect() as conn:
                rows = conn.execute(query, parameters).fetchall()
        else:
            rows = connection.execute(query, parameters).fetchall()
        if len(rows) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "当前节点观测超过资源上限")
        return [dict(row, payload=decode(row["sanitized_payload_json"])) for row in rows]

    def _window_pages(self):
        after = ("", "", -1)
        with self.database.connect() as conn:
            while True:
                rows = conn.execute("SELECT w.task_id,w.window_id,w.item_index,w.window_json,c.cursor_json,c.summary_json,c.payload_hash FROM streaming_batch_windows w JOIN streaming_window_commits c ON c.task_id=w.task_id AND c.window_id=w.window_id WHERE (w.task_id>? OR (w.task_id=? AND w.window_id>?) OR (w.task_id=? AND w.window_id=? AND w.item_index>?)) ORDER BY w.task_id,w.window_id,w.item_index LIMIT ?", (after[0], after[0], after[1], after[0], after[1], after[2], PAGE_ROWS)).fetchall()
                if not rows:
                    break
                for row in rows:
                    yield row
                last = rows[-1]
                after = (last["task_id"], last["window_id"], last["item_index"])

    @staticmethod
    def _physical(row: Any, window: dict[str, Any]) -> str | None:
        item = window.get("source_item_id") or window.get("raw_log_id") or window.get("template_instance_hash")
        if window.get("source_namespace") and item is not None and window.get("window_start"):
            return digest([window["source_namespace"], window.get("source_batch_id") or row["window_id"], window["window_start"], item])
        return None

    def historical_projection(
        self, cluster: str, node_id: str, physical_keys: set[str] | None = None,
        *, window_index: _OverviewWindows | None = None,
        commit_verifications: CommitVerifications | None = None,
    ) -> dict[str, Any]:
        """Read committed facts in stable pages; never invoke old side effects."""
        from logrisk.streaming_state import StreamingIncompleteError, _stored_payload_hash
        if (
            window_index is not None and window_index.scoped_physical_keys is not None
            and physical_keys and not physical_keys <= window_index.scoped_physical_keys
        ):
            # A contribution appeared after the group keys were read.
            window_index = None
        windows, linked, commits = [], {}, {}
        def matching_windows():
            if window_index is not None:
                for item in window_index.select((cluster, node_id), physical_keys):
                    yield item.row, item.window, item.physical, item.node == (cluster, node_id)
            else:
                for row in self._window_pages():
                    window = decode(row["window_json"])
                    physical = self._physical(row, window)
                    scoped = window.get("entity_type") == "node" and window.get("entity_id") == node_id and window.get("cluster") == cluster
                    if scoped or physical_keys and physical in physical_keys:
                        yield row, window, physical, scoped

        for row, window, physical, scoped in matching_windows():
            key = (row["task_id"], row["window_id"])
            commits[key] = window_index.commits[key] if window_index is not None else row
            if physical:
                linked[physical] = window
            if scoped:
                windows.append((row, window, physical))
            if len(windows) > MAX_SCOPE_ROWS or len(linked) > MAX_SCOPE_ROWS:
                raise ServerRiskError("query_resource_limit", "当前节点历史事实超过资源上限")
        complete = True
        frontiers = []
        verified_commits = {} if commit_verifications is None else commit_verifications
        complete_index = window_index is not None and window_index.scoped_physical_keys is None
        needs_storage = not complete_index and any(
            (*key, row["payload_hash"]) not in verified_commits for key, row in commits.items()
        )
        with self.database.connect() if needs_storage else nullcontext() as conn:
            for key, row in commits.items():
                revision = row["payload_hash"]
                verification_key = (*key, revision)
                if verification_key not in verified_commits:
                    if complete_index:
                        verified_commits[verification_key] = window_index.verify(key, row)
                    else:
                        try:
                            summary = decode(row["summary_json"])
                            actual, count = _stored_payload_hash(conn, *key, decode(row["cursor_json"]), summary)
                            expected = summary.get("template_count")
                            verified_commits[verification_key] = bool(revision) and actual == revision and (expected is None or expected == count)
                        except (StreamingIncompleteError, ValueError, TypeError, KeyError):
                            verified_commits[verification_key] = False
                verified = verified_commits[verification_key]
                complete = complete and verified
                frontiers.append({"task_ref": key[0], "commit_ref": key[1], "input_revision": row["payload_hash"], "projection_complete": verified})
        return {"windows": windows, "linked": linked, "projection_complete": complete, "frontiers": frontiers}

    def facts(
        self, cluster: str, node_id: str, connection: Any = None,
        *, observations: list[dict[str, Any]] | None = None,
        window_index: _OverviewWindows | None = None,
        commit_verifications: CommitVerifications | None = None,
    ) -> list[dict[str, Any]]:
        with nullcontext(connection) if connection is not None else self.database.connect() as conn:
            events = conn.execute("SELECT * FROM node_risk_events WHERE cluster=? AND node_id=? AND derivation_status='current' ORDER BY first_seen,event_id LIMIT 10001", (cluster, node_id)).fetchall()
            contributions = conn.execute("SELECT i.* FROM node_risk_ingestions i JOIN node_risk_events e ON e.event_id=i.event_id WHERE e.cluster=? AND e.node_id=? AND i.is_current=1 AND e.derivation_status='current' ORDER BY i.source_event_fingerprint LIMIT 10001", (cluster, node_id)).fetchall()
        if any(len(rows) > MAX_SCOPE_ROWS for rows in (events, contributions)):
            raise ServerRiskError("query_resource_limit", "当前节点历史事实超过资源上限")
        physical_keys = {r["physical_key"] for r in contributions if r["physical_key"]}
        projection = self.historical_projection(
            cluster, node_id, physical_keys, window_index=window_index, commit_verifications=commit_verifications,
        )
        if observations is None:
            observations = self.observations(cluster, node_id, connection=connection)
        by_event: dict[str, list[Any]] = {}
        for row in contributions:
            by_event.setdefault(row["event_id"], []).append(row)
        registered = {}
        for observation in observations:
            if observation["source_fact_key"].startswith("committed:"):
                registered[observation["source_fact_key"].removeprefix("committed:")] = observation["payload"]
        def linked(physical: str | None, risk_type: str) -> bool:
            value = registered.get(physical)
            return bool(value and value["kind"] == "adverse" and value["risk_type"] == risk_type and value["identity"]["cluster"] == cluster and value["identity"]["node_id"] == node_id)
        facts = []
        for event in events:
            for row in by_event.get(event["event_id"]) or [None]:
                if row and linked(row["physical_key"], event["risk_type"]):
                    continue
                window = projection["linked"].get(row["physical_key"]) if row else None
                fields = typed_fields(decode(event["semantic_fields_json"]))
                object_id = fields.get("unit") or fields.get("gpu_uuid") or fields.get("device") or fields.get("mount") or "legacy:" + event["event_id"]
                observed = bool(window and window.get("first_seen") and window.get("last_seen"))
                facts.append({"fact_ref": "legacy:" + (row["source_event_fingerprint"] if row else event["event_id"]), "risk_ref": event["event_id"], "subject_key": None, "episode_ref": None, "cluster": cluster, "node_id": node_id, "risk_type": event["risk_type"], "severity": event["severity"], "object_kind": "legacy", "object_id": str(object_id), "first_seen": window["first_seen"] if observed else event["first_seen"], "last_seen": window["last_seen"] if observed else event["last_seen"], "count": int(row["occurrence_count"] if row else (event["current_occurrence_count"] or event["occurrence_count"])), "time_quality": "observed_interval" if observed else "legacy_unverified", "provenance": "committed_window" if observed else "legacy_unverified", "typed_fields": fields, "template_ref": None, "template_text": "", "processing_status": event["status"], "manual_closed_at": event["recovered_at"]})
        seen = set(physical_keys) | set(registered)
        for row, window, physical in projection["windows"]:
            semantic = window.get("risk_semantic")
            if not isinstance(semantic, dict) or not semantic.get("risk_type") or physical in seen:
                continue
            if physical:
                seen.add(physical)
            fields = typed_fields(semantic.get("semantic_fields"))
            obj = fields.get("unit") or fields.get("gpu_uuid") or fields.get("device") or fields.get("mount") or "legacy:" + digest([row["task_id"], row["window_id"], row["item_index"]])
            first, last = window.get("first_seen"), window.get("last_seen")
            if not first or not last:
                continue
            ref = "window:" + digest([row["task_id"], row["window_id"], row["item_index"], physical])
            facts.append({"fact_ref": ref, "risk_ref": ref, "subject_key": None, "episode_ref": None, "cluster": cluster, "node_id": node_id, "risk_type": semantic["risk_type"], "severity": semantic.get("severity", "high"), "object_kind": "legacy", "object_id": str(obj), "first_seen": first, "last_seen": last, "count": int(window.get("count") or 1), "time_quality": "observed_interval", "provenance": "committed_window", "typed_fields": fields, "template_ref": window.get("template_hash"), "template_text": "", "processing_status": "observed", "manual_closed_at": None})
        for row in observations:
            payload = row["payload"]
            if payload["kind"] != "adverse":
                continue
            identity = payload["identity"]
            facts.append({"fact_ref": row["observation_id"], "risk_ref": row["subject_key"], "subject_key": row["subject_key"], "episode_ref": None, "cluster": cluster, "node_id": node_id, "risk_type": payload["risk_type"], "severity": payload["typed_fields"].get("severity", "high"), "object_kind": identity["object_kind"], "object_id": identity["object_id"], "first_seen": row["interval_start"], "last_seen": row["interval_end"], "count": payload["count"], "time_quality": row["quality"], "provenance": "registered_observation", "typed_fields": typed_fields(payload["typed_fields"]), "template_ref": payload.get("template_ref"), "template_text": "", "processing_status": "observed", "manual_closed_at": None})
        return sorted(facts, key=lambda f: (f["first_seen"], f["fact_ref"]))

    def decisions(self, cluster: str, node_id: str) -> list[dict[str, Any]]:
        clause, parameters = identity_filter("o.sanitized_payload_json", cluster, node_id, self.database.provider)
        with self.database.connect() as conn:
            rows = conn.execute("SELECT d.* FROM server_risk_decisions d JOIN (SELECT DISTINCT o.subject_key FROM server_risk_observations o WHERE " + clause + ") scoped ON scoped.subject_key=d.subject_key ORDER BY d.effective_at,d.decision_id LIMIT 10001", parameters).fetchall()
        if len(rows) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "当前节点决定超过资源上限")
        return [{"decision_ref": r["decision_id"], "episode_ref": r["episode_id"], "subject_key": r["subject_key"], "state": r["state"], "effective_at": r["effective_at"], "valid_until": r["valid_until"], "superseded_by": r["superseded_by"], "evidence_refs": decode(r["evidence_refs_json"]), "evaluation_mode": "historical" if "evaluation_mode:historical" in decode(r["reason_codes_json"]) else "current", "as_of": r["effective_at"] if "evaluation_mode:historical" in decode(r["reason_codes_json"]) else r["decided_at"]} for r in rows]

    def scores(self, cluster: str, node_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as conn:
            rows = conn.execute("SELECT sample_id,assessed_at,overall_score FROM node_risk_score_samples WHERE cluster=? AND node_id=? ORDER BY assessed_at LIMIT 10001", (cluster, node_id)).fetchall()
        if len(rows) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "评分样本超过资源上限")
        return [dict(row) for row in rows]

    def report(self, report_id: str) -> dict[str, Any]:
        with self.database.connect() as conn:
            row = conn.execute("SELECT * FROM server_risk_reports WHERE report_id=?", (report_id,)).fetchone()
        if row is None:
            raise ServerRiskError("report_not_found", "报告不存在", 404)
        return dict(row)


class ReadRiskSnapshot:
    """One request owns a bounded immutable fact view; no stale cross-request cache."""

    def __init__(
        self, repository: ServerRiskRepository, cluster: str, node: str,
        *, window_index: _OverviewWindows | None = None,
        commit_verifications: CommitVerifications | None = None,
    ) -> None:
        from logrisk.server_risk.assessment import AssessmentService
        self.database = repository.database
        self._observations = repository.observations(cluster, node)
        if window_index is None and commit_verifications is None:
            self._facts = repository.facts(cluster, node)
            self._projection = repository.historical_projection(cluster, node)
        else:
            self._facts = repository.facts(
                cluster, node, observations=self._observations, window_index=window_index,
                commit_verifications=commit_verifications,
            )
            self._projection = repository.historical_projection(
                cluster, node, window_index=window_index, commit_verifications=commit_verifications,
            )
        self._scores = repository.scores(cluster, node)
        self._decisions = repository.decisions(cluster, node)
        self.assessment = AssessmentService(self, repository.assessment.policies, repository.assessment.clock) if repository.assessment is not None else None

    def observations(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return self._observations

    def facts(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return self._facts

    def historical_projection(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return self._projection

    def scores(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return self._scores

    def decisions(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return self._decisions
