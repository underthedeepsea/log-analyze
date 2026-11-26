from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from logrisk.database import Database, risk_window_physical
from logrisk.server_risk.contracts import ServerRiskError, decode, digest, json_text, typed_fields, utc

PAGE_ROWS = 250
MAX_SCOPE_ROWS = 10000
MAX_OVERVIEW_WINDOWS = 10_000
OVERVIEW_GROUP_SIZE = 128
CommitVerifications = dict[tuple[str, str, str | None], bool]


def _observation_identity(column: str, key: str, provider: str) -> str:
    if provider == "postgres":
        return f"({column}::jsonb->'identity'->>'{key}')"
    return f"json_extract({column}, '$.identity.{key}')"


def identity_filter(column: str, cluster: str, node_id: str, provider: str) -> tuple[str, list[str]]:
    fields = [_observation_identity(column, key, provider) for key in ("cluster", "node_id")]
    return " AND ".join(f"{field}=?" for field in fields), [cluster, node_id]


def _window_field(key: str, provider: str) -> str:
    if provider == "postgres":
        return f"(w.window_json->>'{key}')"
    return f"(CASE WHEN json_valid(w.window_json) THEN json_extract(w.window_json, '$.{key}') END)"


def _physical_token(provider: str) -> str:
    if provider == "postgres":
        return (
            "ARRAY[w.window_json->>'source_namespace', COALESCE(w.window_json->>'source_batch_id',w.window_id), "
            "w.window_json->>'window_start', COALESCE(w.window_json->>'source_item_id',w.window_json->>'raw_log_id',"
            "w.window_json->>'template_instance_hash')]"
        )
    return (
        "(CASE WHEN json_valid(w.window_json) THEN json_array("
        "json_extract(w.window_json,'$.source_namespace'), COALESCE(json_extract(w.window_json,'$.source_batch_id'),w.window_id), "
        "json_extract(w.window_json,'$.window_start'), COALESCE(json_extract(w.window_json,'$.source_item_id'),"
        "json_extract(w.window_json,'$.raw_log_id'),json_extract(w.window_json,'$.template_instance_hash'))) END)"
    )


DateScope = tuple[datetime, datetime]


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
        self.related_by_node: dict[tuple[str, str], set[str]] = {}

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

    def _recorded_node_keys(
        self, bounds: DateScope | None = None, *, physical_keys: set[str] | None = None,
    ) -> set[tuple[str, str]]:
        provider = self.database.provider
        obs_cluster = _observation_identity("sanitized_payload_json", "cluster", provider)
        obs_node = _observation_identity("sanitized_payload_json", "node_id", provider)
        legacy_filter = obs_filter = ""
        legacy_params: tuple[Any, ...] = ()
        obs_params: tuple[Any, ...] = ()
        if bounds is not None:
            left, right = bounds
            legacy_filter = (
                " WHERE derivation_status='current' AND last_seen>=? AND first_seen<?"
                if provider == "postgres" else
                " WHERE derivation_status='current' AND julianday(last_seen)>=julianday(?) AND julianday(first_seen)<=julianday(?)"
            )
            obs_filter = " WHERE kind='adverse' AND interval_end>=? AND interval_start<?"
            legacy_params = ((left - timedelta(milliseconds=1)).isoformat(), right.isoformat())
            obs_params = ((left - timedelta(seconds=1)).isoformat(), right.isoformat())
        with self.database.connect() as conn:
            keys = {
                (row["cluster"], row["node_id"]) for row in conn.execute(
                    "SELECT DISTINCT cluster,node_id FROM node_risk_events" + legacy_filter, legacy_params,
                )
            }
            keys.update(
                (row["cluster"], row["node_id"]) for row in conn.execute(
                    f"SELECT DISTINCT {obs_cluster} AS cluster,{obs_node} AS node_id FROM server_risk_observations" + obs_filter,
                    obs_params,
                ) if row["cluster"] and row["node_id"]
            )
        keys.update(self._linked_legacy_nodes(physical_keys or set()))
        return keys

    def _linked_legacy_nodes(self, physical_keys: set[str]) -> dict[tuple[str, str], set[str]]:
        result: dict[tuple[str, str], set[str]] = defaultdict(set)
        physical = sorted(physical_keys)
        if physical:
            with self.database.connect() as conn:
                for start in range(0, len(physical), PAGE_ROWS):
                    part = physical[start:start + PAGE_ROWS]
                    marks = ",".join("?" for _ in part)
                    for row in conn.execute(
                        "SELECT e.cluster,e.node_id,i.physical_key FROM node_risk_ingestions i "
                        "JOIN node_risk_events e ON e.event_id=i.event_id "
                        f"WHERE i.is_current=1 AND e.derivation_status='current' AND i.physical_key IN ({marks})", part,
                    ):
                        result[row["cluster"], row["node_id"]].add(row["physical_key"])
        return result

    def _legacy_scope(self, bounds: DateScope | None) -> tuple[str, tuple[str, ...]]:
        if bounds is None:
            return "", ()
        start, end = bounds
        if self.database.provider == "postgres":
            query = " AND e.last_seen>=? AND e.first_seen<?"
        else:
            query = " AND julianday(e.last_seen)>=julianday(?) AND julianday(e.first_seen)<=julianday(?)"
        return query, ((start - timedelta(milliseconds=1)).isoformat(), end.isoformat())

    @staticmethod
    def _node_rows(keys: set[tuple[str, str]]) -> list[dict[str, str]]:
        if len(keys) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "节点查询超过资源上限")
        return [{"cluster": c, "node_id": n, "label": n} for c, n in sorted(keys)]

    def nodes(self) -> list[dict[str, str]]:
        keys = self._recorded_node_keys()
        identity, cluster, entity = (
            _window_field(key, self.database.provider) for key in ("entity_type", "cluster", "entity_id")
        )
        with self.database.connect() as conn:
            keys.update((row["cluster"], row["node_id"]) for row in conn.execute(
                f"SELECT DISTINCT {cluster} AS cluster,{entity} AS node_id FROM streaming_batch_windows w WHERE {identity}='node'"
            ) if row["cluster"] and row["node_id"])
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

    def snapshot(self, cluster: str, node: str, *, bounds: DateScope | None = None) -> Any:
        if bounds is None:
            return ReadRiskSnapshot(self, cluster, node)
        node_row = {"cluster": cluster, "node_id": node, "label": node}
        return next(snapshot for _, snapshot in self._overview_group([node_row], {}, bounds))

    def overview_snapshots(self, bounds: DateScope, *, query: str = "") -> Iterator[tuple[dict[str, str], ReadRiskSnapshot]]:
        """Share one indexed date projection and commit verification across all hosts."""
        def matches(node: dict[str, str]) -> bool:
            return not query or query.casefold() in (node["cluster"] + " " + node["node_id"] + " " + node["label"]).casefold()

        index = self._index_overview_windows(bounds=bounds)
        verifications: CommitVerifications = {}
        if index is None:
            keys = self._recorded_node_keys(bounds)
            physical: set[str] = set()
            for row in self._window_pages(bounds=bounds):
                window = decode(row["window_json"])
                if window.get("entity_type") == "node" and window.get("cluster") and window.get("entity_id"):
                    keys.add((window["cluster"], window["entity_id"]))
                key = self._physical(row, window)
                if key:
                    physical.add(key)
                if len(physical) >= PAGE_ROWS:
                    keys.update(self._recorded_node_keys(bounds, physical_keys=physical))
                    physical.clear()
            keys.update(self._recorded_node_keys(bounds, physical_keys=physical))
            nodes = [node for node in self._node_rows(keys) if matches(node)]
            for start in range(0, len(nodes), OVERVIEW_GROUP_SIZE):
                yield from self._overview_group(nodes[start:start + OVERVIEW_GROUP_SIZE], verifications, bounds)
            return
        keys = self._recorded_node_keys(bounds) | set(index.related_by_node)
        keys.update((cluster, entity) for cluster, entity in index.by_node if cluster and entity)
        for node in self._node_rows(keys):
            if not matches(node):
                continue
            yield node, ReadRiskSnapshot(
                self, node["cluster"], node["node_id"], window_index=index, commit_verifications=verifications, bounds=bounds,
            )

    def _overview_group(
        self, nodes: list[dict[str, str]], verifications: CommitVerifications, bounds: DateScope,
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
                nodes=set(scopes), physical_keys={row["physical_key"] for row in rows}, bounds=bounds,
            )
        if index is None and len(nodes) > 1:
            middle = len(nodes) // 2
            yield from self._overview_group(nodes[:middle], verifications, bounds)
            yield from self._overview_group(nodes[middle:], verifications, bounds)
            return
        for node in nodes:
            yield node, ReadRiskSnapshot(
                self, node["cluster"], node["node_id"], window_index=index, commit_verifications=verifications, bounds=bounds,
            )

    def _index_overview_windows(
        self, *, nodes: set[tuple[str, str]] | None = None, physical_keys: set[str] | None = None,
        bounds: DateScope | None = None,
    ) -> _OverviewWindows | None:
        index = _OverviewWindows(physical_keys)
        retained = 0
        for order, row in enumerate(self._window_pages(bounds=bounds)):
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
        index.related_by_node = self._linked_legacy_nodes(set(index.by_physical))
        return index

    def require_node(self, cluster: str, node_id: str) -> None:
        clause, parameters = identity_filter("sanitized_payload_json", cluster, node_id, self.database.provider)
        with self.database.connect() as conn:
            event = conn.execute("SELECT 1 FROM node_risk_events WHERE cluster=? AND node_id=? LIMIT 1", (cluster, node_id)).fetchone()
            observed = conn.execute("SELECT 1 FROM server_risk_observations WHERE " + clause + " LIMIT 1", parameters).fetchone()
        if event or observed:
            return
        kind, cluster_field, node_field = (
            _window_field(key, self.database.provider) for key in ("entity_type", "cluster", "entity_id")
        )
        with self.database.connect() as conn:
            if conn.execute(
                f"SELECT 1 FROM streaming_batch_windows w WHERE {kind}='node' AND {cluster_field}=? AND {node_field}=? LIMIT 1",
                (cluster, node_id),
            ).fetchone():
                return
        raise ServerRiskError("node_not_found", "节点不存在于指定集群", 404)

    def _observation_scope(
        self, cluster: str, node_id: str, as_of: str | None, subject: str | None, bounds: DateScope | None = None,
    ) -> tuple[str, list[Any]]:
        clause, parameters = identity_filter("o.sanitized_payload_json", cluster, node_id, self.database.provider)
        if subject is not None:
            clause = "o.subject_key=? AND " + clause
            parameters.insert(0, subject)
        if as_of is not None:
            value = utc(as_of)
            # All admitted observation endpoints are canonical UTC. Compare
            # at microsecond precision and prune later times by indexed end.
            bound = value.replace(microsecond=999999).isoformat()
            clause += " AND o.interval_end<=? AND " + (
                "o.interval_end::timestamptz<=?::timestamptz" if self.database.provider == "postgres"
                else "CASE WHEN length(o.interval_end)=25 THEN substr(o.interval_end,1,19)||'.000000+00:00' "
                     "ELSE o.interval_end END<=?"
            )
            parameters.extend((bound, value.isoformat(timespec="microseconds")))
        if bounds is not None:
            start, end = bounds
            # These integer-second outer bounds only narrow the indexed SQL
            # read; the caller still applies the exact point/interval rules.
            clause += " AND o.interval_end>=? AND o.interval_start<?"
            parameters.extend(((start - timedelta(seconds=1)).isoformat(), end.isoformat()))

        return (
            " FROM server_risk_observations o "
            "LEFT JOIN server_risk_projection_state p ON p.input_namespace='fact:' || o.source_fact_key "
            "WHERE (p.last_fact_ref IS NULL OR p.last_fact_ref=o.observation_id) AND " + clause,
            parameters,
        )

    def observations(
        self, cluster: str, node_id: str, connection: Any = None, *,
        as_of: str | None = None, subject: str | None = None, bounds: DateScope | None = None,
    ) -> list[dict[str, Any]]:
        scope, parameters = self._observation_scope(cluster, node_id, as_of, subject, bounds)
        query = "SELECT o.*" + scope + " ORDER BY o.interval_start,o.source_fact_key LIMIT ?"
        with nullcontext(connection) if connection is not None else self.database.connect() as conn:
            rows = conn.execute(query, (*parameters, MAX_SCOPE_ROWS + 1)).fetchall()
        if len(rows) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "当前节点观测超过资源上限")
        return [dict(row, payload=decode(row["sanitized_payload_json"])) for row in rows]

    @contextmanager
    def current_snapshot(self) -> Iterator[Any]:
        with self.database.connect() as conn:
            if self.database.provider == "postgres":
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            else:
                conn.execute("BEGIN")
            yield conn

    def current_subjects(self, cluster: str, node_id: str, as_of: str, conn: Any) -> list[str]:
        scope, parameters = self._observation_scope(cluster, node_id, as_of, None)
        rows = conn.execute(
            "SELECT DISTINCT o.subject_key" + scope + " ORDER BY o.subject_key LIMIT ?",
            (*parameters, MAX_SCOPE_ROWS + 1),
        ).fetchall()
        if len(rows) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "当前节点对象数量超过资源上限")
        return [row["subject_key"] for row in rows]

    def _ordered_observations(self) -> tuple[str, str]:
        instant = ("CASE WHEN length(o.interval_start)=25 THEN substr(o.interval_start,1,19)"
                   "||'.000000+00:00' ELSE o.interval_start END")
        sequence = (
            "(o.sanitized_payload_json::jsonb->>'source_sequence')::bigint" if self.database.provider == "postgres"
            else "json_extract(o.sanitized_payload_json,'$.source_sequence')"
        )
        return f"({instant})", f"({sequence})"

    def iter_subject_observations(
        self, cluster: str, node_id: str, subject: str, as_of: str, conn: Any, *,
        after: tuple[Any, ...] = (),
    ) -> Iterator[dict[str, Any]]:
        scope, parameters = self._observation_scope(cluster, node_id, as_of, subject)
        instant, sequence = self._ordered_observations()
        fields = f"SELECT o.*,{instant} AS start_key,{sequence} AS sequence_key"
        order = f" ORDER BY {instant},{sequence},o.observation_id LIMIT ?"
        while True:
            if after:
                # Separate index seeks for the rest of this instant and then
                # later instants. This also bounds same-time replay pages.
                equal = conn.execute(
                    fields + scope + f" AND {instant}=? AND {sequence}>=? "
                    f"AND ({sequence},o.observation_id)>(?,?)" + order,
                    (*parameters, after[0], after[1], after[1], after[2], PAGE_ROWS),
                ).fetchall()
                additional = PAGE_ROWS - len(equal)
                later = conn.execute(
                    fields + scope + f" AND {instant}>?" + order, (*parameters, after[0], additional),
                ).fetchall() if additional else []
                page = [*equal, *later]
            else:
                page = conn.execute(fields + scope + order, (*parameters, PAGE_ROWS)).fetchall()
            for row in page:
                yield dict(row, payload=decode(row["sanitized_payload_json"]))
            if len(page) < PAGE_ROWS:
                return
            row = page[-1]
            after = row["start_key"], row["sequence_key"], row["observation_id"]

    def lock_current_scopes(self, scopes: set[tuple[str, str, str]], conn: Any) -> None:
        if self.database.provider == "postgres":
            for scope in sorted(scopes):
                key = int.from_bytes(bytes.fromhex(digest(scope)[:16]), "big", signed=True)
                conn.execute("SELECT pg_advisory_xact_lock(?)", (key,))

    def valid_current_checkpoint(
        self, cluster: str, node_id: str, subject: str, as_of: str, policy_digest: str, conn: Any,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT * FROM server_risk_current_checkpoints WHERE cluster=? AND node_id=? AND subject_key=? "
            "AND invalidated=0 AND policy_digest=?", (cluster, node_id, subject, policy_digest),
        ).fetchone()
        now = utc(as_of)
        if row is None or utc(row["as_of"]) > now:
            return None
        if utc(row["as_of"]) < now:
            instant, sequence = self._ordered_observations()
            scope, params = self._observation_scope(cluster, node_id, as_of, subject)
            # A previously admitted future interval can become as-of eligible
            # behind the stored key even if the incoming records are ordered.
            expiry = utc(row["as_of"])
            exact_end = "(CASE WHEN length(o.interval_end)=25 THEN substr(o.interval_end,1,19)||'.000000+00:00' ELSE o.interval_end END)"
            crossed = conn.execute(
                "SELECT 1" + scope + f" AND o.interval_end>=? AND {exact_end}>? "
                f"AND ({instant},{sequence},o.observation_id)<=(?,?,?) LIMIT 1",
                (*params, expiry.replace(microsecond=0).isoformat(), expiry.isoformat(timespec="microseconds"),
                 row["start_key"], row["sequence_key"], row["observation_id"]),
            ).fetchone()
            if crossed:
                return None
        state = decode(row["reducer_json"])
        if not isinstance(state, dict) or state.get("version") != 1:
            return None
        return state

    def active_observation_refs(self, subject: str, refs: list[str], conn: Any) -> bool:
        for start in range(0, len(refs), PAGE_ROWS):
            part = set(refs[start:start + PAGE_ROWS])
            marks = ",".join("?" for _ in part)
            found = conn.execute(
                "SELECT COUNT(*) FROM server_risk_observations o "
                "LEFT JOIN server_risk_projection_state p ON p.input_namespace='fact:'||o.source_fact_key "
                "WHERE o.subject_key=? AND (p.last_fact_ref IS NULL OR p.last_fact_ref=o.observation_id) "
                f"AND o.observation_id IN ({marks})", (subject, *sorted(part)),
            ).fetchone()[0]
            if found != len(part):
                return False
        return True

    def latest_registered_adverse(self, cluster: str, node_id: str) -> str | None:
        scope, parameters = self._observation_scope(cluster, node_id, None, None)
        with self.database.connect() as conn:
            return conn.execute("SELECT MAX(o.interval_end)" + scope + " AND o.kind='adverse'", parameters).fetchone()[0]

    def _window_pages(self, *, bounds: DateScope | None = None):
        if bounds is not None:
            yield from self._dated_windows(bounds)
            return
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

    def _dated_windows(self, bounds: DateScope):
        provider = self.database.provider
        first, last, namespace = (_window_field(key, provider) for key in ("first_seen", "last_seen", "source_namespace"))
        time = "logrisk_iso_time" if provider == "postgres" else "julianday"
        start, end = bounds
        # Last/first indexes use the observed interval. Include all revisions
        # of a physical contribution so its latest link keeps precedence.
        token = _physical_token(provider)
        columns = "w.task_id,w.window_id,w.item_index"
        with self.database.connect() as conn:
            yield from conn.execute(
                f"WITH dated AS (SELECT {columns}, CASE WHEN {namespace}<>'' THEN {token} END AS link "
                f"FROM streaming_batch_windows w WHERE {time}({last})>={time}(?) AND {time}({first})<={time}(?)), "
                f"matched AS (SELECT task_id,window_id,item_index FROM dated UNION SELECT {columns} "
                f"FROM streaming_batch_windows w WHERE {token} IN (SELECT DISTINCT link FROM dated WHERE link IS NOT NULL)) "
                f"SELECT {columns},w.window_json,c.cursor_json,c.summary_json,c.payload_hash FROM matched m "
                "JOIN streaming_batch_windows w ON w.task_id=m.task_id AND w.window_id=m.window_id AND w.item_index=m.item_index "
                "JOIN streaming_window_commits c ON c.task_id=m.task_id AND c.window_id=m.window_id "
                "ORDER BY w.task_id,w.window_id,w.item_index",
                ((start - timedelta(milliseconds=1)).isoformat(), end.isoformat()),
            )

    def _physical_window_pages(self, keys: set[str]):
        wanted = sorted(keys)
        with self.database.connect() as conn:
            for start in range(0, len(wanted), PAGE_ROWS):
                part = wanted[start:start + PAGE_ROWS]
                marks = ",".join("?" for _ in part)
                yield from conn.execute(
                    "SELECT w.task_id,w.window_id,w.item_index,w.window_json,c.cursor_json,c.summary_json,c.payload_hash "
                    "FROM streaming_batch_windows w JOIN streaming_window_commits c ON c.task_id=w.task_id AND c.window_id=w.window_id "
                    f"WHERE logrisk_window_physical(w.window_json,w.window_id) IN ({marks}) "
                    "ORDER BY w.task_id,w.window_id,w.item_index", part,
                )

    @staticmethod
    def _physical(row: Any, window: dict[str, Any]) -> str | None:
        return risk_window_physical(window, row["window_id"])

    def historical_projection(
        self, cluster: str, node_id: str, physical_keys: set[str] | None = None,
        *, window_index: _OverviewWindows | None = None,
        commit_verifications: CommitVerifications | None = None, bounds: DateScope | None = None,
        physical_only: bool = False,
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
            if physical_only:
                for row in self._physical_window_pages(physical_keys or set()):
                    window = decode(row["window_json"])
                    physical = self._physical(row, window)
                    if physical in (physical_keys or ()):
                        yield row, window, physical, False
            elif window_index is not None:
                for item in window_index.select((cluster, node_id), physical_keys):
                    yield item.row, item.window, item.physical, item.node == (cluster, node_id)
            else:
                for row in self._window_pages(bounds=bounds):
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
        def complete_locally(key: tuple[str, str], row: dict[str, Any]) -> bool:
            if window_index is None or window_index.scoped_physical_keys is not None:
                return False
            commit = window_index.by_commit[key]
            return decode(row["summary_json"]).get("template_count") == len(commit)

        needs_storage = any(
            (*key, row["payload_hash"]) not in verified_commits and not complete_locally(key, row)
            for key, row in commits.items()
        )
        with self.database.connect() if needs_storage else nullcontext() as conn:
            for key, row in commits.items():
                revision = row["payload_hash"]
                verification_key = (*key, revision)
                if verification_key not in verified_commits:
                    if complete_locally(key, row):
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
        commit_verifications: CommitVerifications | None = None, bounds: DateScope | None = None,
        include_registered: bool = True, recorded_as_of: str | None = None,
    ) -> list[dict[str, Any]]:
        date_clause, values = self._legacy_scope(bounds)
        with nullcontext(connection) if connection is not None else self.database.connect() as conn:
            events = conn.execute(
                "SELECT e.* FROM node_risk_events e WHERE e.cluster=? AND e.node_id=? AND e.derivation_status='current'"
                + date_clause + " ORDER BY e.first_seen,e.event_id LIMIT ?", (cluster, node_id, *values, MAX_SCOPE_ROWS + 1),
            ).fetchall()
            contributions = conn.execute(
                "SELECT i.* FROM node_risk_ingestions i JOIN node_risk_events e ON e.event_id=i.event_id "
                "WHERE e.cluster=? AND e.node_id=? AND i.is_current=1 AND e.derivation_status='current'"
                + date_clause + " ORDER BY i.source_event_fingerprint LIMIT ?", (cluster, node_id, *values, MAX_SCOPE_ROWS + 1),
            ).fetchall()
            if bounds is not None:
                if window_index is not None:
                    linked = window_index.related_by_node.get((cluster, node_id), set())
                else:
                    # At the one-node fallback the scoped window set could not
                    # fit the in-memory share. Retain its links in bounded chunks.
                    linked = set()
                    pending: set[str] = set()
                    for row in self._window_pages(bounds=bounds):
                        physical = self._physical(row, decode(row["window_json"]))
                        if physical:
                            pending.add(physical)
                        if len(pending) >= PAGE_ROWS:
                            linked.update(self._linked_legacy_nodes(pending).get((cluster, node_id), ()))
                            pending.clear()
                    linked.update(self._linked_legacy_nodes(pending).get((cluster, node_id), ()))
                extra = sorted(linked - {row["physical_key"] for row in contributions})
                known_events = {row["event_id"] for row in events}
                extra_events: set[str] = set()
                for start in range(0, len(extra), PAGE_ROWS):
                    part = extra[start:start + PAGE_ROWS]
                    marks = ",".join("?" for _ in part)
                    for row in conn.execute(
                        "SELECT i.* FROM node_risk_ingestions i "
                        f"WHERE i.is_current=1 AND i.physical_key IN ({marks})", part,
                    ):
                        contributions.append(row)
                        if row["event_id"] not in known_events:
                            extra_events.add(row["event_id"])
                missing = sorted(extra_events)
                for start in range(0, len(missing), PAGE_ROWS):
                    part = missing[start:start + PAGE_ROWS]
                    marks = ",".join("?" for _ in part)
                    events.extend(conn.execute(
                        f"SELECT e.* FROM node_risk_events e WHERE e.event_id IN ({marks}) "
                        "AND e.cluster=? AND e.node_id=? AND e.derivation_status='current'",
                        (*part, cluster, node_id),
                    ))
        if any(len(rows) > MAX_SCOPE_ROWS for rows in (events, contributions)):
            raise ServerRiskError("query_resource_limit", "当前节点历史事实超过资源上限")
        physical_keys = {r["physical_key"] for r in contributions if r["physical_key"]}
        selected = physical_keys if bounds is None or window_index is None else physical_keys & set(window_index.by_physical)
        projection = self.historical_projection(
            cluster, node_id, selected, window_index=window_index, commit_verifications=commit_verifications, bounds=bounds,
        )
        missing = physical_keys - set(projection["linked"])
        if bounds is not None and missing:
            linked = self.historical_projection(
                cluster, node_id, missing, commit_verifications=commit_verifications, physical_only=True,
            )
            projection["linked"].update(linked["linked"])
        if not include_registered:
            candidates = sorted(physical_keys | {physical for _, _, physical in projection["windows"] if physical})
            observations = []
            scope, params = self._observation_scope(cluster, node_id, recorded_as_of, None)
            with nullcontext(connection) if connection is not None else self.database.connect() as conn:
                for start in range(0, len(candidates), PAGE_ROWS):
                    part = ["committed:" + key for key in candidates[start:start + PAGE_ROWS]]
                    marks = ",".join("?" for _ in part)
                    observations.extend(
                        dict(row, payload=decode(row["sanitized_payload_json"])) for row in conn.execute(
                            "SELECT o.source_fact_key,o.sanitized_payload_json" + scope
                            + f" AND o.source_fact_key IN ({marks})", (*params, *part),
                        )
                    )
        elif observations is None:
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
        for row in observations if include_registered else ():
            payload = row["payload"]
            if payload["kind"] != "adverse":
                continue
            identity = payload["identity"]
            facts.append({"fact_ref": row["observation_id"], "risk_ref": row["subject_key"], "subject_key": row["subject_key"], "episode_ref": None, "cluster": cluster, "node_id": node_id, "risk_type": payload["risk_type"], "severity": payload["typed_fields"].get("severity", "high"), "object_kind": identity["object_kind"], "object_id": identity["object_id"], "first_seen": row["interval_start"], "last_seen": row["interval_end"], "count": payload["count"], "time_quality": row["quality"], "provenance": "registered_observation", "typed_fields": typed_fields(payload["typed_fields"]), "template_ref": payload.get("template_ref"), "template_text": "", "processing_status": "observed", "manual_closed_at": None})
        return sorted(facts, key=lambda f: (f["first_seen"], f["fact_ref"]))

    def decisions(self, cluster: str, node_id: str) -> list[dict[str, Any]]:
        with self.database.connect() as conn:
            rows = conn.execute(
                "SELECT d.*,n.decision_id AS scoped_superseded_by FROM server_risk_decisions d "
                "LEFT JOIN server_risk_decisions n ON n.decision_id=d.superseded_by AND n.cluster=d.cluster "
                "AND n.node_id=d.node_id AND n.subject_key=d.subject_key "
                "WHERE d.cluster=? AND d.node_id=? ORDER BY d.effective_at,d.decision_id LIMIT ?",
                (cluster, node_id, MAX_SCOPE_ROWS + 1),
            ).fetchall()
        if len(rows) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "当前节点决定超过资源上限")
        return [{"decision_ref": r["decision_id"], "episode_ref": r["episode_id"], "subject_key": r["subject_key"], "state": r["state"], "effective_at": r["effective_at"], "valid_until": r["valid_until"], "superseded_by": r["scoped_superseded_by"], "evidence_refs": decode(r["evidence_refs_json"]), "evaluation_mode": "historical" if "evaluation_mode:historical" in decode(r["reason_codes_json"]) else "current", "as_of": r["effective_at"] if "evaluation_mode:historical" in decode(r["reason_codes_json"]) else r["decided_at"]} for r in rows]

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
        commit_verifications: CommitVerifications | None = None, bounds: DateScope | None = None,
    ) -> None:
        from logrisk.server_risk.assessment import AssessmentService
        self.database = repository.database
        self.repository = repository
        self.bounds = bounds
        self._observations = repository.observations(cluster, node, bounds=bounds)
        if window_index is None and commit_verifications is None and bounds is None:
            self._facts = repository.facts(cluster, node)
            self._projection = repository.historical_projection(cluster, node)
        else:
            self._facts = repository.facts(
                cluster, node, observations=self._observations, window_index=window_index,
                commit_verifications=commit_verifications, bounds=bounds,
            )
            self._projection = repository.historical_projection(
                cluster, node, window_index=window_index, commit_verifications=commit_verifications, bounds=bounds,
            )
        self._scores = repository.scores(cluster, node) if bounds is None else []
        self._decisions = repository.decisions(cluster, node) if bounds is None else []
        self.assessment = AssessmentService(self, repository.assessment.policies, repository.assessment.clock) if repository.assessment is not None else None

    def overview_checks(self, cluster: str, node: str) -> list[dict[str, Any]]:
        if self.assessment is None:
            return []
        if self.bounds is None:
            return self.assessment.evaluate(cluster, node)["checks"]
        now = utc(self.assessment.clock())
        subjects = {row["subject_key"] for row in self._observations if row["kind"] == "adverse"}
        with self.repository.current_snapshot() as conn:
            checks = [self.repository.assessment.current_subject(cluster, node, subject, now, conn)[0] for subject in sorted(subjects)]
        return [check for check in checks if check]

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
