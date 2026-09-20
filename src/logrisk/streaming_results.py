from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from logrisk.risk_engine import score_window,match_template_rule,level_of

PAGE_ROWS = 100
PAGE_BYTES = 2 * 1024 * 1024


def _json(value: Any) -> str:
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


class ResultBudgetError(ValueError):
    pass


class StreamingResultRepository:
    def __init__(self,database: Any):
        self.database = database

    def build(self,task_id: str,rules: dict[str,Any]) -> dict[str,Any]:
        from logrisk.large_file_pipeline import _merge_template_windows
        from logrisk.streaming_state import StreamingStateRepository
        StreamingStateRepository(self.database).require_complete_prefix(task_id)
        rules_hash = _digest({"rules": rules, "reducer_version": 1})
        with self.database.transaction() as connection:
            task = connection.execute("SELECT cursor_json FROM streaming_tasks WHERE task_id=?",(task_id,)).fetchone()
            if task is None:
                raise KeyError(f"Streaming task not found: {task_id}")
            raw_frontier = task[0]
            frontier = _json(json.loads(raw_frontier) if isinstance(raw_frontier, str) else raw_frontier)
            old = connection.execute("SELECT * FROM streaming_result_generations WHERE task_id=? AND rules_hash=? AND frontier_json=? ORDER BY generation LIMIT 1",(task_id,rules_hash,frontier)).fetchone()
            if old:
                generation = old["generation"]
                if old["status"] == "ready":
                    return {"task_id":task_id,"generation":generation}
                after = (old["after_window"],int(old["after_item"]))
            else:
                generation = uuid.uuid4().hex
                after = ("",-1)
                connection.execute("INSERT INTO streaming_result_generations(task_id,generation,status,rules_hash,frontier_json) VALUES (?,?,'building',?,?)",(task_id,generation,rules_hash,frontier))
        while True:
            with self.database.transaction() as connection:
                rows = connection.execute("SELECT window_id,item_index,window_json FROM streaming_batch_windows WHERE task_id=? AND (window_id>? OR (window_id=? AND item_index>?)) ORDER BY window_id,item_index LIMIT ?",(task_id,after[0],after[0],after[1],PAGE_ROWS)).fetchall()
                if not rows:
                    break
                consumed = 0
                for row in rows:
                    raw_payload = row["window_json"]
                    payload = raw_payload if isinstance(raw_payload, str) else _json(raw_payload)
                    size = len(payload.encode())
                    if size > PAGE_BYTES:
                        raise ResultBudgetError("单个窗口超过结果归约字节预算；完整事实已保留")
                    if consumed and consumed+size > PAGE_BYTES:
                        break
                    consumed += size
                    window = json.loads(payload)
                    canonical = [window.get(key) for key in ("window_start","window_end","cluster","entity_type","entity_id","component","template_hash","source_type","semantic_extractor_version")]+[window.get("semantic_dictionary_versions") or {},(window.get("risk_semantic") or {}).get("risk_type")]
                    window_key = _digest(canonical)
                    entity_key = _json(canonical[:5])
                    core,members = self._split(window)
                    existing = connection.execute("SELECT canonical_key,core_json FROM streaming_result_windows WHERE task_id=? AND generation=? AND window_key=?",(task_id,generation,window_key)).fetchone()
                    if existing:
                        if existing[0] != _json(canonical):
                            raise ValueError("归约 key 哈希冲突")
                        core = _merge_template_windows([json.loads(existing[1]),core])[0]
                    score = score_window(core,rules)
                    rule = match_template_rule(core,rules)
                    if rule:
                        core.update(category=rule.get("category"),feature_hint=rule.get("feature_hint"),rule_name=rule.get("name"))
                    core["window_risk_score"] = score
                    connection.execute("INSERT INTO streaming_result_windows(task_id,generation,window_key,canonical_key,entity_key,count,score,core_json) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(task_id,generation,window_key) DO UPDATE SET count=excluded.count,score=excluded.score,core_json=excluded.core_json",(task_id,generation,window_key,_json(canonical),entity_key,int(core["count"]),score,_json(core)))
                    for kind,value,count in members:
                        member_key = _digest(value)
                        connection.execute("INSERT INTO streaming_result_window_members(task_id,generation,window_key,kind,member_key,value_json,count) VALUES (?,?,?,?,?,?,?) ON CONFLICT(task_id,generation,window_key,kind,member_key) DO UPDATE SET count=streaming_result_window_members.count+excluded.count",(task_id,generation,window_key,kind,member_key,_json(value),count))
                    after = (row["window_id"],int(row["item_index"]))
                connection.execute("UPDATE streaming_result_generations SET after_window=?,after_item=? WHERE task_id=? AND generation=?",(*after,task_id,generation))
        after_entity = ""
        while True:
            with self.database.transaction() as connection:
                keys = connection.execute("SELECT entity_key,COUNT(*) AS windows,MAX(score) AS peak FROM streaming_result_windows WHERE task_id=? AND generation=? AND entity_key>? GROUP BY entity_key ORDER BY entity_key LIMIT ?",(task_id,generation,after_entity,PAGE_ROWS)).fetchall()
                if not keys:
                    break
                for key in keys:
                    ws,we,cluster,etype,eid = json.loads(key["entity_key"])
                    templates = connection.execute("SELECT window_key,core_json FROM streaming_result_windows WHERE task_id=? AND generation=? AND entity_key=? ORDER BY score DESC,canonical_key LIMIT 10",(task_id,generation,key["entity_key"])).fetchall()
                    score = min(100,round(float(key["peak"])+min(10,max(0,int(key["windows"])-1)*3),2))
                    top = [dict(json.loads(row["core_json"]),_window_key=row["window_key"]) for row in templates]
                    hints = [item.get("feature_hint") for item in top if item.get("feature_hint")]
                    entity = {"window_start":ws,"window_end":we,"cluster":cluster,"entity_type":etype,"entity_id":eid,"risk_score":score,"risk_level":level_of(score),"top_templates":top,"affected_entities":[],"summary":hints[0] if hints else "发现异常模板，需要结合时间线、指标和原始日志进一步确认。"}
                    connection.execute("INSERT INTO streaming_result_entities(task_id,generation,entity_key,score,level,entity_id,entity_json) VALUES (?,?,?,?,?,?,?) ON CONFLICT(task_id,generation,entity_key) DO UPDATE SET score=excluded.score,level=excluded.level,entity_json=excluded.entity_json",(task_id,generation,key["entity_key"],score,level_of(score),eid,_json(entity)))
                after_entity = keys[-1]["entity_key"]
        with self.database.transaction() as connection:
            raw_current = connection.execute("SELECT cursor_json FROM streaming_tasks WHERE task_id=?",(task_id,)).fetchone()[0]
            current = _json(json.loads(raw_current) if isinstance(raw_current, str) else raw_current)
            if current != frontier:
                raise ValueError("结果构建期间来源水位变化")
            counts = connection.execute("SELECT COUNT(*) AS windows,COALESCE(SUM(count),0) AS records FROM streaming_result_windows WHERE task_id=? AND generation=?",(task_id,generation)).fetchone()
            summary = dict(counts)
            summary.update(dict(connection.execute("SELECT COUNT(*) AS entities,COALESCE(SUM(CASE WHEN level='critical' THEN 1 ELSE 0 END),0) AS critical,COALESCE(SUM(CASE WHEN level='high' THEN 1 ELSE 0 END),0) AS high FROM streaming_result_entities WHERE task_id=? AND generation=?",(task_id,generation)).fetchone()))
            connection.execute("UPDATE streaming_result_generations SET status='ready',summary_json=? WHERE task_id=? AND generation=?",(_json(summary),task_id,generation))
        return {"task_id":task_id,"generation":generation}

    @staticmethod
    def _split(window: dict[str,Any]) -> tuple[dict[str,Any],list[tuple[str,Any,int]]]:
        core = dict(window)
        members = []
        for field in ("affected_namespaces","affected_pods","entity_keys","entity_relations","semantic_tags"):
            for value in core.pop(field,[]) or []:
                members.append((field,value,1))
        for field,values in (core.pop("semantic_fields",{}) or {}).items():
            for value in values:
                members.append(("semantic_fields",{"field":field,"value":value["value"]},int(value["count"])))
        for value in core.pop("typed_parameters",[]) or []:
            members.append(("typed_parameters",{key:item for key,item in value.items() if key != "count"},int(value.get("count") or 1)))
        core.pop("samples",None)
        core.pop("_commit_item_index",None)
        return core,members

    def _validate(self,connection: Any,reference: dict[str,Any]) -> dict[str,Any]:
        row = connection.execute("SELECT status,summary_json FROM streaming_result_generations WHERE task_id=? AND generation=?",(reference["task_id"],reference["generation"])).fetchone()
        if row is None or row[0] != "ready":
            raise ValueError("结果引用尚未完成或不存在")
        return json.loads(row[1])

    def summary(self,reference: dict[str,Any]) -> dict[str,Any]:
        with self.database.connect() as connection:
            return self._validate(connection,reference)

    def top_windows(self,reference: dict[str,Any]) -> list[dict[str,Any]]:
        with self.database.connect() as connection:
            self._validate(connection,reference)
            rows=connection.execute("SELECT window_key,core_json FROM streaming_result_windows WHERE task_id=? AND generation=? ORDER BY count DESC,canonical_key LIMIT 20",(reference["task_id"],reference["generation"])).fetchall()
            output=[]
            for row in rows:
                value=dict(json.loads(row["core_json"]),_window_key=row["window_key"])
                self._hydrate(connection,reference,value)
                output.append(value)
            if len(_json(output).encode())>PAGE_BYTES:
                raise ResultBudgetError("模板预览超过字节预算")
            return output

    def feature_sources(self,reference: dict[str,Any]):
        from logrisk.feature_jobs import _collapse_risk_entities
        after_id=""
        while True:
            with self.database.connect() as connection:
                self._validate(connection,reference)
                ids=connection.execute("SELECT DISTINCT entity_id FROM streaming_result_entities WHERE task_id=? AND generation=? AND entity_id>? ORDER BY entity_id LIMIT ?",(reference["task_id"],reference["generation"],after_id,PAGE_ROWS)).fetchall()
            if not ids:
                return
            for row in ids:
                entity_id=row[0]; after_key=""; current=None
                while True:
                    with self.database.connect() as connection:
                        rows=connection.execute("SELECT entity_key,entity_json FROM streaming_result_entities WHERE task_id=? AND generation=? AND entity_id=? AND entity_key>? ORDER BY entity_key LIMIT ?",(reference["task_id"],reference["generation"],entity_id,after_key,PAGE_ROWS)).fetchall()
                        for item in rows:
                            entity=json.loads(item["entity_json"])
                            for template in entity["top_templates"]:
                                self._hydrate(connection,reference,template)
                            entity["affected_entities"]=self._entity_pods(connection, reference, item["entity_key"])
                            current=_collapse_risk_entities([current,entity] if current else [entity])[0]
                            if len(_json(current).encode())>PAGE_BYTES:
                                raise ResultBudgetError("单实体审批证据超过预算；需要拆分明确分析范围")
                    if not rows:
                        break
                    after_key=rows[-1]["entity_key"]
                with self.database.transaction() as connection:
                    connection.execute("INSERT INTO streaming_feature_entities(task_id,generation,entity_id,source_json) VALUES (?,?,?,?) ON CONFLICT(task_id,generation,entity_id) DO UPDATE SET source_json=excluded.source_json",(reference["task_id"],reference["generation"],entity_id,_json(current)))
                yield dict(current,_source_ref=dict(reference,entity_id=entity_id))
            after_id=ids[-1][0]

    def feature_source(self,reference: dict[str,Any]) -> dict[str,Any]:
        with self.database.connect() as connection:
            self._validate(connection,reference)
            row=connection.execute("SELECT source_json FROM streaming_feature_entities WHERE task_id=? AND generation=? AND entity_id=?",(reference["task_id"],reference["generation"],reference["entity_id"])).fetchone()
        if row is None:
            raise ValueError("审批来源引用不存在")
        return json.loads(row[0])

    def entities(self,reference: dict[str,Any],*,after: str="",limit: int=PAGE_ROWS) -> dict[str,Any]:
        limit=max(1,min(int(limit),PAGE_ROWS))
        with self.database.connect() as connection:
            self._validate(connection,reference)
            rows = connection.execute("SELECT entity_key,entity_json FROM streaming_result_entities WHERE task_id=? AND generation=? AND entity_key>? ORDER BY entity_key LIMIT ?",(reference["task_id"],reference["generation"],after,max(1,min(limit,PAGE_ROWS))+1)).fetchall()
            items=[]
            consumed=0
            last=None
            for row in rows[:limit]:
                entity=json.loads(row["entity_json"])
                for template in entity["top_templates"]:
                    self._hydrate(connection,reference,template)
                entity["affected_entities"]=self._entity_pods(connection, reference, row["entity_key"])
                size=len(_json(entity).encode())
                if size>PAGE_BYTES:
                    raise ResultBudgetError("单实体超过结果响应预算")
                if consumed and consumed+size>PAGE_BYTES:
                    break
                items.append(entity); consumed+=size; last=row["entity_key"]
            return {"items":items,"next_key":last if len(items)<len(rows) else None}

    def facts(self,reference: dict[str,Any],*,collection: str="windows",after: str="",window_key: str | None=None,limit: int=PAGE_ROWS) -> dict[str,Any]:
        limit=max(1,min(int(limit),PAGE_ROWS))
        with self.database.connect() as connection:
            self._validate(connection,reference)
            if collection == "windows":
                rows=connection.execute("SELECT window_key AS key,core_json AS payload FROM streaming_result_windows WHERE task_id=? AND generation=? AND window_key>? ORDER BY window_key LIMIT ?",(reference["task_id"],reference["generation"],after,limit+1)).fetchall()
            elif collection == "members" and window_key:
                # Tuple encoded with a fixed separator; kind is a registered constant.
                kind,key=(after.split(":",1) if ":" in after else ("",""))
                rows=connection.execute("SELECT kind,member_key,value_json,count FROM streaming_result_window_members WHERE task_id=? AND generation=? AND window_key=? AND (kind>? OR (kind=? AND member_key>?)) ORDER BY kind,member_key LIMIT ?",(reference["task_id"],reference["generation"],window_key,kind,kind,key,limit+1)).fetchall()
            else:
                raise ValueError("未知结果集合")
            items=[]; used=0; last=None
            for row in rows[:limit]:
                if collection == "windows":
                    last_key=row["key"]; item=dict(json.loads(row["payload"]),window_key=last_key,members_ref=dict(reference,window_key=last_key))
                else:
                    last_key=row["kind"]+":"+row["member_key"]; item={"kind":row["kind"],"value":json.loads(row["value_json"]),"count":row["count"]}
                size=len(_json(item).encode())
                if size>PAGE_BYTES:
                    raise ResultBudgetError("单事实超过分页字节预算")
                if used and used+size>PAGE_BYTES:
                    break
                items.append(item); used+=size; last=last_key
            return {"items":items,"next_key":last if len(items)<len(rows) else None}

    def _entity_pods(self, connection: Any, reference: dict[str,Any], entity_key: str) -> list[Any]:
        rows=connection.execute("SELECT DISTINCT m.value_json FROM streaming_result_window_members m JOIN streaming_result_windows w ON w.task_id=m.task_id AND w.generation=m.generation AND w.window_key=m.window_key WHERE w.task_id=? AND w.generation=? AND w.entity_key=? AND m.kind='affected_pods' ORDER BY m.value_json LIMIT 10001",(reference["task_id"],reference["generation"],entity_key))
        result=[]; used=0
        for row in rows:
            used += len(row[0].encode())
            if len(result)>=10000 or used>PAGE_BYTES:
                raise ResultBudgetError("单实体成员超过物化预算；请分页读取成员")
            result.append(json.loads(row[0]))
        return result

    def _hydrate(self,connection: Any,reference: dict[str,Any],window: dict[str,Any]) -> None:
        key=window.pop("_window_key")
        rows=connection.execute("SELECT kind,value_json,count FROM streaming_result_window_members WHERE task_id=? AND generation=? AND window_key=? ORDER BY kind,member_key LIMIT 10001",(reference["task_id"],reference["generation"],key))
        used=len(_json(window).encode()); count=0
        for row in rows:
            count += 1
            used += len(row["value_json"].encode())
            if count>10000 or used>PAGE_BYTES:
                raise ResultBudgetError("单窗口成员超过物化预算；完整成员仍保留")
            kind=row["kind"]; value=json.loads(row["value_json"])
            if kind == "semantic_fields":
                window.setdefault(kind,{}).setdefault(value["field"],[]).append({"value":value["value"],"count":row["count"]})
            elif kind == "typed_parameters":
                window.setdefault(kind,[]).append(dict(value,count=row["count"]))
            else:
                window.setdefault(kind,[]).append(value)
        for field in ("affected_namespaces","affected_pods","entity_keys","semantic_tags"):
            window[field]=sorted(window.get(field) or [])
        window.setdefault("entity_relations",[])
        for values in window.setdefault("semantic_fields",{}).values():
            values.sort(key=lambda value:(-value["count"],str(value["value"])))
        window.setdefault("typed_parameters",[]).sort(key=lambda value:(value["field"],value["typed_mask"]))
