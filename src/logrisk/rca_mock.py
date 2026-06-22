from __future__ import annotations

from typing import Any, Dict, List


def mock_rca_for_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    templates = entity.get("top_templates", [])
    text = " ".join((t.get("component", "") + " " + t.get("template", "") + " " + str(t.get("category", ""))).lower() for t in templates)

    if "out of memory" in text or "oom" in text or "eviction" in text:
        root = "高概率为节点内存压力 / OOM 引发 kubelet 驱逐或业务 Pod 异常"
        confidence = 0.86
        actions = [
            "检查该节点 memory.available、OOM 记录和异常进程。",
            "检查受影响 Pod 的 requests/limits 配置。",
            "检查近期是否有调度、发布或资源配额变更。",
        ]
    elif "containerd" in text or "shim" in text or "oci runtime" in text:
        root = "高概率为容器运行时异常导致 Pod 创建或启动失败"
        confidence = 0.78
        actions = [
            "检查 containerd 服务状态和错误日志。",
            "检查 runtime、cgroup、镜像解压和磁盘空间。",
            "确认同节点是否存在批量 Pod 创建失败。",
        ]
    elif "disk pressure" in text or "garbage collection" in text:
        root = "可能为节点磁盘压力或镜像垃圾回收异常"
        confidence = 0.72
        actions = [
            "检查 node filesystem、imagefs 使用率。",
            "清理无用镜像和异常日志文件。",
            "检查 kubelet image garbage collection 配置。",
        ]
    else:
        root = "当前证据不足，可能为业务侧或依赖侧异常"
        confidence = 0.45
        actions = [
            "补充节点指标、Pod 重启记录和依赖服务日志。",
            "检查异常是否集中在单节点或跨节点扩散。",
            "对比故障窗口前后的发布和变更记录。",
        ]

    evidence_chain = []
    for t in templates[:5]:
        evidence_chain.append({
            "time": f"{t.get('first_seen')} ~ {t.get('last_seen')}",
            "evidence": f"{t.get('component')} / {t.get('template')} / count={t.get('count')}",
            "interpretation": t.get("rca_hint") or "异常模板参与风险评分。",
        })

    return {
        "window_start": entity.get("window_start"),
        "window_end": entity.get("window_end"),
        "cluster": entity.get("cluster"),
        "risk_entity": {
            "type": entity.get("entity_type"),
            "id": entity.get("entity_id"),
        },
        "risk_score": entity.get("risk_score"),
        "risk_level": entity.get("risk_level"),
        "root_cause_candidate": root,
        "confidence": confidence,
        "evidence_chain": evidence_chain,
        "impact": f"影响对象：{', '.join(entity.get('affected_entities') or [])}" if entity.get("affected_entities") else "影响面需要结合 Pod/Service 拓扑进一步确认。",
        "suggested_actions": actions,
        "need_more_data": [
            "节点 CPU/内存/磁盘/网络指标",
            "Pod restart/eviction 事件",
            "变更发布记录",
            "相关告警记录",
        ],
    }


def generate_mock_rca(entities: list[Dict[str, Any]], min_score: float = 40) -> list[Dict[str, Any]]:
    return [mock_rca_for_entity(e) for e in entities if float(e.get("risk_score") or 0) >= min_score]
