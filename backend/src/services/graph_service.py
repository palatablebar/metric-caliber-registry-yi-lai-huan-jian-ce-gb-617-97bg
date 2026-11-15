"""依赖图服务：装配算法 B 的输入，并对外提供「一次校验发布的那份结论」。

统一口径：
- 校验运行把可用性 / 冲突 / 环 / 图结构整体算一次，发布为 active 快照；
- 总览、详情、依赖图、冲突页与导出都通过 `current_analysis` 读取同一份结论；
- 关系变化但尚未成功发布时（直接改库 / 尚未跑过校验），回退到实时计算，保证读侧一致。
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.constants.enums import BLOCK_REASON_LABELS, METRIC_DOMAIN_LABELS
from src.models.metric import Metric
from src.services import formula_service, graph_analysis, report_service, validation_service
from src.utils.errors import AppError


def collect_inputs(db: Session) -> tuple[list[dict], dict[str, list[dict]], dict[str, Metric]]:
    metrics = db.scalars(select(Metric).order_by(Metric.code)).all()
    latest = formula_service.latest_map(db)
    reports_index = report_service.reports_by_metric(db)
    payloads: list[dict] = []
    for metric in metrics:
        formula = latest.get(metric.id)
        parsed = (formula.parsed or {}) if formula else {}
        parse_status = formula.parse_status if formula else "error"
        payloads.append(
            {
                "metric_id": metric.id,
                "code": metric.code,
                "name": metric.name,
                "domain": metric.domain,
                "domain_label": METRIC_DOMAIN_LABELS.get(metric.domain, metric.domain),
                "tier": metric.tier,
                "owner": metric.owner,
                "status": metric.status,
                "enabled": bool(metric.enabled),
                "version": metric.current_version,
                "signature": formula.signature if formula else "",
                "parse_status": parse_status,
                "aggs": parsed.get("aggs") or [],
                "fields": parsed.get("fields") or [],
                "filters": parsed.get("filters") or [],
                "dims": parsed.get("dims") or [],
                "deps": parsed.get("deps") or [],
                "expression": parsed.get("expression") or "",
            }
        )
    return payloads, reports_index, {metric.code: metric for metric in metrics}


def current_analysis(db: Session) -> dict:
    """当前生效的校验结论（一次校验一次性切换，各页面共用）。"""
    return validation_service.analysis(db)


def availability_map(db: Session) -> dict[str, dict]:
    """每个指标的可用性结论（自身 + 全部间接上游正常才可用）。"""
    return current_analysis(db)["availability"]


def availability_report(db: Session) -> dict:
    """把可用性结论整理成统一口径的读模型，供总览 / 详情 / 依赖图 / 导出共用。"""
    analysis = current_analysis(db)
    verdicts = analysis["availability"]
    nodes = {node["code"]: node for node in analysis["nodes"]}
    blocked = analysis["blocked_codes"]
    reasons: dict[str, int] = {}
    for code in blocked:
        reason = verdicts[code]["block_reason"]
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "metrics_total": analysis["metrics_total"],
        "available_total": analysis["available_total"],
        "blocked_total": analysis["blocked_total"],
        "reasons": [
            {"key": key, "label": BLOCK_REASON_LABELS.get(key, key), "count": reasons[key]}
            for key in sorted(reasons)
        ],
        "items": [
            {
                "code": code,
                "name": nodes.get(code, {}).get("name", code),
                "status": item["status"],
                "block_reason": item["block_reason"],
                "blocked_by": item["blocked_by"],
                "blocked_by_name": item.get("blocked_by_name"),
                "chain": item["chain"],
                "chain_names": item.get("chain_names") or [nodes.get(hop, {}).get("name", hop) for hop in item["chain"]],
                "summary": item["summary"],
            }
            for code, item in sorted(verdicts.items())
        ],
    }


def find_conflicts(db: Session) -> list[dict]:
    return current_analysis(db)["conflicts"]


def find_cycles(db: Session) -> list[dict]:
    return current_analysis(db)["cycles"]


def summary(db: Session) -> dict:
    return current_analysis(db)


def graph(db: Session) -> dict:
    analysis = current_analysis(db)
    edges = analysis["edges"]
    return {
        "nodes": analysis["nodes"],
        "edges": edges,
        "dep_edges": [edge for edge in edges if edge["kind"] == "metric"],
        "cycles": analysis["cycles"],
        "blocked": [
            {
                "code": code,
                "name": next(
                    (node["name"] for node in analysis["nodes"] if node["code"] == code), code
                ),
                "reason": analysis["availability"][code]["block_reason"],
                "blocked_by": analysis["availability"][code]["blocked_by"],
                "chain": analysis["availability"][code]["chain"],
                "chain_names": analysis["availability"][code].get("chain_names") or [],
                "summary": analysis["availability"][code]["summary"],
            }
            for code in analysis["blocked_codes"]
        ],
        "stats": analysis["stats"],
    }


def impact_for_field(db: Session, field: str) -> dict:
    metrics, reports_index, _ = collect_inputs(db)
    impact = graph_analysis.impact_of_field(field, metrics, reports_index)
    if not impact.get("metric_codes"):
        raise AppError("NOT_FOUND", 404, {"entity": "field", "field": field})
    return impact


def impact_for_metric(db: Session, code: str) -> dict:
    metrics, reports_index, by_code = collect_inputs(db)
    if code not in by_code:
        raise AppError("NOT_FOUND", 404, {"entity": "metric", "code": code})
    return graph_analysis.impact_of_metric(code, metrics, reports_index)
