"""校验快照服务：把一次校验的结论按「关系指纹」去重落库，并原子切换。

同一份关系快照（指标状态/口径签名/依赖边/报表引用完全一致）只保留一行诊断，
重跑直接复用并重新激活，不会重复落库；失败时整体回滚，上一份快照与依赖边保持可用。
读取方（总览 / 详情 / 依赖图 / 冲突 / 导出）只认当前 active 快照，保证一次性切换。
"""
from __future__ import annotations

import hashlib
import json

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from src.models.validation_snapshot import ValidationSnapshot
from src.services import graph_analysis


def relation_hash(metrics: list[dict], reports_index: dict[str, list[dict]]) -> str:
    """关系指纹：覆盖所有会影响诊断结论与展示内容的输入。

    只覆盖输入本身（不含 run 编号、时间），所以同一关系快照重跑一定得到同一指纹。
    """
    payload = {
        "metrics": [
            {
                "code": metric["code"],
                "name": metric["name"],
                "domain": metric.get("domain"),
                "tier": metric.get("tier"),
                "owner": metric.get("owner"),
                "enabled": bool(metric.get("enabled", True)),
                "status": metric.get("status"),
                "parse_status": metric.get("parse_status"),
                "signature": metric.get("signature") or "",
                "expression": metric.get("expression") or "",
                "deps": sorted(metric.get("deps") or []),
                "fields": sorted(metric.get("fields") or []),
                "filters": list(metric.get("filters") or []),
                "aggs": list(metric.get("aggs") or []),
                "dims": list(metric.get("dims") or []),
            }
            for metric in sorted(metrics, key=lambda item: item["code"])
        ],
        "reports": {
            code: [
                {
                    "code": report["code"],
                    "name": report["name"],
                    "is_core": bool(report.get("is_core")),
                    "weight": report.get("weight"),
                }
                for report in sorted(reports, key=lambda item: item["code"])
            ]
            for code, reports in sorted(reports_index.items())
        },
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def active_snapshot(db: Session) -> ValidationSnapshot | None:
    return db.scalar(
        select(ValidationSnapshot)
        .where(ValidationSnapshot.active.is_(True))
        .order_by(ValidationSnapshot.id.desc())
        .limit(1)
    )


def load(db: Session) -> ValidationSnapshot | None:
    """当前生效的快照行（没有则返回 None，读侧会实时计算）。"""
    return active_snapshot(db)


def _usable_summary(summary: dict | None) -> bool:
    """快照结构完整性校验：老版本代码写的快照缺失字段时按实时计算处理。"""
    return isinstance(summary, dict) and all(
        key in summary for key in ("availability", "conflicts", "cycles", "nodes", "blocked_codes")
    )


def analysis(db: Session) -> dict:
    """统一读模型：当前关系对应的一份分析结论（各页面与导出共用）。

    关系未变时直接复用 active 快照（同一快照重跑不重复诊断）；
    关系已被改动但尚未成功发布时（例如直接改库、或校验失败后关系未回滚干净），
    按当前关系实时计算，保证总览/详情/依赖图/导出看到的结论一致。
    """
    from src.services import graph_service

    snapshot = active_snapshot(db)
    metrics, reports_index, _ = graph_service.collect_inputs(db)
    fingerprint = relation_hash(metrics, reports_index)
    if snapshot is not None and _usable_summary(snapshot.summary) and snapshot.relation_hash == fingerprint:
        return snapshot.summary
    return graph_analysis.build_analysis(metrics, reports_index)


def recompute(db: Session, *, run_id: int | None = None) -> dict:
    """解析全部口径 → 重建依赖边 → 诊断 → 发布快照，返回一次校验的完整结论。

    校验运行与「改口径 / 启停指标」后的即时刷新共用这一条路径：
    调用方负责事务边界，保证依赖边与快照在同一事务里一次性切换。
    """
    from sqlalchemy import select

    from src.models.formula import Formula
    from src.models.metric import Metric
    from src.services import dependency_service, dsl_parser, formula_service, graph_service

    metrics = db.scalars(select(Metric).order_by(Metric.code)).all()
    latest = formula_service.latest_map(db)
    parsed_by_code: dict[str, dict] = {}
    for metric in metrics:
        formula: Formula | None = latest.get(metric.id)
        parsed = dsl_parser.parse_dsl(formula.dsl_text if formula else "", metric.code)
        parsed_by_code[metric.code] = parsed
        if formula is not None:
            formula_service.apply_parse(formula, parsed)
    db.flush()

    dependency_stats = dependency_service.rebuild(db, metrics, parsed_by_code)
    payloads, reports_index, _ = graph_service.collect_inputs(db)
    published = publish(db, run_id=run_id, metrics=payloads, reports_index=reports_index)
    return {
        "metrics_total": len(metrics),
        "dependency_stats": dependency_stats,
        "analysis": published["analysis"],
        "snapshot_id": published["id"],
        "relation_hash": published["relation_hash"],
        "reused": published["reused"],
    }


def publish(db: Session, *, run_id: int | None, metrics: list[dict], reports_index: dict[str, list[dict]]) -> dict:
    """发布一份快照：同一关系指纹复用旧行，其余行取消激活。

    调用方必须把本函数与依赖边重建放在同一事务里，保证读侧一次性切换。
    关系未变化时直接复用旧结论，不再重复诊断（同一快照重跑不会重复记账）。
    """
    fingerprint = relation_hash(metrics, reports_index)
    existing = db.scalar(select(ValidationSnapshot).where(ValidationSnapshot.relation_hash == fingerprint))

    db.execute(
        update(ValidationSnapshot)
        .where(ValidationSnapshot.active.is_(True))
        .values(active=False)
    )

    reused = existing is not None
    if existing is not None and _usable_summary(existing.summary):
        analysis = existing.summary
        existing.run_id = run_id
        existing.availability = analysis["availability"]
        existing.active = True
    elif existing is not None:
        analysis = graph_analysis.build_analysis(metrics, reports_index)
        existing.run_id = run_id
        existing.summary = analysis
        existing.availability = analysis["availability"]
        existing.active = True
    else:
        analysis = graph_analysis.build_analysis(metrics, reports_index)
        existing = ValidationSnapshot(
            run_id=run_id,
            relation_hash=fingerprint,
            summary=analysis,
            availability=analysis["availability"],
            active=True,
        )
        db.add(existing)
    db.flush()
    return {
        "id": existing.id,
        "relation_hash": fingerprint,
        "reused": reused,
        "analysis": analysis,
    }
