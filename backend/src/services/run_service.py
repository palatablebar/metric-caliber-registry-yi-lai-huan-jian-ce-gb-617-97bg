"""校验运行服务：算法 A 解析全部口径 → 重建依赖图 → 算法 B 环检测 + 冲突 → 落统计。"""
from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.config.settings import settings
from src.models.run import Run
from src.services import validation_service
from src.utils.errors import not_found
from src.utils.timeutil import iso_utc, local_now, utcnow


def serialize(run: Run) -> dict:
    return {
        "id": run.id,
        "run_no": run.run_no,
        "trigger": run.trigger,
        "status": run.status,
        "operator_email": run.operator_email,
        "input_count": run.input_count,
        "output_count": run.output_count,
        "metrics": run.metrics or {},
        "log_text": run.log_text,
        "started_at": iso_utc(run.started_at),
        "finished_at": iso_utc(run.finished_at),
    }


def next_run_no(db: Session) -> str:
    prefix = f"V{local_now():%Y%m%d}"
    count = db.scalar(select(func.count()).select_from(Run).where(Run.run_no.like(f"{prefix}%"))) or 0
    return f"{prefix}-{count + 1:04d}"


def execute_run(db: Session, run: Run) -> Run:
    run.status = "running"
    try:
        # 整个「解析 → 重建依赖边 → 诊断 → 发布快照」放进一个 SAVEPOINT：
        # 任一步失败都整体回滚，上一份依赖边与快照原样保留，读侧不会看到半成品。
        with db.begin_nested():
            outcome = validation_service.recompute(db, run_id=run.id)
            summary = outcome["analysis"]
            dependency_stats = outcome["dependency_stats"]

            issue_count = len(summary["conflicts"]) + len(summary["cycles"]) + summary["parse_error_count"]
            run.input_count = outcome["metrics_total"]
            run.output_count = issue_count
            run.metrics = {
                "metrics_total": summary["metrics_total"],
                "parsed_ok": summary["metrics_total"] - summary["parse_error_count"],
                "parse_error_count": summary["parse_error_count"],
                "parse_errors": summary["parse_errors"],
                "conflicts_total": summary["conflicts_total"],
                "cycles_total": summary["cycles_total"],
                "available_total": summary["available_total"],
                "blocked_total": summary["blocked_total"],
                "blocked_codes": summary["blocked_codes"],
                "severity": summary["severity"],
                "dep_edges": dependency_stats["counts"],
                "high_conflicts": [item["name"] for item in summary["conflicts"] if item["severity"] == "high"],
                "cycle_paths": [" → ".join(item["path"]) for item in summary["cycles"]],
                "snapshot_id": outcome["snapshot_id"],
                "snapshot_hash": outcome["relation_hash"],
                "snapshot_reused": outcome["reused"],
            }
            run.status = "success" if outcome["metrics_total"] else "partial"
            run.log_text = (
                f"解析 {outcome['metrics_total']} 个指标口径，失败 {summary['parse_error_count']} 个；"
                f"可用 {summary['available_total']} 个，阻断 {summary['blocked_total']} 个；"
                f"依赖边 {dependency_stats['total']} 条（指标 {dependency_stats['counts']['metric']} / "
                f"字段 {dependency_stats['counts']['field']} / 报表 {dependency_stats['counts']['report']}）；"
                f"同名冲突 {summary['conflicts_total']} 处，依赖环 {summary['cycles_total']} 个；"
                f"新增边 {dependency_stats['added']}，清理边 {dependency_stats['removed']}；"
                f"快照 {outcome['relation_hash'][:12]}"
                + ("（复用，未重复诊断）" if outcome["reused"] else "")
            )
    except Exception as exc:  # noqa: BLE001 - 运行失败要留痕，不抛 500
        run.status = "failed"
        run.log_text = f"校验运行失败：{exc}"
        run.metrics = {"error": str(exc)[:300]}
    run.finished_at = utcnow()
    db.flush()
    return run


def create_and_execute(
    db: Session,
    *,
    trigger: str,
    operator_email: str,
    seed_key: str | None = None,
) -> Run:
    if seed_key:
        existing = db.scalar(select(Run).where(Run.seed_key == seed_key))
        if existing is not None:
            # seed 重跑不新增运行，但仍按当前关系补齐/复用一次结论快照
            # （老库升级到带快照的版本时，读侧也能拿到统一的结论）。
            validation_service.recompute(db, run_id=existing.id)
            return existing
    run = Run(
        run_no=next_run_no(db),
        trigger=trigger,
        status="running",
        operator_email=operator_email,
        seed_key=seed_key,
    )
    db.add(run)
    db.flush()
    return execute_run(db, run)


def list_runs(db: Session, *, status: str | None, limit: int, offset: int) -> dict:
    stmt = select(Run)
    count_stmt = select(func.count()).select_from(Run)
    if status:
        stmt = stmt.where(Run.status == status)
        count_stmt = count_stmt.where(Run.status == status)
    total = db.scalar(count_stmt) or 0
    rows = db.scalars(stmt.order_by(Run.id.desc()).limit(limit).offset(offset)).all()
    return {"items": [serialize(row) for row in rows], "total": total, "limit": limit, "offset": offset}


def get_run(db: Session, run_id: int) -> Run:
    run = db.get(Run, run_id)
    if run is None:
        raise not_found("run", run_id)
    return run


def app_name() -> str:
    return settings.app_name
