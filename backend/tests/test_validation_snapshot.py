"""校验结论快照的集成单测：去重、原子切换、失败不丢数据、修复后恢复。

用 SQLite 内存库跑，不依赖外部服务；被测代码与线上一致（同一个 validation_service）。
"""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from src.models import Base
from src.models.metric import Metric
from src.models.report import Report
from src.models.validation_snapshot import ValidationSnapshot
from src.services import export_service, formula_service, metric_service, validation_service


@pytest.fixture()
def db():
    engine = create_engine("sqlite://", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    try:
        yield session
        session.commit()
    finally:
        session.close()
        engine.dispose()


def _metric(db, code, *, name=None, deps_dsl=None, enabled=True, status="active"):
    metric = metric_service.create_metric(
        db,
        {"code": code, "name": name or code, "domain": "trade", "tier": "normal", "status": status},
        author="admin@metricreg.io",
    )
    metric.enabled = enabled
    if deps_dsl is not None:
        formula_service.save_version(db, metric, dsl_text=deps_dsl, author_email="admin@metricreg.io")
    db.flush()
    return metric


def _report(db, code, metric_codes, *, core=False):
    db.add(
        Report(
            code=code,
            name=code,
            owner="测试",
            is_core=core,
            weight=3 if core else 1,
            metric_codes=list(metric_codes),
        )
    )
    db.flush()


def test_same_relation_rerun_reuses_snapshot(db):
    _metric(db, "gmv", deps_dsl="gmv = sum(dwd.order.amount)")
    _metric(db, "gmv_net", deps_dsl="gmv_net = sum(@gmv)")
    _report(db, "r01", ["gmv", "gmv_net"])
    db.commit()

    first = validation_service.recompute(db)
    db.commit()
    second = validation_service.recompute(db)
    db.commit()

    assert first["relation_hash"] == second["relation_hash"]
    assert second["reused"] is True
    assert db.scalar(select(func.count()).select_from(ValidationSnapshot)) == 1


def test_changed_relation_creates_new_snapshot_and_switches_atomically(db):
    _metric(db, "gmv", deps_dsl="gmv = sum(dwd.order.amount)")
    _metric(db, "gmv_net", deps_dsl="gmv_net = sum(@gmv)")
    db.commit()
    first = validation_service.recompute(db)
    db.commit()

    metric = db.scalar(select(Metric).where(Metric.code == "gmv"))
    metric.enabled = False
    db.flush()
    second = validation_service.recompute(db)
    db.commit()

    assert first["relation_hash"] != second["relation_hash"]
    assert second["reused"] is False
    actives = db.scalars(select(ValidationSnapshot).where(ValidationSnapshot.active.is_(True))).all()
    assert len(actives) == 1
    assert actives[0].relation_hash == second["relation_hash"]
    assert actives[0].summary["availability"]["gmv_net"]["status"] == "blocked"


def test_failed_recompute_keeps_previous_snapshot_and_data(db, monkeypatch):
    _metric(db, "gmv", deps_dsl="gmv = sum(dwd.order.amount)")
    db.commit()
    first = validation_service.recompute(db)
    db.commit()

    def boom(*args, **kwargs):
        raise RuntimeError("依赖边重建失败")

    monkeypatch.setattr("src.services.dependency_service.rebuild", boom)
    with pytest.raises(RuntimeError):
        with db.begin_nested():
            validation_service.recompute(db)
    db.rollback()

    snapshot = validation_service.active_snapshot(db)
    assert snapshot is not None
    assert snapshot.relation_hash == first["relation_hash"]
    assert validation_service.analysis(db)["metrics_total"] == 1


def test_fixing_upstream_restores_availability(db):
    _metric(db, "gmv", deps_dsl="gmv = sum(dwd.order.amount)")
    _metric(db, "gmv_net", deps_dsl="gmv_net = sum(@gmv)")
    db.commit()
    validation_service.recompute(db)
    db.commit()

    gmv = db.scalar(select(Metric).where(Metric.code == "gmv"))
    gmv.enabled = False
    db.flush()
    broken = validation_service.recompute(db)["analysis"]
    db.commit()
    assert broken["availability"]["gmv_net"]["status"] == "blocked"
    assert broken["availability"]["gmv_net"]["chain"] == ["gmv_net", "gmv"]

    gmv.enabled = True
    db.flush()
    fixed = validation_service.recompute(db)["analysis"]
    db.commit()
    assert fixed["availability"]["gmv_net"]["status"] == "available"
    assert fixed["available_total"] == fixed["metrics_total"]


def test_shared_downstream_counted_once_in_run_metrics(db):
    _metric(db, "a", deps_dsl="a = sum(dwd.a)")
    _metric(db, "b", deps_dsl="b = sum(dwd.b)")
    _metric(db, "shared", deps_dsl="shared = sum(@a) + sum(@b)")
    db.commit()
    validation_service.recompute(db)
    db.commit()

    a = db.scalar(select(Metric).where(Metric.code == "a"))
    b = db.scalar(select(Metric).where(Metric.code == "b"))
    a.enabled = False
    # b 的口径变成无法解析（例如上游解析器收紧语法），重算时应记为解析失败。
    b_formula = formula_service.latest_for(db, b)
    b_formula.dsl_text = "b = median(dwd.b)"
    db.flush()
    analysis = validation_service.recompute(db)["analysis"]
    db.commit()

    assert analysis["blocked_codes"] == ["a", "b", "shared"]
    assert analysis["blocked_total"] == 3
    assert analysis["availability"]["shared"]["chain"] == ["shared", "a"]


def test_export_columns_carry_one_chain_per_metric(db):
    _metric(db, "a", deps_dsl="a = sum(dwd.a)")
    _metric(db, "b", deps_dsl="b = sum(dwd.b)")
    _metric(db, "shared", deps_dsl="shared = sum(@a) + sum(@b)")
    db.commit()
    validation_service.recompute(db)
    db.commit()

    a = db.scalar(select(Metric).where(Metric.code == "a"))
    a.enabled = False
    db.flush()
    validation_service.recompute(db)
    db.commit()

    text = export_service.metrics_csv(db).decode("utf-8-sig")
    rows = text.splitlines()
    header = rows[0].split(",")
    assert "可用性" in header and "阻断链" in header and "阻断上游" in header
    line = next(row for row in rows if row.startswith("shared,"))
    assert "不可用" in line
    assert "shared → a" in line
    # 共享下游只出现一条链，不会因为两个上游各写一次
    assert line.count("shared → a") == 1
