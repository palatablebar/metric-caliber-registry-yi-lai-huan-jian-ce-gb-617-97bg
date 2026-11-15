"""指标可用性传播单测：停用 / 解析失败 / 间接依赖 / 共享下游去重 / 恢复 / 确定性。"""
from __future__ import annotations

from src.services import graph_analysis


def _metric(code, *, enabled=True, status="active", parse_status="ok", deps=None, name=None):
    return {
        "code": code,
        "name": name or code,
        "domain": "trade",
        "tier": "normal",
        "owner": "测试",
        "status": status,
        "enabled": enabled,
        "signature": f"sig-{code}",
        "parse_status": parse_status,
        "aggs": ["sum"],
        "fields": ["t.x"],
        "filters": [],
        "dims": [],
        "deps": deps or [],
        "expression": f"{code} = sum(t.x)",
    }


def test_disabled_upstream_blocks_direct_and_indirect_downstream():
    metrics = [
        _metric("root", enabled=False),
        _metric("mid", deps=["root"]),
        _metric("leaf", deps=["mid"]),
        _metric("other"),
    ]
    verdicts = graph_analysis.availability_map(metrics)
    assert verdicts["root"]["status"] == "blocked"
    assert verdicts["root"]["chain"] == ["root"]
    assert verdicts["root"]["block_reason"] == "disabled"
    assert verdicts["mid"]["status"] == "blocked"
    assert verdicts["mid"]["chain"] == ["mid", "root"]
    assert verdicts["mid"]["blocked_by"] == "root"
    assert verdicts["leaf"]["status"] == "blocked"
    assert verdicts["leaf"]["chain"] == ["leaf", "mid", "root"]
    assert verdicts["other"]["status"] == "available"


def test_parse_error_upstream_blocks_downstream():
    metrics = [
        _metric("bad", parse_status="error"),
        _metric("dep", deps=["bad"]),
    ]
    verdicts = graph_analysis.availability_map(metrics)
    assert verdicts["bad"]["block_reason"] == "parse_error"
    assert verdicts["dep"]["status"] == "blocked"
    assert verdicts["dep"]["chain"] == ["dep", "bad"]


def test_deprecated_upstream_is_blocked():
    metrics = [_metric("old", status="deprecated"), _metric("dep", deps=["old"])]
    verdicts = graph_analysis.availability_map(metrics)
    assert verdicts["old"]["block_reason"] == "deprecated"
    assert verdicts["dep"]["status"] == "blocked"


def test_shared_downstream_with_two_bad_upstreams_reports_one_chain():
    metrics = [
        _metric("bad_a", enabled=False),
        _metric("bad_b", parse_status="error"),
        _metric("shared", deps=["bad_a", "bad_b"]),
    ]
    verdicts = graph_analysis.availability_map(metrics)
    shared = verdicts["shared"]
    assert shared["status"] == "blocked"
    # 一个下游只给一条链，同长度取字典序最小（bad_a < bad_b）
    assert shared["chain"] == ["shared", "bad_a"]
    assert shared["blocked_by"] == "bad_a"
    blocked = [code for code, item in verdicts.items() if item["status"] == "blocked"]
    assert blocked == ["bad_a", "bad_b", "shared"]  # 去重：shared 只计一次


def test_shortest_chain_wins_when_two_paths_exist():
    metrics = [
        _metric("bad", enabled=False),
        _metric("long", deps=["bad"]),
        _metric("target", deps=["long", "bad"]),
    ]
    verdicts = graph_analysis.availability_map(metrics)
    assert verdicts["target"]["chain"] == ["target", "bad"]


def test_fixing_upstream_restores_downstream():
    broken = [
        _metric("root", enabled=False),
        _metric("mid", deps=["root"]),
        _metric("leaf", deps=["mid"]),
    ]
    assert graph_analysis.availability_map(broken)["leaf"]["status"] == "blocked"

    fixed = [
        _metric("root"),
        _metric("mid", deps=["root"]),
        _metric("leaf", deps=["mid"]),
    ]
    verdicts = graph_analysis.availability_map(fixed)
    assert all(item["status"] == "available" for item in verdicts.values())
    assert all(item["chain"] == [] for item in verdicts.values())


def test_availability_is_deterministic():
    metrics = [
        _metric("bad_b", parse_status="error"),
        _metric("bad_a", enabled=False),
        _metric("shared", deps=["bad_b", "bad_a"]),
        _metric("leaf", deps=["shared"]),
    ]
    first = graph_analysis.availability_map(metrics)
    second = graph_analysis.availability_map(list(reversed(metrics)))
    assert first == second


def test_summary_counts_blocked_once_per_metric():
    metrics = [
        _metric("bad_a", enabled=False),
        _metric("bad_b", parse_status="error"),
        _metric("shared", deps=["bad_a", "bad_b"]),
        _metric("leaf", deps=["shared"]),
        _metric("ok"),
    ]
    summary = graph_analysis.summarize(metrics, {})
    assert summary["blocked_total"] == 4
    assert summary["available_total"] == 1
    assert summary["blocked_codes"] == ["bad_a", "bad_b", "leaf", "shared"]
    assert len(summary["availability"]["shared"]["chain"]) == 2


def test_cycle_members_without_defect_stay_available():
    metrics = [
        _metric("a", deps=["b"]),
        _metric("b", deps=["a"]),
    ]
    verdicts = graph_analysis.availability_map(metrics)
    assert verdicts["a"]["status"] == "available"
    assert verdicts["b"]["status"] == "available"


def test_disabled_metric_in_cycle_blocks_other_members():
    metrics = [
        _metric("a", enabled=False, deps=["b"]),
        _metric("b", deps=["a"]),
    ]
    verdicts = graph_analysis.availability_map(metrics)
    assert verdicts["a"]["status"] == "blocked"
    assert verdicts["b"]["chain"] == ["b", "a"]
