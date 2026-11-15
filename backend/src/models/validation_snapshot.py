"""一次校验发布的关系快照：可用性结论 + 冲突/环，按关系指纹去重。

同一份关系快照（指标/口径/边完全一致）只保留一行诊断，重跑不会重复落库；
每一行带 active 标记，读取方只认当前 active 行，实现一次性切换。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from src.models import Base
from src.utils.timeutil import utcnow


class ValidationSnapshot(Base):
    __tablename__ = "validation_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(ForeignKey("runs.id", ondelete="SET NULL"), nullable=True, index=True)
    relation_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    summary: Mapped[dict] = mapped_column(JSON, default=dict)
    availability: Mapped[dict] = mapped_column(JSON, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
