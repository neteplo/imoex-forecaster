from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

BigIntAuto = BigInteger().with_variant(Integer(), "sqlite")


class Base(DeclarativeBase):
    pass


class Candle(Base):
    __tablename__ = "candles"

    dt: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    open: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)


class News(Base):
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(BigIntAuto, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(128), nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    title: Mapped[str | None] = mapped_column(String(512))
    body: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[str | None] = mapped_column(String(512))

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_news_source"),
        Index("ix_news_ts", "ts"),
    )


class Prediction(Base):
    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(BigIntAuto, primary_key=True, autoincrement=True)
    dt: Mapped[datetime] = mapped_column(DateTime, unique=True, nullable=False, index=True)
    y_pred: Mapped[float] = mapped_column(Float, nullable=False)
    n_news: Mapped[int] = mapped_column(Integer, nullable=False)
    ret_1: Mapped[float | None] = mapped_column(Float)
    ret_60: Mapped[float | None] = mapped_column(Float)
    ret_120: Mapped[float | None] = mapped_column(Float)
    ner_org_weight_sum_mean: Mapped[float | None] = mapped_column(Float)
    ner_has_top_company_any: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)


class Subscription(Base):
    __tablename__ = "subscriptions"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    threshold_pct: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)


class Notification(Base):
    __tablename__ = "notifications"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    dt: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
