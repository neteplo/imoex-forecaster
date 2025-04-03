from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import settings
from src.storage.models import Base, Candle, News, Notification, Prediction, Subscription

_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    global _engine, _SessionLocal
    if _engine is None:
        _engine = create_engine(settings.database_url, future=True, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    return _engine


def init_schema() -> None:
    Base.metadata.create_all(get_engine())


@contextmanager
def session_scope() -> Iterator[Session]:
    get_engine()
    assert _SessionLocal is not None
    s = _SessionLocal()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def _parse_dt(dt: str | datetime) -> datetime:
    if isinstance(dt, datetime):
        return dt
    return datetime.fromisoformat(dt)


def save_prediction(session: Session, payload: dict[str, Any]) -> None:
    dt = _parse_dt(payload["dt"])
    existing = session.execute(
        select(Prediction).where(Prediction.dt == dt)
    ).scalar_one_or_none()
    if existing is None:
        existing = Prediction(dt=dt)
        session.add(existing)
    existing.y_pred = float(payload["y_pred"])
    existing.n_news = int(payload["n_news"])
    existing.ret_1 = payload.get("ret_1")
    existing.ret_60 = payload.get("ret_60")
    existing.ret_120 = payload.get("ret_120")
    existing.ner_org_weight_sum_mean = payload.get("ner_org_weight_sum_mean")
    existing.ner_has_top_company_any = bool(payload.get("ner_has_top_company_any"))


def recent_predictions(session: Session, limit: int) -> list[dict[str, Any]]:
    rows = session.execute(
        select(Prediction).order_by(Prediction.dt.desc()).limit(limit)
    ).scalars().all()
    return [
        {
            "dt": r.dt.isoformat(sep="T"),
            "y_pred": r.y_pred,
            "n_news": r.n_news,
            "ner_has_top_company_any": int(r.ner_has_top_company_any),
        }
        for r in rows
    ]


def prediction_by_dt(session: Session, dt: datetime) -> dict[str, Any] | None:
    row = session.execute(
        select(Prediction).where(Prediction.dt == dt)
    ).scalar_one_or_none()
    if row is None:
        return None
    return {
        "dt": row.dt.isoformat(sep="T"),
        "y_pred": row.y_pred,
        "n_news": row.n_news,
        "ner_has_top_company_any": int(row.ner_has_top_company_any),
    }


def upsert_subscription(session: Session, chat_id: int, threshold_pct: float) -> None:
    sub = session.get(Subscription, chat_id)
    if sub is None:
        session.add(Subscription(chat_id=chat_id, threshold_pct=float(threshold_pct)))
    else:
        sub.threshold_pct = float(threshold_pct)


def delete_subscription(session: Session, chat_id: int) -> int:
    result = session.execute(delete(Subscription).where(Subscription.chat_id == chat_id))
    return result.rowcount or 0


def list_subscriptions(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(
        select(Subscription).order_by(Subscription.chat_id)
    ).scalars().all()
    return [{"chat_id": r.chat_id, "threshold_pct": r.threshold_pct} for r in rows]


def was_notified(session: Session, chat_id: int, dt: str | datetime) -> bool:
    dt_parsed = _parse_dt(dt)
    row = session.get(Notification, (chat_id, dt_parsed))
    return row is not None


def mark_notified(session: Session, chat_id: int, dt: str | datetime) -> None:
    dt_parsed = _parse_dt(dt)
    if session.get(Notification, (chat_id, dt_parsed)) is None:
        session.add(Notification(chat_id=chat_id, dt=dt_parsed))


def upsert_candles(session: Session, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    stmt = pg_insert(Candle).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["dt"],
        set_={"open": stmt.excluded.open, "close": stmt.excluded.close},
    )
    session.execute(stmt)


def candles_overview(session: Session) -> tuple[int, datetime | None, datetime | None]:
    row = session.execute(
        select(func.count(Candle.dt), func.min(Candle.dt), func.max(Candle.dt))
    ).one()
    return int(row[0] or 0), row[1], row[2]


def latest_candle_dt(session: Session) -> datetime | None:
    return session.execute(select(func.max(Candle.dt))).scalar()


def insert_news(session: Session, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    stmt = pg_insert(News).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=["source", "source_id"]).returning(News.id)
    inserted_ids = session.execute(stmt).scalars().all()
    return len(inserted_ids)
