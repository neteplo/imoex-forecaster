from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from src.api.schemas import (
    CompanyContributionOut,
    ExplainOut,
    HistoryItem,
    HistoryOut,
    NewsContributionOut,
    PredictionOut,
)
from src.config import settings
from src.inference.cache import PredictionCache
from src.inference.explain import explain_at
from src.inference.predict_worker import RESULT_PREFIX
from src.inference.queue import publish_predict_task
from src.inference.worker import _latest_valid_dt, _latest_valid_dts, load_artifacts
from src.storage.db import (
    init_schema,
    prediction_by_dt,
    save_prediction,
    session_scope,
)

POLL_INTERVAL_SEC = 0.1

state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_schema()
    state["artifacts"] = load_artifacts()
    state["cache"] = PredictionCache()
    yield
    state.clear()


app = FastAPI(title="imoex-forecaster", version="0.1.0", lifespan=lifespan)


def _resolve_dt(dt: str | None) -> datetime:
    if dt is None:
        return _latest_valid_dt()
    try:
        return datetime.fromisoformat(dt)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Невалидный dt: {exc}") from exc


def _db_payload(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "dt": result["dt"],
        "y_pred": result["y_pred"],
        "n_news": result["n_news"],
        "ret_1": result["ret_1"],
        "ret_60": result["ret_60"],
        "ret_120": result["ret_120"],
        "ner_org_weight_sum_mean": result["ner_org_weight_sum_mean"],
        "ner_has_top_company_any": result["ner_has_top_company_any"],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


async def _compute_via_worker(dt_iso: str) -> dict[str, Any]:
    cache: PredictionCache = state["cache"]
    request_id = await asyncio.to_thread(publish_predict_task, dt_iso)
    timeout = settings.predict_task_timeout_sec
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        result = cache.get_raw(f"{RESULT_PREFIX}{request_id}")
        if result is None:
            continue
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        with session_scope() as session:
            save_prediction(session, _db_payload(result))
        return result
    raise HTTPException(
        status_code=504,
        detail=f"predict-worker не ответил за {timeout}с (req={request_id})",
    )


@app.get("/predict", response_model=PredictionOut)
async def predict(dt: str | None = Query(default=None, description="ISO-таймстемп закрытия свечи, МСК")):
    t = _resolve_dt(dt)
    cache: PredictionCache = state["cache"]
    cache_key = t.isoformat()

    cached = cache.get(cache_key)
    if cached is not None:
        return PredictionOut(**cached)

    result = await _compute_via_worker(cache_key)
    return PredictionOut(**result)


def _history_item_from_payload(
    dt: datetime, y_pred: float, n_news: int, has_top: bool,
) -> HistoryItem:
    return HistoryItem(
        dt=dt,
        y_pred=y_pred,
        y_pred_pct=y_pred * 100,
        n_news=n_news,
        ner_has_top_company_any=has_top,
    )


async def _resolve_history_item(dt: datetime) -> HistoryItem:
    iso = dt.isoformat()
    cache: PredictionCache = state["cache"]

    cached = cache.get(iso)
    if cached is not None:
        return _history_item_from_payload(
            dt, float(cached["y_pred"]), int(cached["n_news"]),
            bool(cached["ner_has_top_company_any"]),
        )

    with session_scope() as session:
        row = prediction_by_dt(session, dt)
    if row is not None:
        return _history_item_from_payload(
            dt, float(row["y_pred"]), int(row["n_news"]),
            bool(row["ner_has_top_company_any"]),
        )

    result = await _compute_via_worker(iso)
    return _history_item_from_payload(
        dt, float(result["y_pred"]), int(result["n_news"]),
        bool(result["ner_has_top_company_any"]),
    )


@app.get("/history", response_model=HistoryOut)
async def history(
    k: int = Query(default=5, ge=1, le=50, description="Сколько часов назад вернуть прогнозов"),
):
    try:
        dts = await asyncio.to_thread(_latest_valid_dts, k)
    except SystemExit as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    items = [await _resolve_history_item(dt) for dt in dts]
    return HistoryOut(items=items)


@app.get("/explain", response_model=ExplainOut)
def explain(
    dt: str | None = Query(default=None),
    top_news: int = Query(default=5, ge=1, le=20),
    top_companies: int = Query(default=5, ge=1, le=20),
):
    t = _resolve_dt(dt)
    try:
        result = explain_at(
            state["artifacts"], t,
            top_news=top_news, top_companies=top_companies,
        )
    except SystemExit as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ExplainOut(
        dt=result.dt,
        y_pred=result.y_pred,
        y_pred_pct=result.y_pred * 100,
        y_no_news_pct=result.y_no_news * 100,
        market_status=result.market_status,
        window_start=result.window_start,
        window_end=result.window_end,
        top_news=[
            NewsContributionOut(
                title=n.title,
                contribution=n.contribution,
                contribution_pct=n.contribution * 100,
                tickers=n.tickers,
            )
            for n in result.top_news
        ],
        top_companies=[
            CompanyContributionOut(
                ticker=c.ticker,
                name=c.name,
                n_news=c.n_news,
                contribution=c.contribution,
                contribution_pct=c.contribution * 100,
            )
            for c in result.top_companies
        ],
    )
