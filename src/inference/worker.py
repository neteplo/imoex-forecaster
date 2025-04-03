from __future__ import annotations

import argparse
import pickle
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from gensim.models import KeyedVectors
from sklearn.preprocessing import StandardScaler
from sqlalchemy import select

from src.common.time_utils import market_is_open, now_msk
from src.config import settings
from src.ml.dataset import (
    EMBED_DIM,
    NUMERIC_DIM,
    FitState,
    build_numeric_row,
    embed_news,
)
from src.ml.lstm import NewsLSTM
from src.preprocessing.features import add_returns, add_time_features
from src.preprocessing.ner import (
    build_matcher,
    extract_for_row,
    load_tickers,
    top_tickers,
)
from src.preprocessing.text_clean import clean, normalize_title
from src.storage.db import session_scope
from src.storage.models import Candle, News

DEFAULT_W2V = settings.paths.w2v
DEFAULT_LSTM = settings.paths.lstm
DEFAULT_SCALER = settings.paths.scaler
DEFAULT_TICKERS = settings.paths.tickers
DEFAULT_TOP_N = 5

HIDDEN_SIZE = 256
NUM_LAYERS = 1
DROPOUT = 0.2
WINDOW_HOURS = 4
MAX_NEWS_IN_WINDOW = 50


def news_window(t: datetime, now: datetime | None = None) -> tuple[datetime, datetime, str]:
    now = now or now_msk()
    base_start = t - timedelta(hours=WINDOW_HOURS)
    if market_is_open(now):
        return base_start, t, "open"
    return base_start, now, "closed"


@dataclass
class NerContext:
    pattern: re.Pattern
    variant_to_ticker: dict[str, str]
    weights: dict[str, float]
    names: dict[str, str]
    top_set: set[str]


@dataclass
class InferenceArtifacts:
    kv: KeyedVectors
    model: NewsLSTM
    scaler: StandardScaler
    ner: NerContext
    device: torch.device


@dataclass
class PredictionResult:
    dt: datetime
    y_pred: float
    n_news: int
    n_news_window_total: int
    ret_1: float
    ret_60: float
    ret_120: float
    ner_org_weight_sum_mean: float
    ner_has_top_company_any: bool
    market_status: str
    window_start: datetime
    window_end: datetime


def pick_device(arg: str | None) -> torch.device:
    if arg:
        if arg == "cuda" and not torch.cuda.is_available():
            raise SystemExit("--device cuda, но cuda недоступна")
        return torch.device(arg)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def load_artifacts(
    w2v_path: Path = DEFAULT_W2V,
    lstm_path: Path = DEFAULT_LSTM,
    scaler_path: Path = DEFAULT_SCALER,
    tickers_path: Path = DEFAULT_TICKERS,
    top_n: int = DEFAULT_TOP_N,
    device: str | None = None,
) -> InferenceArtifacts:
    dev = pick_device(device)

    kv = KeyedVectors.load(str(w2v_path))
    if kv.vector_size != EMBED_DIM:
        raise RuntimeError(f"W2V dim={kv.vector_size}, ожидалось {EMBED_DIM}")

    model = NewsLSTM(
        embed_dim=EMBED_DIM,
        hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS,
        num_numeric=NUMERIC_DIM,
        dropout=DROPOUT,
    )
    state = torch.load(lstm_path, map_location=dev)
    model.load_state_dict(state)
    model.to(dev).eval()

    with scaler_path.open("rb") as f:
        fit_state: FitState = pickle.load(f)

    tickers = load_tickers(tickers_path)
    pattern, variant_to_ticker = build_matcher(tickers)
    weights = {t: float(info.get("weight", 0.0)) for t, info in tickers.items()}
    names = {t: info.get("name", t) for t, info in tickers.items()}
    ner = NerContext(
        pattern=pattern,
        variant_to_ticker=variant_to_ticker,
        weights=weights,
        names=names,
        top_set=top_tickers(tickers, top_n),
    )

    return InferenceArtifacts(
        kv=kv,
        model=model,
        scaler=fit_state.numeric_scaler,
        ner=ner,
        device=dev,
    )


def fetch_candles(until: datetime) -> pd.DataFrame:
    with session_scope() as s:
        rows = s.execute(
            select(Candle.dt, Candle.open, Candle.close)
            .where(Candle.dt <= until)
            .order_by(Candle.dt)
        ).all()
    return pd.DataFrame(rows, columns=["dt", "open", "close"])


def fetch_news(start: datetime, end: datetime) -> pd.DataFrame:
    with session_scope() as s:
        rows = s.execute(
            select(News.source, News.source_id, News.ts, News.title, News.body)
            .where(News.ts >= start, News.ts < end)
            .order_by(News.ts)
        ).all()
    return pd.DataFrame(rows, columns=["source", "source_id", "ts", "title", "body"])


def _clean_news_texts(news: pd.DataFrame) -> list[str]:
    titles = news["title"].fillna("").map(normalize_title)
    bodies = news["body"].fillna("")
    cleaned = (titles + " " + bodies).map(clean)
    return [t for t in cleaned.tolist() if t]


def _ner_aggregates(cleaned_texts: list[str], ner: NerContext) -> dict[str, float | int | bool]:
    if not cleaned_texts:
        return {
            "n_news": 0,
            "ner_org_weight_sum_mean": 0.0,
            "ner_org_weight_sum_max": 0.0,
            "ner_n_index_components_sum": 0,
            "ner_has_top_company_any": False,
        }
    weights, counts, has_top = [], [], False
    for text in cleaned_texts:
        _, w, n, top = extract_for_row(
            text, ner.pattern, ner.variant_to_ticker, ner.weights, ner.top_set
        )
        weights.append(w)
        counts.append(n)
        has_top = has_top or top
    weights_arr = np.array(weights, dtype=np.float64)
    return {
        "n_news": len(cleaned_texts),
        "ner_org_weight_sum_mean": float(weights_arr.mean()),
        "ner_org_weight_sum_max": float(weights_arr.max()),
        "ner_n_index_components_sum": int(sum(counts)),
        "ner_has_top_company_any": bool(has_top),
    }


def _candle_row_at(candles: pd.DataFrame, t: datetime) -> pd.Series:
    enriched = add_time_features(add_returns(candles.copy()))
    mask = enriched["dt"] == pd.Timestamp(t)
    if not mask.any():
        raise SystemExit(
            f"Нет свечи на момент {t} в БД (последняя: {enriched['dt'].max()})"
        )
    row = enriched.loc[mask].iloc[0]
    for col in ("ret_1", "ret_60", "ret_120"):
        if pd.isna(row[col]):
            raise SystemExit(f"Нет валидного {col} на {t} (мало истории)")
    return row


def predict_at(
    artifacts: InferenceArtifacts,
    t: datetime,
    now: datetime | None = None,
) -> PredictionResult:
    candles = fetch_candles(until=t)
    candle_row = _candle_row_at(candles, t)

    window_start, window_end, status = news_window(t, now)
    news = fetch_news(window_start, window_end)
    cleaned_texts = _clean_news_texts(news)
    n_total = len(cleaned_texts)
    if n_total > MAX_NEWS_IN_WINDOW:
        cleaned_texts = cleaned_texts[-MAX_NEWS_IN_WINDOW:]
    ner_agg = _ner_aggregates(cleaned_texts, artifacts.ner)

    feature_row = pd.Series({**candle_row.to_dict(), **ner_agg})
    numeric_raw = build_numeric_row(feature_row).reshape(1, -1)
    numeric_scaled = artifacts.scaler.transform(numeric_raw).astype(np.float32)

    if cleaned_texts:
        news_vecs = np.stack([embed_news(text, artifacts.kv) for text in cleaned_texts])
    else:
        news_vecs = np.zeros((0, EMBED_DIM), dtype=np.float32)

    n_news = news_vecs.shape[0]
    max_len = max(1, n_news)
    text_emb = torch.zeros(1, max_len, EMBED_DIM, dtype=torch.float32)
    if n_news:
        text_emb[0, :n_news] = torch.from_numpy(news_vecs)
    lengths = torch.tensor([n_news], dtype=torch.long)
    numeric = torch.from_numpy(numeric_scaled)

    with torch.no_grad():
        text_emb_d = text_emb.to(artifacts.device)
        numeric_d = numeric.to(artifacts.device)
        y_pred = artifacts.model(text_emb_d, lengths, numeric_d).cpu().item()

    return PredictionResult(
        dt=t,
        y_pred=float(y_pred),
        n_news=ner_agg["n_news"],
        n_news_window_total=n_total,
        ret_1=float(candle_row["ret_1"]),
        ret_60=float(candle_row["ret_60"]),
        ret_120=float(candle_row["ret_120"]),
        ner_org_weight_sum_mean=ner_agg["ner_org_weight_sum_mean"],
        ner_has_top_company_any=ner_agg["ner_has_top_company_any"],
        market_status=status,
        window_start=window_start,
        window_end=window_end,
    )


def _latest_valid_dts(k: int = 1) -> list[datetime]:
    candles = fetch_candles(until=now_msk())
    enriched = add_returns(candles.copy())
    eligible = enriched[enriched[["ret_1", "ret_60", "ret_120"]].notna().all(axis=1)]
    if eligible.empty:
        raise SystemExit("Нет свечей с валидным lookback (мало истории)")
    tail = eligible["dt"].sort_values(ascending=False).head(k)
    return [ts.to_pydatetime() for ts in tail]


def _latest_valid_dt() -> datetime:
    return _latest_valid_dts(1)[0]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LSTM inference: прогноз % изменения IMOEX через 60 мин")
    p.add_argument("--dt", type=str, default=None, help="ISO-таймстемп закрытия свечи (МСК)")
    p.add_argument("--latest", action="store_true", help="последняя валидная точка")
    p.add_argument("--w2v", type=Path, default=DEFAULT_W2V)
    p.add_argument("--lstm", type=Path, default=DEFAULT_LSTM)
    p.add_argument("--scaler", type=Path, default=DEFAULT_SCALER)
    p.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    p.add_argument("--device", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dt and not args.latest:
        raise SystemExit("Укажи --dt ISO или --latest")

    if args.latest:
        t = _latest_valid_dt()
    else:
        t = datetime.fromisoformat(args.dt)

    artifacts = load_artifacts(
        w2v_path=args.w2v,
        lstm_path=args.lstm,
        scaler_path=args.scaler,
        tickers_path=args.tickers,
        device=args.device,
    )
    result = predict_at(artifacts, t)

    print(f"dt={result.dt}  y_pred={result.y_pred * 100:+.4f}%  n_news={result.n_news}")
    print(f"market={result.market_status}  window=[{result.window_start} … {result.window_end})")
    print(f"ret_1={result.ret_1:+.4f}  ret_60={result.ret_60:+.4f}  ret_120={result.ret_120:+.4f}")
    print(f"org_weight_mean={result.ner_org_weight_sum_mean:.4f}  has_top={result.ner_has_top_company_any}")


if __name__ == "__main__":
    main()
