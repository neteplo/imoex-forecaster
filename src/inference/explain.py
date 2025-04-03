from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch

from src.inference.worker import (
    MAX_NEWS_IN_WINDOW,
    InferenceArtifacts,
    _candle_row_at,
    _ner_aggregates,
    fetch_candles,
    fetch_news,
    news_window,
)
from src.ml.dataset import EMBED_DIM, build_numeric_row, embed_news
from src.preprocessing.ner import extract_for_row
from src.preprocessing.text_clean import clean, normalize_title


@dataclass
class NewsContribution:
    title: str
    contribution: float
    tickers: list[str]


@dataclass
class CompanyContribution:
    ticker: str
    name: str
    n_news: int
    contribution: float


@dataclass
class ExplainResult:
    dt: datetime
    y_pred: float
    y_no_news: float
    market_status: str
    window_start: datetime
    window_end: datetime
    top_news: list[NewsContribution]
    top_companies: list[CompanyContribution]


def _build_news_window(news: pd.DataFrame) -> list[dict]:
    if news.empty:
        return []
    news = news.copy()
    news["title"] = news["title"].fillna("").astype(str)
    news["body"] = news["body"].fillna("").astype(str)
    items = []
    for _, row in news.iterrows():
        title_norm = normalize_title(row["title"])
        body_raw = row["body"]
        cleaned = clean(title_norm + " " + body_raw)
        if not cleaned:
            continue
        display = (title_norm or body_raw).strip().replace("\n", " ")
        if not display:
            display = cleaned
        items.append({"display": display[:160], "cleaned": cleaned})
    return items


def _predict_batch(
    artifacts: InferenceArtifacts,
    text_embs_per_news: list[np.ndarray],
    numeric_scaled: np.ndarray,
    drop_index: list[int | None],
) -> np.ndarray:
    n_news = len(text_embs_per_news)
    full = np.stack(text_embs_per_news) if n_news else np.zeros((0, EMBED_DIM), dtype=np.float32)

    B = len(drop_index)
    max_len = max(1, n_news)
    text_emb = torch.zeros(B, max_len, EMBED_DIM, dtype=torch.float32)
    lengths = torch.zeros(B, dtype=torch.long)

    for b, drop_i in enumerate(drop_index):
        if n_news == 0:
            continue
        if drop_i is None:
            seq = full
        else:
            seq = np.delete(full, drop_i, axis=0)
        seq_len = seq.shape[0]
        lengths[b] = seq_len
        if seq_len:
            text_emb[b, :seq_len] = torch.from_numpy(seq)

    numeric = torch.from_numpy(np.repeat(numeric_scaled, B, axis=0))

    with torch.no_grad():
        text_emb = text_emb.to(artifacts.device)
        numeric = numeric.to(artifacts.device)
        return artifacts.model(text_emb, lengths, numeric).cpu().numpy()


def explain_at(
    artifacts: InferenceArtifacts,
    t: datetime,
    top_news: int = 5,
    top_companies: int = 5,
    now: datetime | None = None,
) -> ExplainResult:
    candles = fetch_candles(until=t)
    candle_row = _candle_row_at(candles, t)

    window_start, window_end, status = news_window(t, now)
    news = fetch_news(window_start, window_end)
    items = _build_news_window(news)
    if len(items) > MAX_NEWS_IN_WINDOW:
        items = items[-MAX_NEWS_IN_WINDOW:]

    cleaned_texts = [it["cleaned"] for it in items]
    ner_agg = _ner_aggregates(cleaned_texts, artifacts.ner)
    feature_row = {**candle_row.to_dict(), **ner_agg}
    numeric_raw = build_numeric_row(feature_row).reshape(1, -1)
    numeric_scaled = artifacts.scaler.transform(numeric_raw).astype(np.float32)

    text_embs = [embed_news(text, artifacts.kv) for text in cleaned_texts]

    if not items:
        no_news_pred = _predict_batch(artifacts, [], numeric_scaled, [None])[0]
        return ExplainResult(
            dt=t,
            y_pred=float(no_news_pred),
            y_no_news=float(no_news_pred),
            market_status=status,
            window_start=window_start,
            window_end=window_end,
            top_news=[],
            top_companies=[],
        )

    preds = _predict_batch(
        artifacts,
        text_embs,
        numeric_scaled,
        [None, *range(len(items))],
    )
    y_base = float(preds[0])

    no_news_pred = _predict_batch(artifacts, [], numeric_scaled, [None])[0]
    y_no_news = float(no_news_pred)

    contribs_per_news = []
    for i, item in enumerate(items):
        y_without = float(preds[1 + i])
        tickers, *_ = extract_for_row(
            item["cleaned"],
            artifacts.ner.pattern,
            artifacts.ner.variant_to_ticker,
            artifacts.ner.weights,
            artifacts.ner.top_set,
        )
        contribs_per_news.append(
            {"display": item["display"], "contribution": y_base - y_without, "tickers": tickers}
        )

    news_sorted = sorted(contribs_per_news, key=lambda d: abs(d["contribution"]), reverse=True)
    top_news_list = [
        NewsContribution(title=d["display"], contribution=d["contribution"], tickers=d["tickers"])
        for d in news_sorted[:top_news]
    ]

    by_ticker: dict[str, dict] = {}
    for d in contribs_per_news:
        for ticker in d["tickers"]:
            agg = by_ticker.setdefault(ticker, {"contribution": 0.0, "n_news": 0})
            agg["contribution"] += d["contribution"]
            agg["n_news"] += 1

    companies_sorted = sorted(
        by_ticker.items(), key=lambda kv: abs(kv[1]["contribution"]), reverse=True
    )
    top_companies_list = [
        CompanyContribution(
            ticker=ticker,
            name=artifacts.ner.names.get(ticker, ticker),
            n_news=agg["n_news"],
            contribution=agg["contribution"],
        )
        for ticker, agg in companies_sorted[:top_companies]
    ]

    return ExplainResult(
        dt=t,
        y_pred=y_base,
        y_no_news=y_no_news,
        market_status=status,
        window_start=window_start,
        window_end=window_end,
        top_news=top_news_list,
        top_companies=top_companies_list,
    )
