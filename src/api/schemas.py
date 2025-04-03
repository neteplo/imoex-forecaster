from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class PredictionOut(BaseModel):
    dt: datetime
    y_pred: float
    y_pred_pct: float
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


class HistoryItem(BaseModel):
    dt: datetime
    y_pred: float
    y_pred_pct: float
    n_news: int
    ner_has_top_company_any: bool


class HistoryOut(BaseModel):
    items: list[HistoryItem]


class NewsContributionOut(BaseModel):
    title: str
    contribution: float
    contribution_pct: float
    tickers: list[str]


class CompanyContributionOut(BaseModel):
    ticker: str
    name: str
    n_news: int
    contribution: float
    contribution_pct: float


class ExplainOut(BaseModel):
    dt: datetime
    y_pred: float
    y_pred_pct: float
    y_no_news_pct: float
    market_status: str
    window_start: datetime
    window_end: datetime
    top_news: list[NewsContributionOut]
    top_companies: list[CompanyContributionOut]
