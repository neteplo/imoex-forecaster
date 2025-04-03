import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_DB = Path("data/raw/imoex.db")
DEFAULT_NER = Path("data/processed/news_ner.parquet")
DEFAULT_OUT = Path("data/processed/candle_features.parquet")

LAGS = (1, 60, 120)
DEFAULT_WINDOW_HOURS = 1
# target невалиден если gap к следующей свече > tol (overnight, выходные)
TARGET_GAP_HOURS_TOL = 1.2


def load_candles(db_path: Path) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(
            "SELECT dt, open, close FROM candles ORDER BY dt",
            conn,
            parse_dates=["dt"],
        )
    return df.sort_values("dt").reset_index(drop=True)


def add_returns(df: pd.DataFrame, lags=LAGS) -> pd.DataFrame:
    for k in lags:
        df[f"ret_{k}"] = (df["close"] - df["close"].shift(k)) / df["close"].shift(k)
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df["hour_of_day"] = df["dt"].dt.hour.astype("int16")
    df["day_of_week"] = df["dt"].dt.dayofweek.astype("int16")
    return df


def add_target(df: pd.DataFrame, tol_h: float = TARGET_GAP_HOURS_TOL) -> pd.DataFrame:
    next_dt = df["dt"].shift(-1)
    next_close = df["close"].shift(-1)
    gap_h = (next_dt - df["dt"]) / pd.Timedelta(hours=1)
    valid = (gap_h >= 0.9) & (gap_h <= tol_h)
    target = (next_close - df["close"]) / df["close"]
    df["target_ret_next"] = np.where(valid, target, np.nan)
    return df


def aggregate_ner_per_window(
    candles: pd.DataFrame,
    ner: pd.DataFrame,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> pd.DataFrame:
    # Новость в 10:35 относится к свече 11:00 (окно [10:00, 11:00) её включает).
    # При window_hours=N — и к свечам 12:00, …, 11:00+(N-1)ч.
    if window_hours < 1:
        raise ValueError(f"window_hours должен быть >= 1, получено {window_hours}")

    ner = ner.copy()
    ner["ts_dt"] = pd.to_datetime(ner["ts"])
    base_candle = ner["ts_dt"].dt.floor("h") + pd.Timedelta(hours=1)
    pieces = []
    for offset in range(window_hours):
        piece = ner.copy()
        piece["target_candle"] = base_candle + pd.Timedelta(hours=offset)
        pieces.append(piece)
    expanded = pd.concat(pieces, ignore_index=True)

    agg = (
        expanded.groupby("target_candle")
        .agg(
            n_news=("source_id", "size"),
            ner_org_weight_sum_mean=("org_weight_sum", "mean"),
            ner_org_weight_sum_max=("org_weight_sum", "max"),
            ner_n_index_components_sum=("n_index_components", "sum"),
            ner_has_top_company_any=("has_top_company", "any"),
        )
        .reset_index()
        .rename(columns={"target_candle": "dt"})
    )

    merged = candles.merge(agg, on="dt", how="left")
    merged["n_news"] = merged["n_news"].fillna(0).astype("int32")
    merged["ner_org_weight_sum_mean"] = merged["ner_org_weight_sum_mean"].fillna(0.0)
    merged["ner_org_weight_sum_max"] = merged["ner_org_weight_sum_max"].fillna(0.0)
    merged["ner_n_index_components_sum"] = (
        merged["ner_n_index_components_sum"].fillna(0).astype("int32")
    )
    merged["ner_has_top_company_any"] = (
        merged["ner_has_top_company_any"].fillna(False).astype(bool)
    )
    return merged


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Признаки на свечу IMOEX (финансовые + NER-агрегаты + таргет)."
    )
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--ner", type=Path, default=DEFAULT_NER)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument(
        "--window-hours", type=int, default=DEFAULT_WINDOW_HOURS,
        help="Окно агрегации NER-фич в часах",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    print(f"Свечи:  {args.db}")
    print(f"NER:    {args.ner}")
    print(f"Out:    {args.out}\n")

    candles = load_candles(args.db)
    print(f"Свечей: {len(candles):,}; диапазон: {candles.dt.min()} … {candles.dt.max()}")

    candles = add_returns(candles)
    candles = add_time_features(candles)
    candles = add_target(candles)

    ner = pd.read_parquet(args.ner)
    print(f"NER записей: {len(ner):,}")

    print(f"Окно агрегации: {args.window_hours}ч")
    merged = aggregate_ner_per_window(candles, ner, window_hours=args.window_hours)

    valid_target = merged["target_ret_next"].notna()
    valid_lookback = merged[[f"ret_{k}" for k in LAGS]].notna().all(axis=1)
    valid = valid_target & valid_lookback
    print(
        f"\nВалидных строк (есть таргет и все лаги): "
        f"{int(valid.sum()):,} / {len(merged):,}"
    )
    print(f"Из них с новостями в окне: {int((valid & (merged.n_news > 0)).sum()):,}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(args.out, index=False)
    print(f"\nЗаписано: {args.out}, {len(merged):,} строк, {len(merged.columns)} колонок")
    print("Колонки:", list(merged.columns))

    print("\n=== n_news per свечу ===")
    print(merged["n_news"].describe().round(2).to_string())

    print("\n=== target_ret_next ===")
    print(merged["target_ret_next"].describe().round(5).to_string())


if __name__ == "__main__":
    main()
