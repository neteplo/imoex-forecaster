import argparse
from pathlib import Path

import pandas as pd

DEFAULT_FEATURES = Path("data/processed/candle_features.parquet")
DEFAULT_NEWS_CLEAN = Path("data/processed/news_clean.parquet")
DEFAULT_OUT_DIR = Path("data/processed")

SPLIT_TRAIN = 0.70
SPLIT_VAL = 0.15
DEFAULT_WINDOW_HOURS = 1


def load_inputs(features_path: Path, news_clean_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = pd.read_parquet(features_path)
    features["dt"] = pd.to_datetime(features["dt"])
    news = pd.read_parquet(news_clean_path)
    news["ts_dt"] = pd.to_datetime(news["ts"])
    return features, news


def aggregate_text_per_candle(news: pd.DataFrame, window_hours: int = 1) -> pd.DataFrame:
    if window_hours < 1:
        raise ValueError(f"window_hours должен быть >= 1, получено {window_hours}")
    news = news.copy()
    base_candle = news["ts_dt"].dt.floor("h") + pd.Timedelta(hours=1)
    pieces = []
    for offset in range(window_hours):
        piece = news[["ts_dt", "text"]].copy()
        piece["target_candle"] = base_candle + pd.Timedelta(hours=offset)
        pieces.append(piece)
    expanded = pd.concat(pieces, ignore_index=True)
    agg = (
        expanded.sort_values(["target_candle", "ts_dt"])
        .groupby("target_candle")["text"]
        .agg(list)
        .reset_index()
        .rename(columns={"target_candle": "dt", "text": "text_sequence"})
    )
    return agg


def chronological_split(
    df: pd.DataFrame, train_frac: float = SPLIT_TRAIN, val_frac: float = SPLIT_VAL
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(df)
    i_train = int(n * train_frac)
    i_val = i_train + int(n * val_frac)
    return df.iloc[:i_train], df.iloc[i_train:i_val], df.iloc[i_val:]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Сборка train/val/test для модели.")
    p.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    p.add_argument("--news-clean", type=Path, default=DEFAULT_NEWS_CLEAN)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument(
        "--window-hours", type=int, default=DEFAULT_WINDOW_HOURS,
        help="Размер окна агрегации новостей в часах (новость попадает в окна N свечей)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    features, news = load_inputs(args.features, args.news_clean)
    print(f"Свечей-фичей: {len(features):,}; новостей очищенных: {len(news):,}")
    print(f"Окно агрегации: {args.window_hours}ч")

    news_max_ts = news["ts_dt"].max()
    print(f"News покрытие до: {news_max_ts}")

    text_per_candle = aggregate_text_per_candle(news, window_hours=args.window_hours)
    print(f"Часов с новостями: {len(text_per_candle):,}")

    merged = features.merge(text_per_candle, on="dt", how="left")
    merged["text_sequence"] = merged["text_sequence"].apply(
        lambda v: v if isinstance(v, list) else []
    )

    has_target = merged["target_ret_next"].notna()
    has_lookback = merged[["ret_1", "ret_60", "ret_120"]].notna().all(axis=1)
    in_news_range = merged["dt"] <= news_max_ts
    samples = merged[has_target & has_lookback & in_news_range].copy()
    samples = samples.sort_values("dt").reset_index(drop=True)
    print(
        f"Валидных сэмплов: {len(samples):,} "
        f"(всего {len(merged):,}, отфильтровано "
        f"target={(~has_target).sum()}, lookback={(~has_lookback).sum()}, "
        f"out-of-news={(~in_news_range).sum()})"
    )

    train, val, test = chronological_split(samples)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    for name, part in [("train", train), ("val", val), ("test", test)]:
        path = args.out_dir / f"{name}.parquet"
        part.to_parquet(path, index=False)
        print(
            f"  {name:5s}  {len(part):5d} строк  "
            f"{part.dt.min()} … {part.dt.max()}  →  {path}"
        )

    print("\n=== Сводка ===")
    n = len(samples)
    print(f"  train: {len(train):,} ({len(train) / n:.1%})")
    print(f"  val:   {len(val):,} ({len(val) / n:.1%})")
    print(f"  test:  {len(test):,} ({len(test) / n:.1%})")
    print(f"  Покрытие новостями (text_sequence != []): "
          f"{(samples.text_sequence.map(len) > 0).mean():.1%}")
    print(f"  Среднее новостей в окне: "
          f"{samples.text_sequence.map(len).mean():.1f}")
    print(f"  Максимум новостей в окне: "
          f"{samples.text_sequence.map(len).max()}")

    print("\n=== target_ret_next по сплитам ===")
    for name, part in [("train", train), ("val", val), ("test", test)]:
        t = part["target_ret_next"]
        print(
            f"  {name:5s}  mean={t.mean():+.5f}  std={t.std():.5f}  "
            f"|>0|={(t > 0).mean():.1%}  range=[{t.min():+.4f}, {t.max():+.4f}]"
        )


if __name__ == "__main__":
    main()
