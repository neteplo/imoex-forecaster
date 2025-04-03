import argparse
import re
from pathlib import Path

import pandas as pd
import yaml

DEFAULT_CORPUS = Path("data/processed/news_clean.parquet")
DEFAULT_OUT = Path("data/processed/news_ner.parquet")
DEFAULT_TICKERS = Path("config/tickers.yaml")
DEFAULT_TOP_N = 5


def load_tickers(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)["tickers"]


def build_matcher(tickers: dict) -> tuple[re.Pattern, dict[str, str]]:
    variant_to_ticker: dict[str, str] = {}
    for ticker, info in tickers.items():
        for variant in info["variants"]:
            v = variant.strip().lower()
            if not v:
                continue
            if v in variant_to_ticker and variant_to_ticker[v] != ticker:
                print(f"  warn: variant {v!r} назначен и {variant_to_ticker[v]}, и {ticker} — оставляю первый")
                continue
            variant_to_ticker[v] = ticker

    # Длинные варианты первыми, чтобы «газпром нефть» (SIBN) не схлопывалось в «газпром» (GAZP).
    parts = sorted(variant_to_ticker.keys(), key=len, reverse=True)
    big = r"\b(" + "|".join(re.escape(p) for p in parts) + r")\b"
    return re.compile(big, re.UNICODE), variant_to_ticker


def top_tickers(tickers: dict, n: int) -> set[str]:
    items = sorted(tickers.items(), key=lambda kv: kv[1].get("weight", 0.0), reverse=True)
    return {t for t, _ in items[:n]}


def extract_for_row(
    text: str,
    pattern: re.Pattern,
    variant_to_ticker: dict[str, str],
    weights: dict[str, float],
    top_set: set[str],
) -> tuple[list[str], float, int, bool]:
    if not text:
        return [], 0.0, 0, False
    matches = pattern.findall(text)
    if not matches:
        return [], 0.0, 0, False
    tickers_set = {variant_to_ticker[m] for m in matches}
    weight = sum(weights.get(t, 0.0) for t in tickers_set)
    return (
        sorted(tickers_set),
        round(weight, 5),
        len(tickers_set),
        bool(tickers_set & top_set),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Сопоставление IMOEX-тикеров в новостях.")
    p.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    p.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Читаю tickers: {args.tickers}")
    tickers = load_tickers(args.tickers)
    weights = {t: float(info.get("weight", 0.0)) for t, info in tickers.items()}
    top_set = top_tickers(tickers, args.top_n)
    print(f"Тикеров в словаре: {len(tickers)}, top-{args.top_n} (по весу): {sorted(top_set)}")

    pattern, variant_to_ticker = build_matcher(tickers)
    print(f"Всего variants для матчинга: {len(variant_to_ticker)}")

    print(f"\nЧитаю {args.corpus}…")
    df = pd.read_parquet(args.corpus, columns=["source", "source_id", "ts", "text"])
    print(f"Документов: {len(df)}")

    print("Матчинг…")
    results = df["text"].map(
        lambda t: extract_for_row(t, pattern, variant_to_ticker, weights, top_set)
    )
    df["tickers"] = [r[0] for r in results]
    df["org_weight_sum"] = [r[1] for r in results]
    df["n_index_components"] = [r[2] for r in results]
    df["has_top_company"] = [r[3] for r in results]
    df = df.drop(columns=["text"])

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"\nЗаписано: {args.out}  ({len(df)} строк)")

    with_any = (df["n_index_components"] > 0).sum()
    with_top = df["has_top_company"].sum()
    print(f"С хотя бы одним тикером:        {with_any} ({with_any / len(df):.1%})")
    print(f"С топ-{args.top_n} компонентой:           {with_top} ({with_top / len(df):.1%})")
    print(f"org_weight_sum: mean={df['org_weight_sum'].mean():.3f}, "
          f"max={df['org_weight_sum'].max():.3f}")

    print("\n=== Распределение по числу тикеров на новость ===")
    print(df["n_index_components"].value_counts().sort_index().head(15).to_string())

    print("\n=== Топ-15 тикеров по числу упоминаний ===")
    all_tickers = df["tickers"].explode().dropna()
    top_hits = all_tickers.value_counts().head(15)
    for t, c in top_hits.items():
        print(f"  {t:6s}  {c:6d}  (вес {weights.get(t, 0.0):.3f}, {tickers[t]['name']})")


if __name__ == "__main__":
    main()
