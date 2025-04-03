import argparse
import hashlib
import re
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import select

from src.storage.db import get_engine
from src.storage.models import News

DEFAULT_OUT = Path("data/processed/news_clean.parquet")
DEFAULT_MIN_LEN = 20

# У 2/3 строк HF-датасета title = "no title" — буквальный плейсхолдер, не заголовок.
TITLE_PLACEHOLDERS = {"no title", "no_title", "без заголовка", "без названия", "untitled"}

URL_RE = re.compile(r"https?://\S+|www\.\S+")
HTML_ENTITY_RE = re.compile(r"&[a-zA-Z#0-9]+;")
NON_WORD_RE = re.compile(r"[^\w\s]", re.UNICODE)
WHITESPACE_RE = re.compile(r"\s+")


def normalize_title(title) -> str:
    if title is None:
        return ""
    title = str(title)
    if not title or title.strip().lower() in TITLE_PLACEHOLDERS:
        return ""
    return title


def strip_html(text) -> str:
    if text is None:
        return ""
    text = str(text)
    if not text:
        return ""
    if "<" not in text and "&" not in text:
        return text
    return BeautifulSoup(text, "lxml").get_text(separator=" ")


def clean(text) -> str:
    if text is None:
        return ""
    text = str(text)
    if not text:
        return ""
    text = strip_html(text)
    text = URL_RE.sub(" ", text)
    text = HTML_ENTITY_RE.sub(" ", text)
    text = text.lower()
    text = NON_WORD_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def _hash16(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def process(out_path: Path, min_len: int) -> pd.DataFrame:
    engine = get_engine()
    print(f"Читаю Postgres ({engine.url.render_as_string(hide_password=True)})…")
    stmt = select(News.source, News.source_id, News.ts, News.title, News.body)
    df = pd.read_sql(stmt, engine)
    n_raw = len(df)
    print(f"Прочитано: {n_raw}")

    df["title"] = df["title"].fillna("").map(normalize_title)
    df["body"] = df["body"].fillna("")
    df["text"] = (df["title"] + " " + df["body"]).map(clean)
    df["text_len"] = df["text"].str.len()

    too_short_mask = df["text_len"] < min_len
    n_too_short = int(too_short_mask.sum())
    df = df.loc[~too_short_mask].copy()

    df["text_hash"] = df["text"].map(_hash16)
    n_before_dedup = len(df)
    df = df.sort_values("ts").drop_duplicates(subset=["text_hash"], keep="first")
    n_deduped = n_before_dedup - len(df)

    df = df.drop(columns=["body", "text_hash"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    print(f"\nЗаписано: {out_path}  ({len(df)} строк)")
    print(f"Исходно:           {n_raw}")
    print(f"Слишком короткие:  {n_too_short}  (<{min_len} символов)")
    print(f"Дубликаты (hash):  {n_deduped}")
    print(f"text_len: mean={df['text_len'].mean():.0f}, "
          f"p50={df['text_len'].median():.0f}, "
          f"p95={df['text_len'].quantile(0.95):.0f}, "
          f"max={df['text_len'].max()}")
    print("\nПо источникам:")
    by_src = df.groupby("source").agg(
        n=("text_len", "size"),
        len_p50=("text_len", "median"),
        len_mean=("text_len", "mean"),
    ).round(0).astype(int).sort_values("n", ascending=False)
    print(by_src.to_string())

    return df


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Очистка корпуса новостей.")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument(
        "--min-len", type=int, default=DEFAULT_MIN_LEN,
        help=f"Минимальная длина cleaned-текста (по умолчанию {DEFAULT_MIN_LEN})",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    process(args.out, args.min_len)


if __name__ == "__main__":
    main()
