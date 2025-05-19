import argparse
import hashlib
import re
import sqlite3
from datetime import date
from pathlib import Path

from datasets import load_dataset

TIME_RE = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2}))?$")

DEFAULT_DATASET = "Kasymkhan/RussianFinancialNews"
DEFAULT_DB_PATH = Path("data/raw/news.db")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            source_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            title TEXT,
            body TEXT,
            tags TEXT,
            UNIQUE(source, source_id)
        );
        CREATE INDEX IF NOT EXISTS idx_news_ts ON news(ts);
        """
    )


def make_source_id(row: dict) -> str:
    payload = (
        f"{row.get('date', '')}|{row.get('time', '')}|"
        f"{(row.get('source') or '').strip().lower()}|"
        f"{(row.get('title') or '')[:200]}|"
        f"{(row.get('body') or '')[:400]}"
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def to_iso_msk(date_str: str, time_str: str | None) -> str:
    raw = (time_str or "").strip()
    m = TIME_RE.match(raw)
    if not m:
        return f"{date_str}T00:00:00"
    hh, mm, ss = m.group(1), m.group(2), m.group(3) or "00"
    return f"{date_str}T{hh}:{mm}:{ss}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Скачать русскоязычные финновости с HuggingFace в SQLite.",
    )
    parser.add_argument("--from", dest="date_from", required=True, help="ГГГГ-ММ-ДД")
    parser.add_argument(
        "--till", dest="date_till", required=True,
        help="ГГГГ-ММ-ДД, включительно",
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--dataset", default=DEFAULT_DATASET,
        help=f"HF dataset id (по умолчанию {DEFAULT_DATASET})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    date_from = date.fromisoformat(args.date_from)
    date_till = date.fromisoformat(args.date_till)

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db_path)
    ensure_schema(conn)

    print(f"Загружаю датасет {args.dataset} с HuggingFace…")
    ds = load_dataset(args.dataset, split="train")
    print(f"Всего строк в датасете: {len(ds)}")

    inserted = 0
    skipped = 0
    for row in ds:
        d_str = row.get("date")
        if not d_str:
            skipped += 1
            continue
        try:
            d = date.fromisoformat(d_str)
        except ValueError:
            skipped += 1
            continue
        if d < date_from or d > date_till:
            continue

        src_raw = (row.get("source") or "unknown").strip().lower()
        source = f"hf:{src_raw}"
        source_id = make_source_id(row)
        ts = to_iso_msk(d_str, row.get("time"))

        conn.execute(
            "INSERT OR IGNORE INTO news "
            "(source, source_id, ts, title, body, tags) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (source, source_id, ts, row.get("title"), row.get("body"), row.get("tags")),
        )
        inserted += 1
        if inserted % 5000 == 0:
            conn.commit()
            print(f"  …обработано {inserted}")

    conn.commit()
    cur = conn.execute(
        "SELECT COUNT(*), MIN(ts), MAX(ts) FROM news WHERE source LIKE 'hf:%'"
    )
    cnt, min_ts, max_ts = cur.fetchone()
    cur = conn.execute(
        "SELECT source, COUNT(*) FROM news WHERE source LIKE 'hf:%' "
        "GROUP BY source ORDER BY COUNT(*) DESC"
    )
    by_source = cur.fetchall()
    conn.close()

    print(f"\nОбработано из датасета: {inserted}, пропущено (нет даты): {skipped}")
    print(f"В БД HF-новостей: {cnt}; диапазон {min_ts} … {max_ts}")
    print("Разбивка по источникам:")
    for src, c in by_source:
        print(f"  {src:30s}  {c}")


if __name__ == "__main__":
    main()
