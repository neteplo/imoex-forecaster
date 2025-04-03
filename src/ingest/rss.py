from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from time import struct_time

import feedparser
import requests
import yaml

from src.common.time_utils import MSK
from src.config import settings
from src.preprocessing.text_clean import strip_html
from src.storage.db import init_schema, insert_news, session_scope

DEFAULT_SOURCES = settings.paths.sources
USER_AGENT = settings.http.user_agent
HTTP_TIMEOUT = settings.http.timeout_sec


def load_feeds(sources_path: Path) -> list[dict]:
    with sources_path.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return list(cfg.get("rss") or [])


def _entry_ts(entry) -> datetime | None:
    for attr in ("published_parsed", "updated_parsed"):
        st: struct_time | None = entry.get(attr)
        if st is not None:
            return datetime(*st[:6], tzinfo=timezone.utc)
    for attr in ("published", "updated"):
        raw = entry.get(attr)
        if raw:
            try:
                return parsedate_to_datetime(raw).astimezone(timezone.utc)
            except (TypeError, ValueError):
                continue
    return None


def _entry_source_id(entry) -> str | None:
    return entry.get("id") or entry.get("guid") or entry.get("link")


def _entry_body(entry) -> str:
    for attr in ("summary", "description"):
        raw = entry.get(attr)
        if raw:
            return strip_html(raw).strip()
    content = entry.get("content")
    if content and isinstance(content, list) and content[0].get("value"):
        return strip_html(content[0]["value"]).strip()
    return ""


def fetch_feed(url: str, source_tag: str) -> list[dict]:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    parsed = feedparser.parse(response.content)
    if parsed.bozo and not parsed.entries:
        raise RuntimeError(f"feed {url}: {parsed.bozo_exception!r}")

    source = f"rss:{source_tag}"
    rows: list[dict] = []
    for entry in parsed.entries:
        source_id = _entry_source_id(entry)
        ts_utc = _entry_ts(entry)
        if not source_id or ts_utc is None:
            continue
        ts_msk = ts_utc.astimezone(MSK).replace(tzinfo=None)
        title = (entry.get("title") or "").strip()[:512]
        body = _entry_body(entry) or None
        rows.append({
            "source": source,
            "source_id": str(source_id)[:128],
            "ts": ts_msk,
            "title": title,
            "body": body,
            "tags": None,
        })
    return rows


def run_once(sources_path: Path = DEFAULT_SOURCES) -> dict[str, int]:
    feeds = load_feeds(sources_path)
    if not feeds:
        print("Нет RSS-источников в", sources_path)
        return {}

    init_schema()
    summary: dict[str, int] = {}
    for feed in feeds:
        url = feed["url"]
        tag = feed["source_tag"]
        try:
            rows = fetch_feed(url, tag)
        except Exception as exc:
            print(f"  [{tag:10s}] FAIL: {exc!r}")
            summary[tag] = -1
            continue
        with session_scope() as s:
            inserted = insert_news(s, rows)
        summary[tag] = inserted
        print(f"  [{tag:10s}] получено {len(rows):4d}, вставлено {inserted:4d}")

    return summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Опрос RSS-лент → Postgres")
    p.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    print(f"Источники: {args.sources}\n")
    summary = run_once(args.sources)
    total = sum(v for v in summary.values() if v >= 0)
    print(f"\nИтого вставлено: {total}")


if __name__ == "__main__":
    main()
