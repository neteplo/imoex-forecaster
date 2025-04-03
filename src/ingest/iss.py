import argparse
import time
from datetime import datetime, timedelta

import requests

from src.common.time_utils import MSK
from src.storage.db import (
    candles_overview,
    init_schema,
    latest_candle_dt,
    session_scope,
    upsert_candles,
)

ISS_URL_TEMPLATE = (
    "https://iss.moex.com/iss/engines/stock/markets/index"
    "/securities/{security}/candles.json"
)
HTTP_TIMEOUT = 30
HTTP_RETRIES = 4
HTTP_RETRY_BACKOFF = 2.0
LIVE_BOOTSTRAP_DAYS = 30
LIVE_OVERLAP_DAYS = 2


def fetch_page(
    security: str,
    date_from: str,
    date_till: str,
    interval: int,
    start: int,
) -> list[dict]:
    url = ISS_URL_TEMPLATE.format(security=security)
    params = {
        "interval": interval,
        "from": date_from,
        "till": date_till,
        "start": start,
    }
    last_err: Exception | None = None
    for attempt in range(HTTP_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            payload = response.json()["candles"]
            columns = payload["columns"]
            return [dict(zip(columns, row)) for row in payload["data"]]
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_err = exc
            wait = HTTP_RETRY_BACKOFF * (2 ** attempt)
            print(f"  ISS {type(exc).__name__} (start={start}), retry в {wait:.0f}s")
            time.sleep(wait)
    raise RuntimeError(f"ISS не отвечает после {HTTP_RETRIES} попыток") from last_err


def download(
    security: str,
    date_from: str,
    date_till: str,
    interval: int,
) -> None:
    init_schema()
    start = 0
    downloaded = 0
    while True:
        rows = fetch_page(security, date_from, date_till, interval, start)
        if not rows:
            break
        mapped = [
            {
                "dt": datetime.fromisoformat(row["begin"]),
                "open": row["open"],
                "close": row["close"],
            }
            for row in rows
        ]
        with session_scope() as s:
            upsert_candles(s, mapped)
        downloaded += len(rows)
        start += len(rows)

    with session_scope() as s:
        total, first_dt, last_dt = candles_overview(s)

    print(f"Скачано в этот запуск: {downloaded}")
    print(f"Всего в БД:            {total}")
    print(f"Период в БД:           {first_dt} … {last_dt}")


def live_range() -> tuple[str, str]:
    today = datetime.now(MSK).date()
    with session_scope() as s:
        last_dt = latest_candle_dt(s)
    if last_dt is not None:
        start = last_dt.date() - timedelta(days=LIVE_OVERLAP_DAYS)
    else:
        start = today - timedelta(days=LIVE_BOOTSTRAP_DAYS)
    return start.isoformat(), today.isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Скачать OHLC-свечи индекса с ISS МосБиржи в Postgres.",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Авто-режим: с MAX(dt)-overlap до сегодня; bootstrap на 30 дней если БД пуста",
    )
    parser.add_argument(
        "--from", dest="date_from", default=None,
        help="ГГГГ-ММ-ДД, начало диапазона (МСК)",
    )
    parser.add_argument(
        "--till", dest="date_till", default=None,
        help="ГГГГ-ММ-ДД, конец диапазона включительно (МСК)",
    )
    parser.add_argument(
        "--interval", type=int, default=60,
        help="Интервал свечи в минутах: 1, 10, 60 (час), 24*60=1440 (день)",
    )
    parser.add_argument(
        "--security", default="IMOEX",
        help="Тикер индекса (по умолчанию IMOEX)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.live:
        if args.date_from or args.date_till:
            raise SystemExit("--live несовместим с --from/--till")
        args.date_from, args.date_till = live_range()
        print(f"[live] диапазон: {args.date_from} … {args.date_till}")
    elif not (args.date_from and args.date_till):
        raise SystemExit("Укажи либо --live, либо --from + --till")

    download(
        security=args.security,
        date_from=args.date_from,
        date_till=args.date_till,
        interval=args.interval,
    )


if __name__ == "__main__":
    main()
