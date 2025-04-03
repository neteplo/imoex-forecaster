from __future__ import annotations

import argparse
import logging
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

from src.config import settings
from src.ingest.iss import download as iss_download
from src.ingest.iss import live_range as iss_live_range
from src.ingest.rss import run_once as rss_run_once
from src.storage.db import init_schema

logger = logging.getLogger("ingest.scheduler")


def job_rss(sources: Path) -> None:
    try:
        summary = rss_run_once(sources)
        total = sum(v for v in summary.values() if v >= 0)
        logger.info("rss: вставлено %d (по источникам: %s)", total, summary)
    except Exception:
        logger.exception("rss: упало")


def job_iss() -> None:
    try:
        date_from, date_till = iss_live_range()
        iss_download(
            security="IMOEX",
            date_from=date_from,
            date_till=date_till,
            interval=60,
        )
    except Exception:
        logger.exception("iss: упало")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Live-ingest: RSS + ISS по расписанию")
    p.add_argument("--sources", type=Path, default=settings.paths.sources)
    p.add_argument("--rss-interval-min", type=int, default=settings.schedules.rss_interval_min)
    p.add_argument("--iss-interval-min", type=int, default=settings.schedules.iss_interval_min)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    init_schema()
    logger.info(
        "Старт: RSS каждые %d мин, ISS каждые %d мин",
        args.rss_interval_min,
        args.iss_interval_min,
    )

    logger.info("Initial run: RSS + ISS")
    job_rss(args.sources)
    job_iss()

    scheduler = BlockingScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        job_rss,
        "interval",
        minutes=args.rss_interval_min,
        args=(args.sources,),
        id="rss_poll",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        job_iss,
        "interval",
        minutes=args.iss_interval_min,
        id="iss_poll",
        max_instances=1,
        coalesce=True,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка")


if __name__ == "__main__":
    main()
