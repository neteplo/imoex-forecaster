from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

MSK = timezone(timedelta(hours=3))

MAIN_SESSION = (time(10, 0), time(18, 50))
EVENING_SESSION = (time(19, 0), time(23, 50))


def now_msk() -> datetime:
    return datetime.now(MSK).replace(tzinfo=None)


def market_is_open(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    t = now.time()
    if MAIN_SESSION[0] <= t < MAIN_SESSION[1]:
        return True
    if EVENING_SESSION[0] <= t < EVENING_SESSION[1]:
        return True
    return False


def align_to_10min(dt: datetime) -> datetime:
    minute = (dt.minute // 10) * 10
    return dt.replace(minute=minute, second=0, microsecond=0)
