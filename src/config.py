from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Paths:
    models_dir: Path = Path("models")
    w2v: Path = Path("models/word2vec.kv")
    lstm: Path = Path("models/lstm_best.pt")
    scaler: Path = Path("models/lstm_scaler.pkl")
    tickers: Path = Path("config/tickers.yaml")
    sources: Path = Path("config/sources.yaml")


@dataclass(frozen=True)
class Schedules:
    rss_interval_min: int = 2
    iss_interval_min: int = 5
    notifier_interval_sec: int = 300
    notifier_first_delay_sec: int = 30


@dataclass(frozen=True)
class Http:
    user_agent: str = "Mozilla/5.0 (compatible; imoex-forecaster/0.1; +https://github.com/)"
    timeout_sec: int = 15


@dataclass(frozen=True)
class Settings:
    paths: Paths = field(default_factory=Paths)
    schedules: Schedules = field(default_factory=Schedules)
    http: Http = field(default_factory=Http)
    redis_url: str = "redis://localhost:6379/0"
    redis_ttl_sec: int = 120
    database_url: str = "postgresql+psycopg://imoex:imoex@localhost:5432/imoex"
    rabbitmq_url: str = "amqp://imoex:imoex@localhost:5672/"
    predict_task_timeout_sec: int = 10
    telegram_bot_token: str = ""
    api_url: str = "http://127.0.0.1:8765"


def _from_env(default: Settings) -> Settings:
    return replace(
        default,
        redis_url=os.environ.get("REDIS_URL", default.redis_url),
        redis_ttl_sec=int(os.environ.get("REDIS_TTL_SEC", default.redis_ttl_sec)),
        database_url=os.environ.get("DATABASE_URL", default.database_url),
        rabbitmq_url=os.environ.get("RABBITMQ_URL", default.rabbitmq_url),
        predict_task_timeout_sec=int(
            os.environ.get("PREDICT_TASK_TIMEOUT_SEC", default.predict_task_timeout_sec)
        ),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", default.telegram_bot_token),
        api_url=os.environ.get("IMOEX_API_URL", default.api_url),
    )


settings: Settings = _from_env(Settings())
