from __future__ import annotations

import json
import logging
from typing import Any

import redis

from src.config import settings

KEY_PREFIX = "predict:"

logger = logging.getLogger("inference.cache")


class PredictionCache:
    def __init__(self, url: str | None = None, ttl_sec: int | None = None) -> None:
        self._url = url or settings.redis_url
        self._ttl = ttl_sec if ttl_sec is not None else settings.redis_ttl_sec
        self._client: redis.Redis | None = None
        try:
            client = redis.Redis.from_url(self._url, decode_responses=True)
            client.ping()
            self._client = client
            logger.info("redis connected: %s (ttl=%ds)", self._url, ttl_sec)
        except Exception as exc:
            logger.warning("redis unavailable (%s): %s — кэш отключён", self._url, exc)

    @property
    def available(self) -> bool:
        return self._client is not None

    @staticmethod
    def key(dt_iso: str) -> str:
        return f"{KEY_PREFIX}{dt_iso}"

    def get(self, dt_iso: str) -> dict[str, Any] | None:
        if self._client is None:
            return None
        try:
            raw = self._client.get(self.key(dt_iso))
        except redis.RedisError as exc:
            logger.warning("redis get failed: %s", exc)
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("redis: невалидный JSON в %s", dt_iso)
            return None

    def set(self, dt_iso: str, payload: dict[str, Any]) -> None:
        self.set_raw(self.key(dt_iso), payload)

    def get_raw(self, key: str) -> dict[str, Any] | None:
        if self._client is None:
            return None
        try:
            raw = self._client.get(key)
        except redis.RedisError as exc:
            logger.warning("redis get failed (%s): %s", key, exc)
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("redis: невалидный JSON в %s", key)
            return None

    def set_raw(self, key: str, payload: dict[str, Any], ttl_sec: int | None = None) -> None:
        if self._client is None:
            return
        ttl = ttl_sec if ttl_sec is not None else self._ttl
        try:
            self._client.setex(key, ttl, json.dumps(payload, default=str))
        except redis.RedisError as exc:
            logger.warning("redis set failed (%s): %s", key, exc)
