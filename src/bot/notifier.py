from __future__ import annotations

import logging

import httpx
import telegram
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from src.bot.handlers import _fmt_prediction
from src.storage.db import (
    list_subscriptions,
    mark_notified,
    session_scope,
    was_notified,
)

logger = logging.getLogger("bot.notifier")


async def _fetch_predict(api_url: str) -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.get(f"{api_url}/predict")
            r.raise_for_status()
            return r.json()
    except httpx.HTTPError as exc:
        logger.warning("notifier: /predict failed: %s", exc)
        return None


async def check_and_notify(context: ContextTypes.DEFAULT_TYPE) -> None:
    api_url: str = context.bot_data["api_url"]
    payload = await _fetch_predict(api_url)
    if payload is None:
        return

    abs_pct = abs(payload["y_pred_pct"])
    dt = payload["dt"]

    with session_scope() as session:
        subs = list_subscriptions(session)
        if not subs:
            return
        text = "⚠️ <b>Алерт</b>\n" + _fmt_prediction(payload)
        for sub in subs:
            if abs_pct < sub["threshold_pct"]:
                continue
            if was_notified(session, sub["chat_id"], dt):
                continue
            try:
                await context.bot.send_message(
                    chat_id=sub["chat_id"],
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
                mark_notified(session, sub["chat_id"], dt)
                logger.info(
                    "notifier: alert chat=%d dt=%s |y|=%.3f%% threshold=%.2f%%",
                    sub["chat_id"], dt, abs_pct, sub["threshold_pct"],
                )
            except telegram.error.TelegramError as exc:
                logger.warning("notifier: send failed chat=%d: %s", sub["chat_id"], exc)
