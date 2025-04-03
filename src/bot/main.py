from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.error import NetworkError, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes

from src.bot.handlers import (
    cmd_explain,
    cmd_help,
    cmd_history,
    cmd_predict,
    cmd_start,
    cmd_subs,
    cmd_subscribe,
    cmd_unsubscribe,
)
from src.bot.notifier import check_and_notify
from src.config import settings


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    log = logging.getLogger("bot")
    if isinstance(err, (TimedOut, NetworkError)):
        log.warning("network: %s: %s", type(err).__name__, err)
        return
    if isinstance(update, Update) and update.effective_chat:
        log.exception("handler failed for chat %s", update.effective_chat.id, exc_info=err)
    else:
        log.exception("handler failed", exc_info=err)


def build_app(token: str, api_url: str) -> Application:
    app = Application.builder().token(token).build()
    app.bot_data["api_url"] = api_url
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("predict", cmd_predict))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("subs", cmd_subs))
    app.add_handler(CommandHandler("explain", cmd_explain))

    if app.job_queue is None:
        logging.getLogger("bot").warning(
            "JobQueue недоступен (нет python-telegram-bot[job-queue]) — notifier не активен",
        )
    else:
        app.job_queue.run_repeating(
            check_and_notify,
            interval=settings.schedules.notifier_interval_sec,
            first=settings.schedules.notifier_first_delay_sec,
            name="notifier",
        )
    return app


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Telegram-бот IMOEX-forecaster")
    p.add_argument("--api-url", default=settings.api_url)
    p.add_argument("--env", type=Path, default=Path(".env"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.env.exists():
        load_dotenv(args.env)

    token = settings.telegram_bot_token
    if not token:
        raise SystemExit("Не задан TELEGRAM_BOT_TOKEN (положи в .env)")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logging.getLogger("bot").info("api_url=%s", args.api_url)
    build_app(token, args.api_url).run_polling()


if __name__ == "__main__":
    main()
