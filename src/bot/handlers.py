from __future__ import annotations

from datetime import datetime

import httpx
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from src.storage.db import (
    delete_subscription,
    list_subscriptions,
    session_scope,
    upsert_subscription,
)

HELP_TEXT = (
    "<b>IMOEX-forecaster</b> — прогноз % изменения индекса МосБиржи через 60 мин.\n\n"
    "<b>Команды</b>\n"
    "/predict — последний прогноз\n"
    "/history [k] — прогнозы за последние k часов (default 5)\n"
    "/subscribe N — присылать прогнозы при |Δ| ≥ N%\n"
    "/unsubscribe — отписаться\n"
    "/explain — топ слов из окна по вкладу в прогноз\n"
    "/help — эта справка"
)


def _arrow(y_pct: float) -> str:
    if y_pct > 0.1:
        return "▲"
    if y_pct < -0.1:
        return "▼"
    return "≈"


def _window_hours(p: dict) -> float:
    start = datetime.fromisoformat(p["window_start"])
    end = datetime.fromisoformat(p["window_end"])
    return (end - start).total_seconds() / 3600


def _horizon_label(p: dict) -> str:
    if p.get("market_status") == "open":
        return "→ +1ч"
    return "→ к открытию"


def _fmt_prediction(p: dict) -> str:
    arrow = _arrow(p["y_pred_pct"])
    dt = datetime.fromisoformat(p["dt"]).strftime("%Y-%m-%d %H:%M")
    line1 = f"{arrow} <b>{p['y_pred_pct']:+.3f}%</b>  (от {dt} МСК {_horizon_label(p)})"
    win = _window_hours(p)
    status = "биржа открыта" if p.get("market_status") == "open" else "биржа закрыта"
    line2 = (
        f"{status}, окно {win:.1f}ч, "
        f"ret_1={p['ret_1']:+.4f}, ret_60={p['ret_60']:+.4f}"
    )
    return f"{line1}\n<i>{line2}</i>"


def _fmt_history_item(p: dict) -> str:
    arrow = _arrow(p["y_pred_pct"])
    dt = datetime.fromisoformat(p["dt"]).strftime("%Y-%m-%d %H:%M")
    return f"{arrow} <b>{p['y_pred_pct']:+.3f}%</b>  ({dt}, новостей: {p['n_news']})"


async def _fetch_predict(api_url: str) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{api_url}/predict")
        r.raise_for_status()
        return r.json()


async def _fetch_history(api_url: str, k: int) -> list[dict]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{api_url}/history", params={"k": k})
        r.raise_for_status()
        return r.json()["items"]


async def _fetch_explain(api_url: str) -> dict:
    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.get(f"{api_url}/explain")
        r.raise_for_status()
        return r.json()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


async def cmd_predict(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    api_url: str = context.bot_data["api_url"]
    try:
        payload = await _fetch_predict(api_url)
    except httpx.HTTPError as exc:
        await update.message.reply_text(f"Не удалось получить прогноз: {exc}")
        return
    await update.message.reply_text(_fmt_prediction(payload), parse_mode=ParseMode.HTML)


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    api_url: str = context.bot_data["api_url"]
    k = 5
    if context.args:
        try:
            k = max(1, min(50, int(context.args[0])))
        except ValueError:
            await update.message.reply_text("Использование: /history [k]")
            return
    try:
        items = await _fetch_history(api_url, k)
    except httpx.HTTPError as exc:
        await update.message.reply_text(f"Не удалось получить историю: {exc}")
        return
    if not items:
        await update.message.reply_text("История пуста — сделай /predict хотя бы раз.")
        return
    lines = [_fmt_history_item(p) for p in items]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Использование: /subscribe N — порог в % (например, 0.5)")
        return
    try:
        threshold = float(context.args[0])
    except ValueError:
        await update.message.reply_text("N должно быть числом, например 0.5")
        return
    if threshold <= 0:
        await update.message.reply_text("Порог должен быть > 0")
        return

    chat_id = update.effective_chat.id
    with session_scope() as session:
        upsert_subscription(session, chat_id, threshold)
    await update.message.reply_text(
        f"Подписка сохранена: уведомления при |Δ| ≥ {threshold:.2f}%"
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    with session_scope() as session:
        removed = delete_subscription(session, chat_id)
    if removed:
        await update.message.reply_text("Подписка снята.")
    else:
        await update.message.reply_text("Активной подписки не было.")


async def cmd_subs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    with session_scope() as session:
        subs = list_subscriptions(session)
    if not subs:
        await update.message.reply_text("Подписок пока нет.")
        return
    lines = [f"{s['chat_id']}: ≥ {s['threshold_pct']:.2f}%" for s in subs]
    await update.message.reply_text("Подписчики:\n" + "\n".join(lines))


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def cmd_explain(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    api_url: str = context.bot_data["api_url"]
    await update.message.reply_text("Считаю вклад каждой новости…")
    try:
        payload = await _fetch_explain(api_url)
    except httpx.HTTPError as exc:
        await update.message.reply_text(f"Не удалось получить explain: {exc}")
        return

    dt = datetime.fromisoformat(payload["dt"]).strftime("%Y-%m-%d %H:%M")
    arrow = _arrow(payload["y_pred_pct"])
    delta_news = payload["y_pred_pct"] - payload["y_no_news_pct"]
    win = _window_hours(payload)
    status = "биржа открыта" if payload.get("market_status") == "open" else "биржа закрыта"

    lines = [
        f"{arrow} <b>{payload['y_pred_pct']:+.3f}%</b>  (от {dt} МСК {_horizon_label(payload)})",
        f"<i>{status}, окно {win:.1f}ч</i>",
        f"<i>без новостей: {payload['y_no_news_pct']:+.3f}%, вклад новостей: {delta_news:+.3f}%</i>",
    ]

    if payload["top_companies"]:
        lines.append("\n<b>Компании</b>")
        for c in payload["top_companies"]:
            a = _arrow(c["contribution_pct"])
            lines.append(
                f"  {a} {_escape(c['name'])} <code>{c['ticker']}</code> "
                f"({c['n_news']}) {c['contribution_pct']:+.3f}%"
            )

    if payload["top_news"]:
        lines.append("\n<b>Новости</b>")
        for n in payload["top_news"]:
            a = _arrow(n["contribution_pct"])
            tickers = f" [{', '.join(n['tickers'])}]" if n["tickers"] else ""
            lines.append(
                f"  {a} {n['contribution_pct']:+.3f}%{tickers}\n"
                f"     {_escape(n['title'])}"
            )
    else:
        lines.append("\n<i>В окне нет распознанных новостей.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
