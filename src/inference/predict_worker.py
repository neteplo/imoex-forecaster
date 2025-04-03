from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import Any

from src.inference.cache import KEY_PREFIX, PredictionCache
from src.inference.queue import consume_loop
from src.inference.worker import (
    PredictionResult,
    load_artifacts,
    predict_at,
)

logger = logging.getLogger("inference.predict_worker")

RESULT_PREFIX = "predict:result:"


def _to_payload(result: PredictionResult) -> dict[str, Any]:
    return {
        "dt": result.dt.isoformat(sep="T"),
        "y_pred": result.y_pred,
        "y_pred_pct": result.y_pred * 100,
        "n_news": result.n_news,
        "n_news_window_total": result.n_news_window_total,
        "ret_1": result.ret_1,
        "ret_60": result.ret_60,
        "ret_120": result.ret_120,
        "ner_org_weight_sum_mean": result.ner_org_weight_sum_mean,
        "ner_has_top_company_any": result.ner_has_top_company_any,
        "market_status": result.market_status,
        "window_start": result.window_start.isoformat(),
        "window_end": result.window_end.isoformat(),
    }


def _make_handler(artifacts, cache: PredictionCache):
    def handler(message: dict) -> None:
        request_id = message["request_id"]
        dt = datetime.fromisoformat(message["dt"])
        logger.info("predict req=%s dt=%s", request_id, dt)
        try:
            result = predict_at(artifacts, dt)
        except SystemExit as exc:
            logger.warning("predict req=%s failed: %s", request_id, exc)
            cache.set_raw(f"{RESULT_PREFIX}{request_id}", {"error": str(exc)})
            return

        payload = _to_payload(result)
        cache.set_raw(f"{RESULT_PREFIX}{request_id}", payload)
        cache.set(dt.isoformat(), payload)
        logger.info(
            "predict req=%s y=%.4f%% n_news=%d", request_id, result.y_pred * 100, result.n_news
        )

    return handler


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consumer для predict_tasks из RabbitMQ")
    p.add_argument("--device", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger.info("predict-worker: загружаю артефакты")
    artifacts = load_artifacts(device=args.device)
    cache = PredictionCache()
    if not cache.available:
        raise SystemExit("Redis недоступен — predict-worker не может писать результаты")
    handler = _make_handler(artifacts, cache)
    consume_loop(handler)


if __name__ == "__main__":
    main()
