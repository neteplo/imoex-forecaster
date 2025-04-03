from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable

import pika

from src.config import settings

QUEUE_NAME = "predict_tasks"
RECONNECT_DELAY_SEC = 5

logger = logging.getLogger("inference.queue")


def _connect() -> pika.BlockingConnection:
    params = pika.URLParameters(settings.rabbitmq_url)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    return pika.BlockingConnection(params)


def _declare_queue(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.queue_declare(queue=QUEUE_NAME, durable=True)


def publish_predict_task(dt_iso: str) -> str:
    request_id = uuid.uuid4().hex
    conn = _connect()
    try:
        channel = conn.channel()
        _declare_queue(channel)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps({"request_id": request_id, "dt": dt_iso}),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        conn.close()
    return request_id


def consume_loop(handler: Callable[[dict], None]) -> None:
    while True:
        try:
            conn = _connect()
            channel = conn.channel()
            _declare_queue(channel)
            channel.basic_qos(prefetch_count=1)

            def on_message(ch, method, properties, body):
                payload = None
                try:
                    payload = json.loads(body)
                    handler(payload)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    logger.exception("worker: ошибка обработки %s", payload)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)
            logger.info("worker: ожидаю задачи из %s", QUEUE_NAME)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning("AMQP connection lost: %s, retry через %ds", exc, RECONNECT_DELAY_SEC)
            time.sleep(RECONNECT_DELAY_SEC)
        except KeyboardInterrupt:
            logger.info("worker: остановка по KeyboardInterrupt")
            return
