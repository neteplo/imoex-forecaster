# Прогнозирование состояния индекса ММВБ на основе анализа новостных потоков и открытых данных

В данной дипломной работе представлен сервис краткосрочного прогнозирования индекса ММВБ
на основе новостного потока. Модель – LSTM поверх Word2Vec и финансовых
признаков; всё обёрнуто в FastAPI + Telegram-бота и поднимается одной
командой через docker-compose.

Целевая переменная:

```
y_t = (I_{t+60min} − I_t) / I_t
```

– процентное изменение цены закрытия часовой свечи IMOEX через 60 минут.

## Компоненты сервиса

- **Postgres** хранит новости, свечи IMOEX, прогнозы и подписки.
- **Redis** кэширует прогнозы.
- **RabbitMQ** – очередь `predict_tasks` между API и воркером.
- **FastAPI** отдаёт `/predict`, `/history`, `/explain`.
- **Telegram-бот** – пользовательский интерфейс.
- **Ingest** каждые 2 минуты опрашивает RSS, каждые 5 минут – ISS МосБиржи.
- **LSTM** (PyTorch) живёт в `predict-worker` и отрабатывает задачи из очереди.

Все Python-сервисы собираются в один образ – у каждого свой `command:` в
`docker-compose.yml`.

## Запуск

```bash
cp .env.example .env       # внутри только TELEGRAM_BOT_TOKEN заполнять
docker compose build api
docker compose up -d
```

Проверить:

```bash
curl http://127.0.0.1:8765/health
curl "http://127.0.0.1:8765/predict"
curl "http://127.0.0.1:8765/history?k=5"
```

## Источники данных

Новости – RSS-ленты РБК, Финама, Smart-Lab, Коммерсанта и Ведомостей,
настраиваются в `config/sources.yaml`. Данные о движении индекса – ISS МосБиржи
(`iss.moex.com`).

Тренировочный корпус – HuggingFace `Kasymkhan/RussianFinancialNews`
(июнь 2022 → декабрь 2024). Скачивается скриптом `scripts/download_news.py`
и заливается в Postgres.

## Как работает прогноз

1. Запрос приходит в FastAPI: `/predict?dt=...` (или без `dt` – берётся
   последняя валидная часовая свеча).
2. Сначала смотрим в Redis. Если кэш свежий – отдаём из него.
3. Иначе публикуем задачу в RabbitMQ и polling'ом ждём результата.
4. `predict-worker` тянет свечи и новости из Postgres за окно
   `[t-4ч, t)`, прогоняет через Word2Vec → LSTM, пишет результат
   обратно в Redis и в таблицу `predictions`.

`/history?k=N` делает то же самое для N последних часов.

`/explain` оценивает вклад каждой новости через leave-one-out: считаем
базовый прогноз со всеми новостями в окне, затем для каждой новости
выкидываем её и перегоняем LSTM ещё раз. Вклад = `y_base − y_without`.
Дополнительно агрегируем вклады по тикерам компаний (через NER) и
получаем топ влияющих компаний.

## Обучение

Схема обучения:

```
text_clean → ner → features → dataset_builder → train_lstm
```

```bash
export DATABASE_URL=postgresql+psycopg://imoex:imoex@127.0.0.1:5432/imoex

poetry run python -m src.preprocessing.text_clean
poetry run python -m src.preprocessing.ner
poetry run python -m src.preprocessing.features --window-hours 4
poetry run python -m src.preprocessing.dataset_builder --window-hours 4
poetry run python -m src.ml.train_lstm
```

`--window-hours` должен совпадать с `WINDOW_HOURS` в
`src/inference/worker.py`

После переобучения необходимо подтянуть новые артефакты:

```bash
docker compose restart api predict-worker
docker compose exec postgres psql -U imoex -d imoex -c "TRUNCATE TABLE predictions;"
docker compose exec redis redis-cli FLUSHDB
```

Артефакты лежат в `models/`: `word2vec.kv`, `lstm_best.pt`,
`lstm_model.pt`, `lstm_scaler.pkl`.

## API

| End-Point             | Функция                                           |
|-----------------------|---------------------------------------------------|
| `GET /health`         | Liveness                                          |
| `GET /predict?dt=ISO` | Прогноз на `dt` или последний валидный            |
| `GET /history?k=N`    | N прогнозов за последние N часовых свечей         |
| `GET /explain?dt=ISO` | Топ-новости и топ-компании, повлиявшие на прогноз |

## Telegram-бот

| Команда | Функция                                                                      |
|---------|------------------------------------------------------------------------------|
| `/predict` | Последний прогноз                                                            |
| `/explain` | "Объяснение" полученного прогноза                                            |
| `/history k` | Прогнозы за последние k часов (по умолчанию 5)                               |
| `/subscribe N` | Уведомлять при прогнозировании изменения индекса более чем на N% (по модулю) |
| `/unsubscribe` | Отписаться                                                                   |
| `/help` | Справка                                                                      |

## Структура репозитория

```
imoex-forecaster/
├── docker-compose.yml + Dockerfile
├── pyproject.toml + poetry.lock + poetry.toml
├── config/                # sources.yaml, tickers.yaml
├── data/                  # raw/, processed/, runtime/ – все gitignored
├── models/                # gitignored
├── notebooks/             # LSTM_train.ipynb – обучение в Colab
├── scripts/               # download_news.py и др.
└── src/
    ├── config.py          # pydantic-settings
    ├── common/            # time_utils (MSK)
    ├── storage/           # SQLAlchemy
    ├── ingest/            # iss.py, rss.py, scheduler.py
    ├── preprocessing/     # ETL для обучения
    ├── ml/                # LSTM
    ├── inference/         # worker.py, predict_worker.py, queue.py, cache.py
    ├── api/               # FastAPI
    └── bot/               # Telegram-бот
```

## Разработка

```bash
poetry install
poetry run pytest
poetry run python -m src.ingest.rss   # любой модуль как пакет

poetry add <package>                  # новая зависимость – только через Poetry
poetry add --group dev <package>
```

После `poetry add/remove` в коммите должны быть оба файла:
`pyproject.toml` и `poetry.lock`.

Точка истины для конфига – `src/config.py` (pydantic-settings, читает
`.env` и YAML из `config/`). Модули импортируют `settings`, а не
`os.environ`.
