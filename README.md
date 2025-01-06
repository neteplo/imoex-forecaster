# Описание проекта

Источником вдохновения является тема моей дипломной работы.

### Установка и запуск

1. **Настройка S3**:

Экспортируйте s3 credentials в переменные окружения

```bash
export AWS_ACCESS_KEY_ID=your-access-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-access-key
```

```bash
export KEY_ID=your-access-key-id
export SECRET_ACCESS_KEY=your-secret-access-key
```

2. **Просмотр логов:**

Используйте TensorBoard для просмотра логов:

```bash
tensorboard --logdir plots
```

3. **Запуск обучения:**

```bash
python imoex_forecaster/models/train.py
```

### Основные компоненты

- Конфигурации: Использование Hydra для управления конфигурациями проекта через
  YAML-файлы.
- DVC: Управление версиями данных и моделей с использованием DVC и удаленного хранилища
  S3.
- Скраппинг данных: Сбор данных с новостных сайтов и Московской биржи c помощью selenium и
  requests
- Предобработка: Очистка и векторизация текстовых данных с использованием TF-IDF.
- Модель: Обучение простой нейронной сети с использованием PyTorch Lightning.
- Логирование: Логирование метрик и гиперпараметров с использованием TensorBoard.

## Структура

```
.
├── conf
│   ├── config.yaml
│   ├── data_loading
│   │   └── data_loading.yaml
│   ├── logs
│   ├── model
│   │   └── model.yaml
│   └── training
│       └── training.yaml
├── data
│   └── raw-data
│       ├── imoex-data-daily
│       ├── imoex-data-daily.dvc
│       ├── news-data-daily
│       └── news-data-daily.dvc
├── imoex_forecaster
│   ├── __init__.py
│   ├── data
│   │   ├── __init__.py
│   │   ├── make_dataset.py
│   │   ├── preprocess_language.py
│   │   ├── scrap_features.py
│   │   └── scrap_target.py
│   ├── models
│   │   ├── __init__.py
│   │   ├── infer.py
│   │   └── train.py
│   └── utils
│       ├── __init__.py
│       ├── datetime_utils.py
│       └── s3_utils.py
├── logs
├── Dockerfile
├── README.md
├── commands.md
├── poetry.lock
└── pyproject.toml
```

## Формулировка задачи

### Введение

В свете недавних заявлений представителей власти Российской Федерации, касающихся развития
фондового рынка, актуальность исследований и проектов в этой области значительно
возрастает. Президент Российской Федерации в своем обращении к Федеральному Собранию
[заявил](https://www.rbc.ru/quote/news/article/65e05bd49a794704415b7f6e) о планах удвоить
капитализацию фондового рынка к 2030 году. В кругу финансовых экспертов бытует мнение, что
этот план предполагает активное вовлечение крупных российских компаний и потенциальное
принуждение к формулированию дивполитики при листинге акций, что может привести к более
широкому распределению активов среди населения.

### Цель проекта

Целью данного проекта является разработка новых инструментов и подходов для анализа и
прогнозирования индекса МосБиржи (IMOEX) на основе данных из открытых источников, включая
новостные ленты и социальные сети. Проект направлен на создание соверменных и эффективных
инструментов управления инвестициями.