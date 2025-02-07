# Оф. образ python 3.10
FROM python:3.10-slim

# Отключаем буферизацию и запись pycache для ускорения работы
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Устанавливаем рабочую директорию
WORKDIR /usr/src/app

# Устанавливаем Poetry
RUN pip install --no-cache-dir poetry

# Копируем конфиги poetry
COPY pyproject.toml poetry.lock ./

# Устанавливаем зависимости указанные в конфигах poetry
RUN poetry install

# Задаем поведение контейнера при запуске
ENTRYPOINT ["python"]
CMD ["imoex_forecaster/models/train.py"]
