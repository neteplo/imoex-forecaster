from collections import defaultdict
from datetime import datetime
from time import mktime, sleep
from typing import Dict, List

import feedparser
from bs4 import BeautifulSoup


def parse_summary(raw_summary: str) -> str:
    """
    Парсит и очищает HTML-контент из саммари.

    Args:
        raw_summary (str): Исходное HTML-саммари из записи RSS-ленты.

    Returns:
        str: Очищенный текст, извлеченный из саммари.
    """
    soup = BeautifulSoup(raw_summary, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def fetch_news(max_retries: int = 5, delay: int = 5) -> Dict[str, List]:
    """
    Получает последние новости из указанного URL RSS-ленты с логикой повторных попыток.

    Args:
        max_retries (int): Максимальное количество попыток повторного запроса, если получение ленты не удалось. По умолчанию 5.
        delay (int): Задержка в секундах между попытками повторного запроса. По умолчанию 5.

    Returns:
        Dict[str, List]: Словарь, содержащий списки атрибутов новостей, таких как 'id', 'title', 'summary' и 'dt'.
    """
    rss_url = "https://rssexport.rbc.ru/rbcnews/news/99/full.rss"
    collector = defaultdict(list)
    retries = 0

    while retries < max_retries:
        feed = feedparser.parse(rss_url)

        if feed.status == 200:
            for entry in feed.entries:
                # collector["id"].append(entry.id)
                collector["dt"].append(
                    datetime.fromtimestamp(mktime(entry.published_parsed))
                )
                collector["title"].append(entry.title)

                #clean_summary = parse_summary(entry.summary)
                #collector["summary"].append(clean_summary)

            return collector
        else:
            print(
                f"Не удалось получить RSS-ленту. Код: {feed.status}. Повторная попытка через {delay} секунд"
            )
            retries += 1
            sleep(delay)

    print(
        "Достигнуто максимальное количество повторных попыток. Не удалось получить RSS-ленту."
    )
    return collector
