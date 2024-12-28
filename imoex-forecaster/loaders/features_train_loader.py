import time
from collections import defaultdict
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options

from utils.date_utils import parse_russian_datetime


def fetch_news_history(from_dt: str, till_dt: str):
    """
    Извлекает историю новостей с сайта РБК за указанный период времени.

    Функция использует Selenium для автоматизации процесса прокрутки страницы
    и BeautifulSoup для извлечения данных из HTML-кода. Данные собираются в
    словарь, где ключи представляют собой категории данных, а значения - списки
    соответствующих элементов.

    Args:
        from_dt (str): Дата начала периода в формате "YYYY-MM-DD".
        till_dt (str): Дата окончания периода в формате "YYYY-MM-DD".

    Returns:
    dict: Словарь, содержащий следующие ключи:
        - "dt": список дат публикации статей.
        - "title": список заголовков статей.
        - "summary": список кратких описаний статей (если отсутствует, значение будет None).
        - "tags": список тегов, связанных с каждой статьей.
    """
    blanc_url = "https://www.rbc.ru/search/?query=&project=rbcnews&dateFrom={from_dt}&dateTo={till_dt}"
    from_date = datetime.strptime(from_dt, "%Y-%m-%d").strftime("%d.%m.%Y")
    till_date = datetime.strptime(till_dt, "%Y-%m-%d").strftime("%d.%m.%Y")
    url = blanc_url.format(from_dt=from_date, till_dt=till_date)

    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    driver.get(url)

    last_height = driver.execute_script("return document.body.scrollHeight")

    SLEEP_SEC = 1.5
    while True:
        page_end = driver.find_element(By.TAG_NAME, "html")
        page_end.send_keys(Keys.END)

        time.sleep(SLEEP_SEC)
        SLEEP_SEC += 0.02

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

        soup = BeautifulSoup(driver.page_source, "lxml")
        articles = soup.find_all("div", {"class": "search-item js-search-item"})

        collector = defaultdict(list)
        for article in articles:
            title = article.find("span", {"class": "search-item__title"})
            #summary = article.find("span", {"class": "search-item__text"})
            category = article.find(
                "span", {"class": "search-item__category"}
            ).get_text(strip=True)

            category_split = category.split(",")
            #tags = category_split[:-2]
            ts_str = "".join(category_split[-2:])
            ts = parse_russian_datetime(ts_str)

            collector["ts"].append(ts)
            collector["title"].append(title.get_text(strip=True))
            #if summary:
            #    collector["summary"].append(summary.get_text(strip=True))
            #else:
            #    collector["summary"].append(None)
            #collector["tags"].append(tags)

    driver.quit()

    return collector