import json
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, DefaultDict, Dict, List

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from imoex_forecaster.utils.datetime_utils import parse_rbc_datetime


def init_driver(silent: bool = True) -> webdriver.Firefox:
    """
    Инициализирует selenium.webdriver

    Args:
        silent (bool): True для драйвера без отображения UI, False в ином случае

    Returns:
        selenium.webdriver.Firefox
    """
    options = Options()
    if silent:
        options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    return driver


def close_rbc_popup(driver: WebDriver) -> None:
    """
    Закрывает всплывающее окно после открытия сайта РБК

    Args:
        driver (selenium.webdriver): Вебдрайвер selenium

    Returns:
        None
    """
    try:
        WebDriverWait(driver, 15, poll_frequency=1).until(
            EC.presence_of_element_located((By.CLASS_NAME, "popmechanic-close"))
        ).click()
    except Exception as e:
        print(f"An error occurred while closing the popup: {e}")


def scroll_rbc_feed(driver: WebDriver) -> bool:
    """
    Проматывает ленту выдачи поиска новостей РБК до конца

    В связи с особенностями сайта РБК, однократное выполнение команды прокрутки страницы не позволяет достичь её конца.
    Для обхода этой особенности в функцию встроена многократная прокрутка. Количество попыток прокрутки определяется
    параметром MAX_RETRIES. Опытным путём установлено, что пяти попыток достаточно, чтобы прокрутить страницу до конца.

    Args:
        driver (selenium.webdriver): Вебдрайвер selenium

    Returns:
        None
    """
    last_height = driver.execute_script("return document.body.scrollHeight")
    MAX_RETRIES = 5
    retries = 0

    while retries <= MAX_RETRIES:
        time.sleep(0.1)
        driver.find_element(By.TAG_NAME, "html").send_keys(Keys.END)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            retries += 1
        else:
            retries = 0
        last_height = new_height

    return True


def parse_rbc_titles(html: str) -> DefaultDict[str, List[Any]]:
    """
    Извлекает заголовки и дату публикаций новостей из HTML-кода страницы.

    Аргументы:
        html (str): HTML-код страницы.

    Возвращает:
        DefaultDict[str, List[Any]]: Словарь, содержащий списки временных меток
                                     и заголовков статей.
    """
    soup = BeautifulSoup(html, "lxml")
    articles = soup.find_all("div", {"class": "search-item js-search-item"})
    collector = defaultdict(list)

    for article in articles:
        title_element = article.find("span", {"class": "search-item__title"})
        if title_element:
            title = title_element.get_text(strip=True)
            category = article.find(
                "span", {"class": "search-item__category"}
            ).get_text(strip=True)
            category_split = category.split(",")
            ts_str = "".join(category_split[-2:])
            ts = parse_rbc_datetime(ts_str)

            collector["ts"].append(ts)
            collector["title"].append(title)

    return collector


def scrap_rbc_feed_daily(data_conf: Dict[str, Any], date_str: str) -> str:
    """
    Извлекает новости за конкретный день со страницы поиска РБК.

    Args:
        date_str (str): Дата в формате "YYYY-MM-DD".

    Returns:
        DefaultDict[str, List[Any]]: Словарь, содержащий списки временных меток
                                     и заголовков статей.
    """
    formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    base_url = data_conf["rbc_base_url"]
    url = base_url.format(formatted_date, formatted_date)

    driver = init_driver()
    driver.get(url)
    close_rbc_popup(driver)
    scroll_rbc_feed(driver)
    html = driver.page_source
    driver.quit()
    collector = parse_rbc_titles(html)

    return json.dumps(collector, ensure_ascii=False, separators=(",", ":"))


def scrap_rbc_feed(from_dt: str, till_dt: str) -> str:
    """
    Извлекает новости из поиска РБК за каждый день в указанном диапазоне дат.

    Args:
        from_dt (str): Начальная дата в формате "YYYY-MM-DD".
        till_dt (str): Конечная дата в формате "YYYY-MM-DD".

    Returns:
        DefaultDict[str, List[Any]]: Словарь, содержащий списки временных меток
                                     и заголовков статей за весь период.
    """
    all_results = defaultdict(list)
    from_date = datetime.strptime(from_dt, "%Y-%m-%d")
    till_date = datetime.strptime(till_dt, "%Y-%m-%d")

    current_date = from_date
    total_days = (till_date - from_date).days + 1

    for _ in tqdm(range(total_days), desc="RBC news scrapping:"):
        date_str = current_date.strftime("%Y-%m-%d")
        daily_json = scrap_rbc_feed_daily(date_str)
        daily_results = json.loads(daily_json)

        for key, values in daily_results.items():
            all_results[key].extend(values)

        current_date += timedelta(days=1)

    return json.dumps(all_results, ensure_ascii=False, separators=(",", ":"))
