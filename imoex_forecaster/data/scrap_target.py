import json

import fire
import requests


def scrap_imoex(from_dt: str, till_dt: str) -> str:
    """
    Получает данные индекса IMOEX с Московской биржи за указанный диапазон дат и сохраняет их в CSV-файл.

    Args:
        from_dt (str): Начальная дата для получения данных в формате 'YYYY-MM-DD'.
        till_dt (str): Конечная дата для получения данных в формате 'YYYY-MM-DD'.

    Exception:
        HTTPError: Если HTTP-запрос к API MOEX завершился неудачно.
    """
    url = "https://iss.moex.com/iss/engines/stock/markets/index/securities/IMOEX/candles.json"

    params = {
        "interval": 24,
        "from": from_dt,
        "till": till_dt,
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    data_rows = data["candles"]["data"]
    columns = data["candles"]["columns"]

    # Find the indices for 'close' and 'begin'
    close_index = columns.index("close")
    open_index = columns.index("open")
    dt_index = columns.index("begin")

    # Transform data into the desired format
    collector = {"dt": [], "imoex_open_val": [], "imoex_close_val": []}

    for row in data_rows:
        collector["dt"].append(row[dt_index])
        collector["imoex_open_val"].append(row[open_index])
        collector["imoex_close_val"].append(row[close_index])

    return json.dumps(collector, ensure_ascii=False, separators=(",", ":"))


if __name__ == "__main__":
    fire.Fire(scrap_imoex)