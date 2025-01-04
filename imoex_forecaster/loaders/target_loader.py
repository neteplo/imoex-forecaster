from typing import NoReturn

import fire
import pandas as pd
import requests


def fetch_imoex(from_dt: str, till_dt: str) -> NoReturn:
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
    candles = data["candles"]["data"]
    columns = data["candles"]["columns"]

    df = pd.DataFrame(candles, columns=columns)

    df["dt"] = pd.to_datetime(df["begin"])

    return df[["dt", "close"]].rename(columns={"close": "imoex_close_val"})


if __name__ == "__main__":
    fire.Fire(fetch_imoex)
