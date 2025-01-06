import json
import os
from datetime import datetime, timedelta

import fire
import pandas as pd
from tqdm import tqdm

from imoex_forecaster.data.preprocess_language import clear_and_vectorize
from imoex_forecaster.data.scrap_features import scrap_rbc_feed_daily
from imoex_forecaster.data.scrap_target import scrap_imoex
from imoex_forecaster.utils.s3_utils import S3Client


def check_raw_data_local(data_dir: str, all_date_strs) -> list:
    """
    Проверяет наличие сырых данных в локальном окружении за указанный период.

    Args:
        data_dir (str): Директория, содержащая сырые данные.
        all_date_strs (set): Набор всех дат в строковом формате, которые должны быть доступны.

    Returns:
        list: Список дат, для которых отсутствуют сырые данные.
    """
    missing_dates = []
    for date_str in all_date_strs:
        news_path = os.path.join(
            data_dir, f"raw-data/news-data-daily/rbc_{date_str}.json"
        )
        imoex_path = os.path.join(
            data_dir, f"raw-data/imoex-data-daily/candles_{date_str}.json"
        )
        if not os.path.exists(news_path) or not os.path.exists(imoex_path):
            missing_dates.append(date_str)
    return missing_dates


def download_data_local(s3_client, missing_dates):
    """
    Скачивает недостающие данные из S3 в локальное окружение.

    Args:
        s3_client (S3Client): Экземпляр S3Client для взаимодействия с S3.
        missing_dates (list): Список дат, для которых необходимо скачать данные.
    """
    for date_str in tqdm(missing_dates, desc="Downloading data from S3:"):
        news_key = f"raw-data/news-data-daily/rbc_{date_str}.json"
        news_data = s3_client.download_json(news_key)
        local_news_path = os.path.join("data", news_key)
        os.makedirs(os.path.dirname(local_news_path), exist_ok=True)
        with open(local_news_path, "w", encoding="utf-8") as f:
            f.write(news_data)

        imoex_key = f"raw-data/imoex-data-daily/candles_{date_str}.json"
        imoex_data = json.dumps(s3_client.download_json(imoex_key))
        local_imoex_path = os.path.join("data", imoex_key)
        os.makedirs(os.path.dirname(local_imoex_path), exist_ok=True)
        with open(local_imoex_path, "w", encoding="utf-8") as f:
            f.write(imoex_data)


def check_missing_data_s3(s3_client, missing_dates) -> set:
    """
    Проверяет наличие данных на S3 и возвращает набор недостающих дат.

    Args:
        s3_client (S3Client): Экземпляр S3Client для взаимодействия с S3.
        missing_dates (list): Список дат, для которых необходимо проверить наличие данных на S3.

    Returns:
        set: Набор дат, для которых данные отсутствуют на S3.
    """
    available_news_dates = {
        obj.split("_")[-1].split(".")[0]
        for obj in s3_client.list_objects(prefix="raw-data/news-data-daily/")
    }
    available_imoex_dates = {
        obj.split("_")[-1].split(".")[0]
        for obj in s3_client.list_objects(prefix="raw-data/imoex-data-daily/")
    }

    missing_news_dates = set(missing_dates) - available_news_dates
    missing_imoex_dates = set(missing_dates) - available_imoex_dates

    missing_s3_dates = missing_news_dates.union(missing_imoex_dates)
    return missing_s3_dates


def scrape_missing_data(s3_client, missing_s3_dates):
    """
    Выполняет скраппинг и загрузку недостающих данных на S3.

    Args:
        s3_client (S3Client): Экземпляр S3Client для взаимодействия с S3.
        missing_s3_dates (set): Набор дат, для которых необходимо выполнить скраппинг и загрузку данных.
    """
    if missing_s3_dates:
        print(f"Missing raw data for {len(missing_s3_dates)} dates in S3.")
        for date_str in tqdm(missing_s3_dates, desc="Scrapping initiated:"):
            news_data = scrap_rbc_feed_daily(date_str)
            s3_client.upload_json(
                news_data, f"raw-data/news-data-daily/rbc_{date_str}.json"
            )

            imoex_data = scrap_imoex(date_str, date_str)
            s3_client.upload_json(
                imoex_data, f"raw-data/imoex-data-daily/candles_{date_str}.json"
            )
    else:
        print("All dates are available on S3.")


def preprocess_data(data_dir: str) -> pd.DataFrame:
    """
    Создает и возвращает DataFrame с предобработанными и векторизованными данными,
    включая временную метку.

    Args:
        data_dir (str): Директория, содержащая загруженные данные.

    Returns:
        pd.DataFrame: DataFrame с предобработанными и векторизованными данными и временными метками.
    """
    news_data = []
    for file_name in os.listdir(os.path.join(data_dir, "raw-data/news-data-daily")):
        file_path = os.path.join(data_dir, "raw-data/news-data-daily", file_name)
        df = pd.read_json(file_path, encoding="utf-8")
        news_data.append(df)

    concat_df = pd.concat(news_data, ignore_index=True)
    concat_df["ts"] = pd.to_datetime(concat_df["ts"])
    dataset = clear_and_vectorize(concat_df)
    return dataset


def make_dataset(
    from_dt: str, till_dt: str, key_id: str, secret_key: str, data_dir: str
) -> None:
    """
    Создает и сохраняет предобработанный набор данных в указанную директорию.

    Args:
        from_dt (str): Начальная дата в формате 'YYYY-MM-DD'.
        till_dt (str): Конечная дата в формате 'YYYY-MM-DD'.
        key_id (str): Идентификатор ключа доступа к S3.
        secret_key (str): Секретный ключ доступа к S3.
        data_dir (str): Директория, содержащая сырые данные.
    """
    s3_client = S3Client(key_id=key_id, secret_key=secret_key)

    start_date = datetime.strptime(from_dt, "%Y-%m-%d")
    end_date = datetime.strptime(till_dt, "%Y-%m-%d")
    all_dates = {
        start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)
    }
    all_date_strs = {date.strftime("%Y-%m-%d") for date in all_dates}

    missing_dates = check_raw_data_local(data_dir, all_date_strs)
    if missing_dates:
        print(f"Missing raw data for {len(missing_dates)} dates locally.\nS3 check...")
        missing_s3_dates = check_missing_data_s3(s3_client, missing_dates)
        scrape_missing_data(s3_client, missing_s3_dates)
        download_data_local(s3_client, missing_dates)
    else:
        print("Requested raw data is available locally.")

    print("Preprocessing data...")
    dataset = preprocess_data(data_dir)
    output_path = os.path.join(data_dir, "dataset.csv")
    dataset.to_csv(output_path, index=False)
    print(f"Preprocessed data saved to {output_path}")


if __name__ == "__main__":
    fire.Fire(
        lambda from_dt, till_dt: make_dataset(
            from_dt, till_dt, os.environ["KEY_ID"], os.environ["SECRET_KEY"], "./data"
        )
    )
