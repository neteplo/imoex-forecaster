import json
from typing import List

import boto3


class S3Client:
    def __init__(
        self,
        key_id: str,
        secret_key: str,
        bucket_name: str = "imoex-forecaster-data",
        region_name: str = "ru-central-1",
    ):
        """
        Инициализирует S3 клиент для Yandex Object Storage.

        Args:
            bucket_name (str): Имя S3 bucket.
            region_name (str): Имя региона. По умолчанию 'ru-central-1' для Yandex.
        """
        self.bucket_name = bucket_name
        self.session = boto3.session.Session(
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )
        self.s3_client = self.session.client(
            "s3", endpoint_url="https://storage.yandexcloud.net"
        )

    def upload_json(self, data: str, key: str) -> None:
        """
        Загружает JSON объект в S3.

        Args:
            data (Dict[str, Any]): Данные для загрузки.
            key (str): Key (путь/имя файла) для объекта в S3.

        Returns:
            None
        """
        json_data = json.dumps(data, ensure_ascii=False, default=str)
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=json_data)
        except Exception as e:
            print(f"Ошибка при загрузке данных в S3: {e}")

    def download_json(self, key: str) -> str:
        """
        Выгружает JSON объект из S3.

        Args:
            key (str): Key (путь/имя файла) для объекта в S3.

        Returns:
            Dict[str, Any]: Загруженные данные.
        """

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        data = response["Body"].read().decode("utf-8")
        return json.loads(data)

    def list_objects(self, prefix: str = "") -> List:
        """
        Перечисляет объекты в S3 bucket с указанным префиксом.

        Args:
            prefix (str): Префикс для фильтрации объектов. По умолчанию '' (без префикса).

        Returns:
            None
        """

        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=prefix
        )

        if response["KeyCount"] > 0:
            return [obj["Key"] for obj in response["Contents"]]
        else:
            return []
