from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
from pathlib import Path

from neclib.devices import Thermometer

env_path = Path(".env")
load_dotenv(dotenv_path=env_path)


class DatabaseCloud:
    RX_list = [
        "thermometer",
    ]

    def __init__(self):
        self.io = Thermometer()

        self.url = os.getenv("INFLUXDB_URL")
        self.token = os.getenv("INFLUXDB_TOKEN")
        self.org = os.getenv("INFLUXDB_ORG")
        self.bucket = os.getenv("INFLUXDB_BUCKET")

        client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = client.write_api(write_options=SYNCHRONOUS)

    def set_points(self, data: dict) -> None:
        """
        point = (
          Point("measurement_name")         # 測定名（例: temperature, voltage）
          .tag("device", "sensor1")         # 任意のタグ
          .field("value", 23.5)             # 数値 or 文字列
          )
        """
        points = []
        for rx_name in self.RX_list:
            for key, value in data.items():
                # TODO "thermometer"の置き換え
                p = Point(rx_name).tag("channel", key).field("value", value)
                points.append(p)

        self.write_api.write(bucket=self.bucket, org=self.org, record=points)
