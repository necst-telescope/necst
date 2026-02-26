from functools import partial
from typing import List, Dict

from necst_msgs.msg import DeviceReading, WeatherMsg
from neclib.devices import Thermometer, VacuumGauge, Weather_station

from .. import namespace, topic, qos
from ..core import DeviceNode


class DBRecorder(DeviceNode):
    NodeName = "dbrecorder"
    Namespace = namespace.rx

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.get_logger().info("Started DBRecorder…")

        self.devices = {
            "thermometer": Thermometer(),
            "vacuum_gauge": VacuumGauge(),
            "weather_station": Weather_station(),
        }
        self.data = {}
        self.required = []

        for name, io in self.devices.items():
            if name == "thermometer":
                for ch in io.Config.thermometer.channel.keys():
                    if not ch in self.required:
                        self.required.append(ch)
                for ch in self.required:
                    topic_name = f"{self.Namespace}/{name}/{ch}"
                    self.create_subscription(
                        DeviceReading,
                        topic_name,
                        partial(self.callback, ch=ch, name=name),
                        qos.realtime,
                    )
            elif name == "weather_station":
                topic_name = f"{self.Namespace}/{name}"
                self.create_subscription(
                    WeatherMsg,
                    topic_name,
                    partial(self.callback, ch=ch, name=name),
                    qos.realtime,
                )
        if self.check_data(self.data, self.required):
            # self.create_timer(1.0, self._flush_if_ready) DBに記録する処理
            pass

    def callback(self, msg, ch: str, name: str) -> None:
        """
        thermometer )
        data = {"Stage4K1": 4.01, "Shield40K1": 40.1, ...}

        weather_station )
        data = {
            "pressure": 1000 [hPa],
            "temperature": 300 [K],
            "humidty": 0.7,
            "in_temperature": 300 [K],
            "in_humidity": 0.6,
            "wind_speed": 1 [m/s],
            "wind_direction": 5 [deg],
            "rain_rate": unknown [],
        }
        msg.__slots__: ['_pressure', '_temperature', '_humidity', ..., '_time']
        """
        if name == "thermometer":
            self.data[ch] = msg.value
        elif name == "weather_station":
            self.data = {
                field.strip("_"): getattr(msg, field.strip("_"))
                for field in msg.__slots__
                if field.strip("_") != "time"
            }

    def check_data(self, data: Dict, required: List) -> bool:
        return all(key in data for key in required)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = DBRecorder()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
