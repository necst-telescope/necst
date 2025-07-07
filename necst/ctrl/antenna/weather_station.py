import time
from typing import Dict

import rclpy
from neclib.devices import WeatherStation
from necst_msgs.msg import WeatherMsg
from rclpy.publisher import Publisher

from ... import namespace, topic
from ...core import DeviceNode


class WeatherStationReader(DeviceNode):
    NodeName = "thermometer_reader"
    Namespace = namespace.root

    def __init__(self):
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)

            self.logger = self.get_logger()
            self.io = WeatherStation()

            self.publisher: Dict[str, Publisher] = {}
            self.create_safe_timer(1, self.check_publisher)
            self.create_timer(1, self.stream)

        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def check_publisher(self) -> None:
        for name in self.io.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.lo_signal[name].publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            msg = WeatherMsg(
                temperature=float(self.io[name].get_temperature().to_value("K")),
                pressure=float(self.io[name].get_pressure().to_value("hPa")),
                humidity=float(self.io[name].get_humidity()),
                time=time.time(),
                id=name,
            )
            publisher.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = WeatherStationReader()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
