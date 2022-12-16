import time

import rclpy
from neclib.devices import WeatherStation
from necst_msgs.msg import WeatherMsg

from ... import config, namespace, topic
from ...core import DeviceNode


class ThermometerReader(DeviceNode):

    NodeName = "thermometer_reader"
    Namespace = namespace.root

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.publisher = topic.weather.publisher(self)

        self.thermo = WeatherStation()
        self.create_timer(1 / config.antenna_command_frequency, self.stream)

    def stream(self):
        msg = WeatherMsg(
            temperature=float(self.thermo.get_temp().to_value("K")),
            pressure=float(self.thermo.get_press().to_value("hPa")),
            humidity=float(self.thermo.get_humid()),
            time=time.time(),
        )
        self.publisher.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerReader()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
