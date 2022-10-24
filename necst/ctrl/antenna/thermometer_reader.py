import time

import rclpy
from rclpy.node import Node

from neclib.devices import WeatherStation
from necst import namespace, qos, config
from necst_msgs.msg import TimeFloat64


class ThermometerReader(Node):

    NodeName = "thermometer_reader"
    Namespace = namespace.antenna

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.publisher = {
            "temperature": self.create_publisher(
                TimeFloat64, "temperature", qos.realtime
            ),
            "humidity": self.create_publisher(TimeFloat64, "humidity", qos.realtime),
            "pressure": self.create_publisher(TimeFloat64, "pressure", qos.realtime),
        }

        self.thermo = weather_station()
        self.create_timer(1 / config.antenna_command_frequency, self.stream)

    def stream(self):

        msg_temp = TimeFloat64(data=float(self.thermo.get_temp.value), time=time.time())
        msg_hum = TimeFloat64(data=float(self.thermo.get_humid), time=time.time())
        msg_press = TimeFloat64(
            data=float(self.thermo.get_press.value), time=time.time()
        )

        self.publisher["temperature"].publish(msg_temp)
        self.publisher["humidity"].publish(msg_hum)
        self.publisher["pressure"].publish(msg_press)


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
