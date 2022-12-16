import time

import rclpy
from neclib.devices import WeatherStation
from necst_msgs.msg import TimedFloat64

from ... import namespace, topic
from ...core import DeviceNode


class WeatherStationReader(DeviceNode):

    NodeName = "thermometer_reader"
    Namespace = namespace.root

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.publisher = {
            "temperature": topic.weather_temperature.publisher(self),
            "humidity": topic.weather_humidity.publisher(self),
            "pressure": topic.weather_pressure.publisher(self),
        }

        self.thermo = WeatherStation()
        self.create_timer(1, self.stream)

    def stream(self):

        msg_temp = TimedFloat64(
            data=float(self.thermo.get_temp().value), time=time.time()
        )
        msg_hum = TimedFloat64(data=float(self.thermo.get_humid()), time=time.time())
        msg_press = TimedFloat64(
            data=float(self.thermo.get_press().value), time=time.time()
        )

        self.publisher["temperature"].publish(msg_temp)
        self.publisher["humidity"].publish(msg_hum)
        self.publisher["pressure"].publish(msg_press)


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
