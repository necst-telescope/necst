import time

import rclpy
from neclib.devices import WeatherStation
from necst_msgs.msg import WeatherMsg

from ... import namespace, topic
from ...core import DeviceNode


class WeatherStationReader(DeviceNode):

    NodeName = "thermometer_reader"
    Namespace = namespace.root

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.publisher = topic.weather.publisher(self)

        self.thermo = WeatherStation()
        self.create_timer(1, self.stream)

    def stream(self):
        msg = WeatherMsg(
            OutTemp=float(self.thermo.get_out_temp().to_value("K")),
            InTemp=float(self.thermo.get_in_temp().to_value("K")),
            press=float(self.thermo.get_pressure().to_value("hPa")),
            OutHum=float(self.thermo.get_out_hum()),
            InHum=float(self.thermo.get_in_hum()),
            WindSpeed=float(self.get_wind_speed().to_value("m/s")),
            WindDir=float(self.get_wind_dir().to_value("deg")),
            RainRate=float(self.get_rain_rate()),
            time=time.time()
        )
        self.publisher.publish(msg)


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
