import time
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
            temperature=float(self.thermo.get_temperature().to_value("K")),
            in_temperature=float(self.thermo.get_in_temperature().to_value("K")),
            pressure=float(self.thermo.get_pressure().to_value("hPa")),
            humidity=float(self.thermo.get_humidity()),
            in_humidity=float(self.thermo.get_in_humidity()),
            wind_speed=float(self.thermo.get_wind_speed().to_value("m/s")),
            wind_direction=float(self.thermo.get_wind_direction().to_value("deg")),
            rain_rate=float(self.thermo.get_rain_rate()),
            time=time.time()
        )
        self.publisher.publish(msg)
