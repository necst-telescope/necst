from necst_msgs.msg import AlertMsg, WeatherMsg, DomeOC, MembraneMsg
from rclpy.node import Node

from ... import config, namespace, topic
import time as pytime


class WeatherConditionAlert(Node):
    NodeName = "weather_condition"
    Namespace = namespace.alert

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.pub_alert_wind_speed = topic.wind_speed_alert.publisher(self)
        self.pub_alert_humidity = topic.humidity_alert.publisher(self)
        self.pub_alert_rain_rate = topic.rain_rate_alert.publisher(self)
        topic.weather.subscription(self, self.update)

        self.wind_speed = None
        self.warning_limit_wind_speed = (
            config.weather_warning_limit_wind_speed.to_value("m/s").item()
        )

        self.critical_limit_wind_speed = (
            config.weather_critical_limit_wind_speed.to_value("m/s").item()
        )

        self.humidity = None
        self.warning_limit_humidity = config.weather_warning_limit_humidity
        self.critical_limit_humidity = config.weather_critical_limit_humidity

        self.rain_rate = None
        self.warning_limit_rain_rate = config.weather_warning_limit_rain_rate
        self.critical_limit_rain_rate = config.weather_critical_limit_rain_rate

        self.create_timer(config.alert_interval_sec, self.stream)

    def update(self, msg: WeatherMsg) -> None:
        self.wind_speed = msg.wind_speed
        self.humidity = msg.humidity
        self.rain_rate = msg.rain_rate

        if (
            (self.wind_speed > self.critical_limit_wind_speed)
            or (self.humidity > self.critical_limit_humidity)
            or (self.rain_rate > self.critical_limit_rain_rate)
        ):
            self.stream()

    def stream(self) -> None:
        if self.wind_speed is not None:
            msg = AlertMsg(
                threshold=[self.critical_limit_wind_speed],
                actual=self.wind_speed,
                warning=abs(self.wind_speed) > self.warning_limit_wind_speed,
                critical=abs(self.wind_speed) > self.critical_limit_wind_speed,
                target=[namespace.antenna, namespace.dome],
            )
            self.pub_alert_wind_speed.publish(msg)

            msg = DomeOC(open=False, time=pytime.time())
            self.publisher["dome_oc"].publish(msg)

            try:
                msg = MembraneMsg(open=False, time=pytime.time())
                self.publisher["membrane"].publish(msg)
            except Exception:
                print("membrane close failed")
                pass

        if self.humidity is not None:
            msg = AlertMsg(
                threshold=[self.critical_limit_humidity],
                actual=self.humidity,
                warning=self.humidity > self.warning_limit_humidity,
                critical=self.humidity > self.critical_limit_humidity,
                target=[namespace.antenna, namespace.dome],
            )
            self.pub_alert_humidity.publish(msg)

            msg = DomeOC(open=False, time=pytime.time())
            self.publisher["dome_oc"].publish(msg)

            try:
                msg = MembraneMsg(open=False, time=pytime.time())
                self.publisher["membrane"].publish(msg)
            except Exception:
                print("membrane close failed")
                pass

        if self.rain_rate is not None:
            msg = AlertMsg(
                threshold=[self.critical_limit_rain_rate],
                actual=self.rain_rate,
                warning=self.rain_rate > self.warning_limit_rain_rate,
                critical=self.rain_rate > self.critical_limit_rain_rate,
                target=[namespace.antenna, namespace.dome],
            )
            self.pub_alert_rain_rate.publish(msg)

            msg = DomeOC(open=False, time=pytime.time())
            self.publisher["dome_oc"].publish(msg)

            try:
                msg = MembraneMsg(open=False, time=pytime.time())
                self.publisher["membrane"].publish(msg)
            except Exception:
                print("membrane close failed")
                pass
