from necst_msgs.msg import AlertMsg, WeatherMsg
from rclpy.node import Node

from ... import config, namespace, topic


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
        self.warning_limit_wind_speed = config.warning_limit_wind_speed.map(
            lambda x: x.to_value("deg")
        )
        self.critical_limit_wind_speed = config.critical_limit_wind_speed.map(
            lambda x: x.to_value("deg")
        )

        self.humidity = None
        self.warning_limit_humidity = config.warning_limit_humidity.map(
            lambda x: x.to_value("deg")
        )
        self.critical_limit_humidity = config.critical_limit_humidity.map(
            lambda x: x.to_value("deg")
        )

        self.rain_rate = None
        self.warning_limit_rain_rate = config.warning_limit_rain_rate.map(
            lambda x: x.to_value("deg")
        )
        self.critical_limit_rain_rate = config.critical_limit_rain_rate.map(
            lambda x: x.to_value("deg")
        )

        self.create_timer(config.alert_interval_sec, self.stream)

    def update(self, msg: WeatherMsg) -> None:
        self.wind_speed = msg.wind_speed
        self.humidity = msg.humidity
        self.rain_rate = msg.rain_rate

        if (self.wind_speed not in self.critical_limit_wind_speed) or (self.humidity not in self.critical_limit_humidity) or (self.rain_rate not in self.critical_limit_rain_rate):
            self.stream()

    def stream(self) -> None:
        if self.wind_speed is not None:
            msg = AlertMsg(
                threshold=list(self.critical_limit_wind_speed),
                actual=self.wind_speed,
                warning=self.wind_speed not in self.warning_limit_wind_speed,
                critical=self.wind_speed not in self.critical_limit_wind_speed,
                target=[namespace.weather],
            )
            self.pub_alert_wind_speed.publish(msg)
        
        if self.humidity is not None:
            msg = AlertMsg(
                threshold=list(self.critical_limit_humidity),
                actual=self.humidity,
                warning=self.humidity not in self.warning_limit_humidity,
                critical=self.humidity not in self.critical_limit_humidity,
                target=[namespace.weather],
            )
            self.pub_alert_humidity.publish(msg)

        if self.rain_rate is not None:
            msg = AlertMsg(
                threshold=list(self.critical_limit_rain_rate),
                actual=self.rain_rate,
                warning=self.rain_rate not in self.warning_limit_rain_rate,
                critical=self.rain_rate not in self.critical_limit_rain_rate,
                target=[namespace.weather],
            )
            self.pub_alert_rain_rate.publish(msg)
