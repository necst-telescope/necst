from rclpy.node import Node

from ... import config, namespace, qos
from necst_msgs.msg import AlertMsg, TimedAzElFloat64


class AntennaSpeedAlert(Node):

    NodeName = "antenna_speed"
    Namespace = namespace.alert

    Critical = 1.3

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.create_subscription(
            TimedAzElFloat64, f"{namespace.antenna}/speed", self.update, qos.realtime
        )
        self.pub_alert_az = self.create_publisher(
            AlertMsg, f"{namespace.alert}/antenna_speed/az", qos.realtime
        )
        self.pub_alert_el = self.create_publisher(
            AlertMsg, f"{namespace.alert}/antenna_speed/el", qos.realtime
        )

        self.speed_az = self.speed_el = None
        self.max_speed_az = config.antenna_max_speed_az.to_value("deg/s").item()
        self.max_speed_el = config.antenna_max_speed_el.to_value("deg/s").item()

        self.create_timer(config.alert_interval_sec, self.stream)

    def update(self, msg: TimedAzElFloat64) -> None:
        self.speed_az = msg.az
        self.speed_el = msg.el

        if (abs(self.speed_az) > self.Critical * self.max_speed_az) or (
            abs(self.speed_el) > self.Critical * self.max_speed_el
        ):
            self.stream()

    def stream(self) -> None:
        if self.speed_az is not None:
            msg = AlertMsg(
                threshold=[self.max_speed_az],
                actual=self.speed_az,
                warning=abs(self.speed_az) > self.max_speed_az,
                critical=abs(self.speed_az) > self.Critical * self.max_speed_az,
                issuer=f"{self.NodeName}/az",
            )
            self.pub_alert_az.publish(msg)

        if self.speed_el is not None:
            msg = AlertMsg(
                threshold=[self.max_speed_el],
                actual=self.speed_el,
                warning=abs(self.speed_el) > self.max_speed_el,
                critical=abs(self.speed_el) > self.Critical * self.max_speed_el,
                issuer=f"{self.NodeName}/el",
            )
            self.pub_alert_el.publish(msg)
