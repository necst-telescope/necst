from necst_msgs.msg import AlertMsg, TimedAzElFloat64
from rclpy.node import Node

from ... import config, namespace, topic


class AntennaSpeedAlert(Node):

    NodeName = "antenna_speed"
    Namespace = namespace.alert

    Critical = 1.3

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        topic.antenna_speed_cmd.subscription(self, self.update)
        self.pub_alert_az = topic.drive_speed_alert_az.publisher(self)
        self.pub_alert_el = topic.drive_speed_alert_el.publisher(self)

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
                target=[namespace.antenna],
            )
            self.pub_alert_az.publish(msg)

        if self.speed_el is not None:
            msg = AlertMsg(
                threshold=[self.max_speed_el],
                actual=self.speed_el,
                warning=abs(self.speed_el) > self.max_speed_el,
                critical=abs(self.speed_el) > self.Critical * self.max_speed_el,
                target=[namespace.antenna],
            )
            self.pub_alert_el.publish(msg)
