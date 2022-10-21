from rclpy.node import Node

from ... import config, namespace, qos
from necst_msgs.msg import AlertMsg, CoordMsg


class DriveRangeLimitAlert(Node):

    NodeName = "drive_range_limit"
    Namespace = namespace.alert

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.pub_alert = self.create_publisher(AlertMsg, namespace.alert, qos.realtime)
        self.create_subscription(
            CoordMsg, f"{namespace.antenna}/encoder", self.update, qos.realtime
        )

        self.enc_az = self.enc_el = None
        self.warning_limit_az = config.antenna_drive_warning_limit_az.map(
            lambda x: x.to_value("deg")
        )
        self.critical_limit_az = config.antenna_drive_critical_limit_az.map(
            lambda x: x.to_value("deg")
        )
        self.warning_limit_el = config.antenna_drive_warning_limit_el.map(
            lambda x: x.to_value("deg")
        )
        self.critical_limit_el = config.antenna_drive_critical_limit_el.map(
            lambda x: x.to_value("deg")
        )

        self.create_timer(config.alert_interval_sec, self.stream)

    def update(self, msg: CoordMsg) -> None:
        if (msg.frame != "altaz") or (msg.unit != "deg"):
            self.logger.error(f"Invalid encoder reading was captured: {msg}")
            return
        self.enc_az = msg.lon
        self.enc_el = msg.lat

        if (self.enc_az not in self.critical_limit_az) or (
            self.enc_el not in self.critical_limit_el
        ):
            self.stream()

    def stream(self) -> None:
        if self.enc_az is not None:
            msg = AlertMsg(
                threshold=list(self.critical_limit_az),
                actual=self.enc_az,
                warning=self.enc_az not in self.warning_limit_az,
                critical=self.enc_az not in self.critical_limit_az,
                issuer=f"{self.NodeName}/az",
            )
            self.pub_alert.publish(msg)

        if self.enc_el is not None:
            msg = AlertMsg(
                threshold=list(self.critical_limit_el),
                actual=self.enc_el,
                warning=self.enc_el not in self.warning_limit_el,
                critical=self.enc_el not in self.critical_limit_el,
                issuer=f"{self.NodeName}/el",
            )
            self.pub_alert.publish(msg)
