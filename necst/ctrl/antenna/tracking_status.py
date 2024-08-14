import time
from typing import Optional

from neclib.data import LinearExtrapolate
from neclib.utils import ConditionChecker, ParameterList
from necst_msgs.msg import CoordMsg, TrackingStatus
from rclpy.node import Node

from ... import config, namespace, topic


class AntennaTrackingStatus(Node):
    NodeName = "tracking_status"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.tracking_checker = ConditionChecker(
            int(0.5 * config.antenna_command_frequency), reset_on_failure=True
        )

        self.cmd = ParameterList.new(2)
        self.enc = ParameterList.new(1)
        self.threshold = config.antenna_pointing_accuracy.to_value("deg").item()
        self.coord_ext = LinearExtrapolate(
            "time", CoordMsg.get_fields_and_field_types().keys()
        )

        self.pub = topic.antenna_tracking.publisher(self)
        topic.antenna_encoder.subscription(self, lambda msg: self.enc.push(msg))
        topic.altaz_cmd.subscription(self, lambda msg: self.cmd.push(msg))
        self.create_timer(
            1 / config.antenna_command_frequency, self.antenna_tracking_status
        )

    def antenna_tracking_status(self) -> None:
        now = time.time()
        if all(not isinstance(x, CoordMsg) for x in self.cmd):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        elif all(not isinstance(x, CoordMsg) for x in self.enc):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        else:
            enc = self.enc[0]
            cmd = self.extrapolate_command_value(enc.time)

            error = ((enc.lon - cmd.lon) ** 2 + (enc.lat - cmd.lat) ** 2) ** 0.5
            ok = self.tracking_checker.check(error < self.threshold)
            msg = TrackingStatus(ok=ok, error=error, time=now)
        self.pub.publish(msg)

    def extrapolate_command_value(self, time: float) -> Optional[CoordMsg]:
        """Perform linear interpolation on encoder reading."""
        *_, newer = self.cmd
        if any(not isinstance(p.time, float) for p in self.cmd) or (
            newer.time < time - 1
        ):
            self.logger.warning("Command value not available.", throttle_duration_sec=5)
            return

        return self.coord_ext(CoordMsg(time=time), self.cmd)
