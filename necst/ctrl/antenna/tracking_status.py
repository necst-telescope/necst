import time

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

        self.cmd = ParameterList.new(2, CoordMsg)
        self.enc = ParameterList.new(1, CoordMsg)

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
        if all(not isinstance(p.time, float) for p in self.cmd):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        elif all(not isinstance(p.time, float) for p in self.enc):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        else:
            enc = self.enc[0]
            cmd = self.coord_ext(CoordMsg(time=enc.time), self.cmd)

            error = ((enc.lon - cmd.lon) ** 2 + (enc.lat - cmd.lat) ** 2) ** 0.5
            ok = self.tracking_checker.check(error < self.threshold)
            msg = TrackingStatus(ok=ok, error=error, time=now)
        self.pub.publish(msg)
