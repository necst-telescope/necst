import time

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

        cfg = config.antenna_command
        n_cmd_to_keep = cfg.frequency * (cfg.offset_sec + 0.5)
        self.cmd = ParameterList.new(int(n_cmd_to_keep))
        self.enc = ParameterList.new(1)
        self.threshold = config.antenna_pointing_accuracy.to_value("deg").item()

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
            (enc,) = self.enc
            cmd_msgs = [msg for msg in self.cmd if isinstance(msg, CoordMsg)]
            cmd, *_ = sorted(cmd_msgs, key=lambda msg: abs(msg.time - now))

            error = ((enc.lon - cmd.lon) ** 2 + (enc.lat - cmd.lat) ** 2) ** 0.5
            ok = self.tracking_checker.check(error < self.threshold)
            msg = TrackingStatus(ok=ok, error=error, time=now)
        self.pub.publish(msg)
