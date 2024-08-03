import time
import numpy as np

from neclib.utils import ConditionChecker, ParameterList
from necst_msgs.msg import CoordMsg, TrackingStatus
from rclpy.node import Node

from ... import config, namespace, topic


class DomeTrackingStatus(Node):
    NodeName = "dome_tracking"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.tracking_checker = ConditionChecker(
            int(0.5 * config.dome_command_frequency), reset_on_failure=True
        )
        self.sync_checker = ConditionChecker(
            int(0.5 * config.dome_command_frequency), reset_on_failure=True
        )

        cfg = config.dome_command
        n_cmd_to_keep = cfg.frequency * (cfg.offset_sec + 0.5)
        self.cmd = ParameterList.new(int(n_cmd_to_keep))
        self.antenna_cmd = ParameterList.new(1)
        self.antenna_enc = ParameterList.new(1)
        self.dome_enc = ParameterList.new(1)
        self.dome_cmd = ParameterList.new(1)
        self.threshold = config.dome_sync_accuracy.to_value("deg")
        self.limit = config.dome_sync_limit.to_value("deg")

        self.pub = topic.dome_tracking.publisher(self)
        self.error_pub = topic.dome_sync_error.publisher(self)

        topic.altaz_cmd.subscription(self, lambda msg: self.antenna_cmd.push(msg))
        topic.antenna_encoder.subscription(self, lambda msg: self.antenna_enc.push(msg))
        topic.dome_encoder.subscription(self, lambda msg: self.dome_enc.push(msg))
        topic.dome_altaz_cmd.subscription(self, lambda msg: self.dome_cmd.push(msg))
        self.create_timer(1 / config.dome_command_frequency, self.dome_status)
        self.create_timer(1 / config.dome_command_frequency, self.dome_sync_status)

    def dome_status(self) -> None:
        now = time.time()
        if all(not isinstance(x, CoordMsg) for x in self.dome_enc):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        elif all(not isinstance(x, CoordMsg) for x in self.dome_cmd):
            self.tracking_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        else:
            error = np.abs(self.dome_cmd[0].lon - self.dome_enc[0].lon)
            ok = self.tracking_checker.check(error < self.threshold)
            msg = TrackingStatus(ok=ok, error=error, time=now)
        self.pub.publish(msg)

    def dome_sync_status(self) -> None:
        now = time.time()
        if all(not isinstance(x, CoordMsg) for x in self.dome_enc):
            self.sync_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        elif all(not isinstance(x, CoordMsg) for x in self.antenna_cmd):
            self.sync_checker.check(False)
            msg = TrackingStatus(ok=False, error=9999.0, time=now)
        else:
            error = np.abs(self.antenna_cmd[0].lon - self.dome_enc[0].lon)
            ok = self.sync_checker.check(error < self.limit)
            msg = TrackingStatus(ok=ok, error=error, time=now)
        self.error_pub.publish(msg)
