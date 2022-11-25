import time

from necst import config, namespace, qos
from necst.core.alert import AntennaSpeedAlert
from necst.utils import spinning
from necst_msgs.msg import TimedAzElFloat64
from ...conftest import TesterAlertHandlingNode, destroy


class TestAntennaSpeed(TesterAlertHandlingNode):

    NodeName = "antenna_speed"

    def test_nominal(self):
        alert = AntennaSpeedAlert()

        speed_pub = self.node.create_publisher(
            TimedAzElFloat64, f"{namespace.antenna}/speed", qos.realtime
        )
        with spinning([alert, self.node]):
            msg = TimedAzElFloat64(az=0.5, el=0.5, time=0.1)
            speed_pub.publish(msg)

            timelimit = time.time() + config.ros_topic_scan_interval_sec + 1
            while (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].warning is None
            ):
                assert time.time() < timelimit
            assert (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].warning is False
            )
            assert (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].critical
                is False
            )

        destroy(alert)
        destroy(speed_pub, node=self.node)

    def test_warning(self):
        alert = AntennaSpeedAlert()

        speed_pub = self.node.create_publisher(
            TimedAzElFloat64, f"{namespace.antenna}/speed", qos.realtime
        )
        with spinning([alert, self.node]):
            msg = TimedAzElFloat64(az=1.7, el=0.5, time=0.1)
            speed_pub.publish(msg)

            timelimit = time.time() + config.ros_topic_scan_interval_sec + 1
            while not self.node.status[f"{namespace.alert}/antenna_speed/az"].warning:
                assert time.time() < timelimit
            assert (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].warning is True
            )
            assert (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].critical
                is False
            )

        destroy(alert)
        destroy(speed_pub, node=self.node)

    def test_critical(self):
        alert = AntennaSpeedAlert()

        speed_pub = self.node.create_publisher(
            TimedAzElFloat64, f"{namespace.antenna}/speed", qos.realtime
        )
        with spinning([alert, self.node]):
            msg = TimedAzElFloat64(az=2.1, el=0.5, time=0.1)
            speed_pub.publish(msg)

            timelimit = time.time() + config.ros_topic_scan_interval_sec + 1
            while not (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].critical
            ):
                assert time.time() < timelimit
            assert (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].warning is True
            )
            assert (
                self.node.status[f"{namespace.alert}/antenna_speed/az"].critical is True
            )

        destroy(alert)
        destroy(speed_pub, node=self.node)
