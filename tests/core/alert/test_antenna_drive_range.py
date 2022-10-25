import time

from necst import config, namespace, qos
from necst.core.alert import AntennaDriveRangeAlert
from necst_msgs.msg import CoordMsg
from ...conftest import TesterAlertHandlingNode, destroy, spinning


class TestAntennaSpeed(TesterAlertHandlingNode):

    NodeName = "antenna_drive_range"

    def test_nominal(self):
        alert = AntennaDriveRangeAlert()

        enc_pub = self.node.create_publisher(
            CoordMsg, f"{namespace.antenna}/encoder", qos.realtime
        )
        with spinning([alert, self.node]):
            msg = CoordMsg(
                lon=50.0, lat=45.0, frame="altaz", unit="deg", time=time.time()
            )
            enc_pub.publish(msg)

            timelimit = time.time() + config.ros_topic_scan_interval_sec + 1
            while (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].warning
                is None
            ):
                assert time.time() < timelimit
            assert (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].warning
                is False
            )
            assert (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].critical
                is False
            )

        destroy(alert)
        destroy(enc_pub, node=self.node)

    def test_warning(self):
        alert = AntennaDriveRangeAlert()

        enc_pub = self.node.create_publisher(
            CoordMsg, f"{namespace.antenna}/encoder", qos.realtime
        )
        with spinning([alert, self.node]):
            msg = CoordMsg(
                lon=351.0, lat=45.0, frame="altaz", unit="deg", time=time.time()
            )
            enc_pub.publish(msg)

            timelimit = time.time() + config.ros_topic_scan_interval_sec + 1
            while not self.node.status[
                f"{namespace.alert}/antenna_drive_range/az"
            ].warning:
                assert time.time() < timelimit
            assert (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].warning
                is True
            )
            assert (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].critical
                is False
            )

        destroy(alert)
        destroy(enc_pub, node=self.node)

    def test_critical(self):
        alert = AntennaDriveRangeAlert()

        enc_pub = self.node.create_publisher(
            CoordMsg, f"{namespace.antenna}/encoder", qos.realtime
        )
        with spinning([alert, self.node]):
            msg = CoordMsg(
                lon=357.0, lat=45.0, frame="altaz", unit="deg", time=time.time()
            )
            enc_pub.publish(msg)

            timelimit = time.time() + config.ros_topic_scan_interval_sec + 1
            while not (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].critical
            ):
                assert time.time() < timelimit
            assert (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].warning
                is True
            )
            assert (
                self.node.status[f"{namespace.alert}/antenna_drive_range/az"].critical
                is True
            )

        destroy(alert)
        destroy(enc_pub, node=self.node)
