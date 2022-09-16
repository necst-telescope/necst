import time

import pytest

from necst.core import Commander
from necst_msgs.msg import CoordMsg
from ..conftest import TesterNode, destroy, spinning


class TestCommander(TesterNode):

    NodeName = "test_commander"

    def test_node_info(self):
        com = Commander()
        assert "core" in com.get_namespace()
        assert "commander" in com.get_name()

        destroy(com)

    def test_antenna_drive(self):
        com = Commander()

        cmd = {
            "lon": 30.0,
            "lat": 45.0,
            "unit": "deg",
            "frame": "fk5",
            "time": time.time(),
        }
        checked = False

        def check(msg: CoordMsg) -> None:
            nonlocal checked
            assert msg.lon == cmd["lon"]
            assert msg.lat == cmd["lat"]
            assert msg.unit == cmd["unit"]
            assert msg.frame == cmd["frame"]
            assert msg.time == cmd["time"]
            checked = True

        ns = com.get_namespace()
        sub = self.node.create_subscription(CoordMsg, f"{ns}/raw_coord", check, 1)

        timelimit = time.time() + 3
        with spinning([com, self.node]):
            com.antenna("drive", **cmd, tracking_check=False)

            while not checked:
                assert time.time() < timelimit, "Coordinate command not published in 3s"
                time.sleep(0.02)

        destroy(com)
        destroy(sub, self.node)

    @pytest.mark.skip(reason="Method not established")
    def test_antenna_drive_with_tracking_check(self):
        ...

    def test_antenna_stop(self):
        com = Commander()

        cmd = {
            "lon": 30.0,
            "lat": 45.0,
            "unit": "deg",
            "frame": "fk5",
            "time": time.time(),
        }
        enc = {"lon": 10.0, "lat": 15.0, "unit": "deg", "frame": "altaz"}
        checked = False

        def check(msg: CoordMsg) -> None:
            nonlocal checked
            assert msg.lon == enc["lon"]
            assert msg.lat == enc["lat"]
            assert msg.unit == enc["unit"]
            assert msg.frame == enc["frame"]
            checked = True

        ns = com.get_namespace()
        sub = self.node.create_subscription(CoordMsg, f"{ns}/raw_coord", check, 1)
        pub_enc = self.node.create_publisher(CoordMsg, f"{ns}/encoder", 1)
        timer = self.node.create_timer(0.01, lambda: pub_enc.publish(CoordMsg(**enc)))

        timelimit = time.time() + 3
        with spinning([com, self.node]):
            com.antenna("stop", **cmd)

            while not checked:
                assert time.time() < timelimit, "Coordinate command not published in 3s"
                time.sleep(0.02)

        destroy(com)
        destroy([sub, pub_enc, timer], self.node)

    @pytest.mark.skip(reason="Method not established")
    def test_antenna_stop_with_tracking_check(self):
        ...

    @pytest.mark.skip(reason="Method not established")
    def test_tracking_check(self):
        ...
