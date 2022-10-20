import time

import pytest

from necst import namespace, qos
from necst.core import Authorizer, Commander
from necst_msgs.msg import CoordMsg
from ..conftest import TesterNode, destroy, spinning


class TestCommander(TesterNode):

    NodeName = "test_commander"

    def test_node_info(self):
        com = Commander()
        assert "core" in com.get_namespace()
        assert "commander" in com.get_name()

        destroy(com)

    def test_antenna_point(self):
        com = Commander()
        auth_server = Authorizer()

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

        ns = namespace.antenna
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/raw_coord", check, qos.reliable
        )

        timelimit = time.time() + 2
        with spinning([com, self.node, auth_server]):
            com.get_privilege()
            com.antenna("point", **cmd, wait=False)

            while not checked:
                assert time.time() < timelimit, "Coordinate command not published in 2s"
                time.sleep(0.02)

        destroy([com, auth_server])
        destroy(sub, self.node)

    @pytest.mark.skip(reason="Method not established")
    def test_antenna_point_with_wait(self):
        ...

    def test_antenna_stop(self):
        com = Commander()
        auth_server = Authorizer()

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

        ns = namespace.antenna
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/raw_coord", check, qos.reliable
        )
        pub_enc = self.node.create_publisher(CoordMsg, f"{ns}/encoder", qos.realtime)
        timer = self.node.create_timer(0.01, lambda: pub_enc.publish(CoordMsg(**enc)))

        timelimit = time.time() + 2
        with spinning([com, self.node, auth_server]):
            com.get_privilege()
            com.antenna("stop", **cmd)

            while not checked:
                assert time.time() < timelimit, "Coordinate command not published in 2s"
                time.sleep(0.02)

        destroy([com, auth_server])
        destroy([sub, pub_enc, timer], self.node)

    @pytest.mark.skip(reason="Method not established")
    def test_antenna_stop_with_wait(self):
        ...

    @pytest.mark.skip(reason="Method not established")
    def test_wait(self):
        ...
