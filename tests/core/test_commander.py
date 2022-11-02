import time

import pytest

from necst import namespace, qos
from necst.core import Authorizer, Commander
from necst.utils import spinning
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
from ..conftest import TesterNode, destroy


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

        com.quit_privilege()
        destroy([com, auth_server])
        destroy(sub, self.node)

    # def test_antenna_point_with_wait(self):
    #     com = Commander()
    #     auth_server = Authorizer()

    #     cmd = {"lon": 30.0, "lat": 45.0, "unit": "deg", "frame": "altaz"}

    def test_antenna_stop(self):
        com = Commander()
        auth_server = Authorizer()

        ns = namespace.antenna
        pub_speed = self.node.create_publisher(
            TimedAzElFloat64, f"{ns}/speed", qos.realtime
        )

        start = time.time()

        def publish_speed():
            decay_factor = 1 / (time.time() - start + 1) ** 5
            msg = TimedAzElFloat64(az=1.6 * decay_factor, el=1e-5, time=time.time())
            pub_speed.publish(msg)

        timer = self.node.create_timer(0.1, publish_speed)

        with spinning([auth_server, self.node]):
            com.get_privilege()

            com.antenna("stop")
            assert com.parameters["speed"].az < 1e-3
            assert com.parameters["speed"].el < 1e-3

            com.antenna("stop", lon=30, lat=45, frame="altaz", unit="deg")
            assert com.parameters["speed"].az < 1e-3
            assert com.parameters["speed"].el < 1e-3

        com.quit_privilege()
        destroy([com, auth_server])
        destroy([pub_speed, timer], node=self.node)

    @pytest.mark.skip(reason="Method not established")
    def test_wait(self):
        ...
