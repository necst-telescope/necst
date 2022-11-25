import time

import pytest
from necst_msgs.msg import CoordMsg

from necst import namespace, qos
from necst.core import Authorizer, Commander
from necst.ctrl import AntennaDeviceSimulator, AntennaPIDController, HorizontalCoord
from necst.utils import spinning

from ..conftest import TesterNode, destroy


class TestCommander(TesterNode):

    NodeName = "test_commander"

    def test_node_info(self):
        com = Commander()
        assert "core" in com.get_namespace()
        assert "commander" in com.get_name()

        destroy(com)

    def test_tracking_check(self):
        com = Commander()

        enc = self.node.create_publisher(
            CoordMsg, f"{namespace.antenna}/encoder", qos.realtime
        )
        cmd = self.node.create_publisher(
            CoordMsg, f"{namespace.antenna}/altaz", qos.realtime
        )

        start = time.time()

        def publish():
            duration_passed = time.time() - start
            x = 30.0 - max(0, 1.6 - 1.6 * duration_passed)
            y = 25.0 + max(0, 0.1 - 1.6 * duration_passed)
            cmd.publish(CoordMsg(lon=30.0, lat=25.0, frame="altaz", unit="deg"))
            enc.publish(CoordMsg(lon=x, lat=y, frame="altaz", unit="deg"))

        timer = self.node.create_timer(0.1, lambda: publish())

        with spinning(self.node):
            com.wait_convergence("antenna")
            assert time.time() - start > 0.99
            # It takes at least 0.99826s to converge `x` within 10arcsec

        destroy([enc, cmd, timer], node=self.node)
        destroy(com)

    def test_antenna_point(self):
        com = Commander()
        auth_server = Authorizer()

        cmd = {"lon": 30.0, "lat": 45.0, "unit": "deg", "frame": "fk5"}
        checked = False

        def check(msg: CoordMsg) -> None:
            nonlocal checked
            assert msg.lon == cmd["lon"]
            assert msg.lat == cmd["lat"]
            assert msg.unit == cmd["unit"]
            assert msg.frame == cmd["frame"]
            # assert msg.time == cmd["time"]
            checked = True

        ns = namespace.antenna
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/raw_coord", check, qos.reliable
        )

        timelimit = time.time() + 2
        with spinning([self.node, auth_server]):
            com.get_privilege()
            com.antenna("point", **cmd, wait=False)

            while not checked:
                assert time.time() < timelimit, "Coordinate command not published in 2s"
                time.sleep(0.02)
            com.quit_privilege()

        destroy([com, auth_server])
        destroy(sub, self.node)

    def test_antenna_point_with_wait(self):
        com = Commander()
        auth_server = Authorizer()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        dev = AntennaDeviceSimulator()

        dev.enc.position.az = 29.0
        dev.enc.position.el = 44.0

        cmd = {"lon": 30.0, "lat": 45.0, "unit": "deg", "frame": "altaz"}

        with spinning([auth_server, horizontal, pid, dev]):
            com.get_privilege()
            com.antenna("point", **cmd, wait=True)
            com.quit_privilege()

        destroy([com, auth_server, horizontal, pid, dev])

    def test_antenna_stop(self):
        com = Commander()
        auth_server = Authorizer()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        dev = AntennaDeviceSimulator()

        with spinning([auth_server, horizontal, pid, dev]):
            com.get_privilege()
            com.antenna(
                "point", lon=340, lat=80, frame="altaz", unit="deg", wait=False
            )  # To accelerate to non-zero speed.
            with spinning(com):
                while com.parameters["speed"] is None:
                    time.sleep(0.05)  # Consider antenna_command_offset=3s
                time.sleep(3)  # Additional acceleration time
            assert com.parameters["speed"].az > 1e-4
            assert com.parameters["speed"].el > 1e-4

            com.antenna("stop")
            assert com.parameters["speed"].az < 1e-5
            assert com.parameters["speed"].el < 1e-5

            com.quit_privilege()
        destroy([com, auth_server, horizontal, pid, dev])

    def test_antenna_stop_even_though_command_is_supplied(self):
        com = Commander()
        auth_server = Authorizer()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        dev = AntennaDeviceSimulator()

        with spinning([auth_server, horizontal, pid, dev]):
            com.get_privilege()

            com.antenna(
                "point", lon=340, lat=80, frame="altaz", unit="deg", wait=False
            )  # To accelerate to non-zero speed.
            with spinning(com):
                while com.parameters["speed"] is None:
                    time.sleep(0.05)  # Consider antenna_command_offset=3s
                time.sleep(3)  # Additional acceleration time
            assert com.parameters["speed"].az > 1e-4
            assert com.parameters["speed"].el > 1e-4

            com.antenna("stop", lon=30, lat=45, frame="altaz", unit="deg")
            assert com.parameters["speed"].az < 1e-5
            assert com.parameters["speed"].el < 1e-5

            com.quit_privilege()
        destroy([com, auth_server, horizontal, pid, dev])
