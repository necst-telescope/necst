import time

from necst_msgs.msg import ChopperMsg, CoordCmdMsg, CoordMsg

from necst import topic
from necst.core import Authorizer, Commander
from necst.ctrl import (
    AntennaDeviceSimulator,
    AntennaPIDController,
    AntennaTrackingStatus,
    HorizontalCoord,
)
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

        enc = topic.antenna_encoder.publisher(self.node)
        cmd = topic.altaz_cmd.publisher(self.node)
        tracking = AntennaTrackingStatus()

        start = time.monotonic()

        def publish():
            duration_passed = time.monotonic() - start
            x = 30.0 - max(0, 1.6 - 1.6 * duration_passed)
            y = 25.0 + max(0, 0.1 - 1.6 * duration_passed)
            cmd.publish(
                CoordMsg(
                    lon=30.0, lat=25.0, frame="altaz", unit="deg", time=time.time()
                )
            )
            enc.publish(
                CoordMsg(lon=x, lat=y, frame="altaz", unit="deg", time=time.time())
            )

        timer = self.node.create_timer(0.1, lambda: publish())

        with spinning([self.node, tracking]):
            com.wait("antenna")
            assert time.monotonic() - start > 0.99
            # It takes at least 0.99826s to converge `x` within 10arcsec

        destroy([enc, cmd, timer], node=self.node)
        destroy([com, tracking])

    def test_antenna_point(self):
        com = Commander()
        auth_server = Authorizer()

        cmd = {"target": (30.0, 45.0, "fk5"), "unit": "deg"}
        checked = False

        def check(msg: CoordCmdMsg) -> None:
            nonlocal checked
            assert msg.lon[0] == cmd["target"][0]
            assert msg.lat[0] == cmd["target"][1]
            assert msg.unit == cmd["unit"]
            assert msg.frame == cmd["target"][2]
            checked = True

        sub = topic.raw_coord.subscription(self.node, check)

        start = time.monotonic()
        with spinning([self.node, auth_server]):
            com.get_privilege()
            com.antenna("point", **cmd, wait=False)

            while not checked:
                assert (
                    time.monotonic() - start < 2
                ), "Coordinate command not published in 2s"
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
        tracking = AntennaTrackingStatus()

        dev.enc.position.az = 29.0
        dev.enc.position.el = 44.0

        cmd = {"target": (30.0, 45.0, "altaz"), "unit": "deg"}

        with spinning([auth_server, horizontal, pid, dev, tracking], n_thread=6):
            com.get_privilege()
            com.antenna("point", **cmd, wait=True)
            com.quit_privilege()

        destroy([com, auth_server, horizontal, pid, dev, tracking])

    def test_antenna_stop(self):
        com = Commander()
        auth_server = Authorizer()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        dev = AntennaDeviceSimulator()

        with spinning([auth_server, horizontal, pid, dev], n_thread=5):
            com.get_privilege()
            com.antenna(
                "point", target=(340, 80, "altaz"), unit="deg", wait=False
            )  # To accelerate to non-zero speed.
            _ = com.get_message("speed")
            time.sleep(5)  # Additional acceleration time
            assert com.get_message("speed").az > 1e-4
            assert com.get_message("speed").el > 1e-4

            com.antenna("stop")
            assert com.get_message("speed").az < 1e-5
            assert com.get_message("speed").el < 1e-5

            com.quit_privilege()
        destroy([com, auth_server, horizontal, pid, dev])

    def test_antenna_stop_even_though_command_is_supplied(self):
        com = Commander()
        auth_server = Authorizer()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        dev = AntennaDeviceSimulator()

        with spinning([auth_server, horizontal, pid, dev], n_thread=5):
            com.get_privilege()

            com.antenna(
                "point", target=(340, 80, "altaz"), unit="deg", wait=False
            )  # To accelerate to non-zero speed.
            _ = com.get_message("speed")
            time.sleep(5)  # Additional acceleration time
            assert com.get_message("speed").az > 1e-4
            assert com.get_message("speed").el > 1e-4

            com.antenna("stop", target=(30, 45, "altaz"), unit="deg")
            assert com.get_message("speed").az < 1e-5
            assert com.get_message("speed").el < 1e-5

            com.quit_privilege()
        destroy([com, auth_server, horizontal, pid, dev])

    def test_pid_parameter_change(self):
        com = Commander()
        pid = AntennaPIDController()
        auth = Authorizer()

        with spinning([pid, auth]):
            com.get_privilege()
            pid.controller["az"].k_p != 3
            pid.controller["az"].k_i != 4
            pid.controller["az"].k_d != 5.0
            com.pid_parameter("set", Kp=3, Ki=4, Kd=5.0, axis="az")
            com.quit_privilege()

            timelimit = time.time() + 3
            while pid.controller["az"].k_p != 3:
                assert time.time() < timelimit
            pid.controller["az"].k_i == 4
            pid.controller["az"].k_d == 5.0

        destroy([com, pid, auth])

    def test_chopper_insert(self):
        com = Commander()
        auth = Authorizer()
        checked = False

        def update(msg: ChopperMsg):
            nonlocal checked
            if msg.insert:
                checked = True

        sub = topic.chopper_cmd.subscription(self.node, update)
        timelimit = time.time() + 3
        with spinning([auth, self.node]):
            com.get_privilege()
            com.chopper("insert", wait=False)
            com.quit_privilege()
            while not checked:
                assert time.time() < timelimit, "Chopper command not published in 3s"
                time.sleep(0.05)

        destroy([com, auth])
        destroy(sub, node=self.node)

    def test_chopper_remove(self):
        com = Commander()
        auth = Authorizer()
        checked = False

        def update(msg: ChopperMsg):
            nonlocal checked
            if not msg.insert:
                checked = True

        sub = topic.chopper_cmd.subscription(self.node, update)
        timelimit = time.time() + 3
        with spinning([auth, self.node]):
            com.get_privilege()
            com.chopper("remove", wait=False)
            com.quit_privilege()
            while not checked:
                assert time.time() < timelimit, "Chopper command not published in 3s"
                time.sleep(0.05)

        destroy([com, auth])
        destroy(sub, node=self.node)

    def test_chopper_move_wait(self):
        com = Commander()
        auth = Authorizer()

        pub = topic.chopper_status.publisher(self.node)

        def update(msg: ChopperMsg):
            response = ChopperMsg(insert=msg.insert, time=time.time())
            pub.publish(response)

        sub = topic.chopper_cmd.subscription(self.node, update)

        with spinning([auth, self.node]):
            com.get_privilege()
            com.chopper("remove", wait=True)
            com.quit_privilege()

        destroy([com, auth])
        destroy([sub, pub], node=self.node)

    def test_chopper_status_query(self):
        com = Commander()
        pub = topic.chopper_status.publisher(self.node)
        now = time.time()
        msg = ChopperMsg(insert=True, time=now)
        self.node.create_timer(0.5, lambda: pub.publish(msg))

        with spinning(self.node):
            status = com.chopper("?")
            assert status.insert is True
            assert status.time == now

        destroy([pub], node=self.node)
        destroy(com)
