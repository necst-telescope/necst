import time
from unittest.mock import Mock

import pytest
from necst_msgs.msg import ChopperMsg, CoordMsg
from necst_msgs.srv import CoordinateCommand

from necst import NECSTTimeoutError, service, topic
from necst.core import Authorizer, Commander
from necst.ctrl import (
    AntennaDeviceSimulator,
    AntennaPIDController,
    AntennaTrackingStatus,
    HorizontalCoord,
)
from necst.utils import spinning

from ..conftest import TesterNode, destroy


def _wait_for_antenna_stop(com):
    timelimit = time.monotonic() + 3
    while time.monotonic() < timelimit:
        speed = com.get_message("speed")
        if speed.az < 1e-5 and speed.el < 1e-5:
            return
        time.sleep(0.02)

    speed = com.get_message("speed")
    assert speed.az < 1e-5
    assert speed.el < 1e-5


class TestCommander(TesterNode):
    NodeName = "test_commander"

    def test_node_info(self):
        com = Commander()
        assert "core" in com.get_namespace()
        assert "commander" in com.get_name()

        destroy(com)

    @pytest.mark.parametrize(
        ("stop", "cached_recording", "expected_recording"),
        [(True, True, False), (False, False, True)],
    )
    def test_record_waits_for_correlated_status_without_command_storm(
        self, stop, cached_recording, expected_recording
    ):
        published = []

        class Publisher:
            def publish(self, msg):
                published.append(msg)

        com = object.__new__(Commander)
        com.publisher = {"recorder": Publisher()}
        com.record_status_timeout_sec = 1.0
        com.record_status_retry_interval_sec = 0.1
        calls = []

        def get_message(key, *, timeout_sec):
            calls.append((key, timeout_sec))
            if len(calls) == 1:
                return type(
                    "Status",
                    (),
                    {"request_id": "old-request", "recording": cached_recording},
                )()
            return type(
                "Status",
                (),
                {
                    "request_id": published[0].request_id,
                    "recording": expected_recording,
                },
            )()

        com.get_message = get_message
        com._record_until_status(
            name="test",
            stop=stop,
            timeout_sec=1.0,
            retry_interval_sec=0.1,
        )

        assert len(published) == 1
        assert published[0].stop is stop
        assert published[0].time != 0.0
        assert published[0].request_id.startswith("record-")

    def test_record_status_timeout_limits_retries(self):
        published = []

        class Publisher:
            def publish(self, msg):
                published.append(msg)

        com = object.__new__(Commander)
        com.publisher = {"recorder": Publisher()}

        def get_message(key, *, timeout_sec):
            return type(
                "Status", (), {"request_id": "old-request", "recording": True}
            )()

        com.get_message = get_message
        with pytest.raises(NECSTTimeoutError):
            com._record_until_status(
                name="test",
                stop=True,
                timeout_sec=0.08,
                retry_interval_sec=0.02,
            )

        assert 1 <= len(published) <= 5
        assert len({msg.request_id for msg in published}) == 1

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

        def check(
            req: CoordinateCommand.Request, res: CoordinateCommand.Response
        ) -> None:
            nonlocal checked
            assert req.lon[0] == cmd["target"][0]
            assert req.lat[0] == cmd["target"][1]
            assert req.unit == cmd["unit"]
            assert req.frame == cmd["target"][2]
            checked = True
            return res

        sub = service.raw_coord.service(self.node, check)

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
        destroy(sub, node=self.node)

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

        try:
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
                _wait_for_antenna_stop(com)

                com.quit_privilege()
        finally:
            destroy([com, auth_server, horizontal, pid, dev])

    def test_antenna_stop_even_though_command_is_supplied(self):
        com = Commander()
        auth_server = Authorizer()
        horizontal = HorizontalCoord()
        pid = AntennaPIDController()
        dev = AntennaDeviceSimulator()

        try:
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
                _wait_for_antenna_stop(com)

                com.quit_privilege()
        finally:
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
            response = ChopperMsg(
                insert=msg.insert,
                position=msg.position,
                time=time.time(),
            )
            pub.publish(response)

        sub = topic.chopper_cmd.subscription(self.node, update)

        with spinning([auth, self.node]):
            com.get_privilege()
            com.chopper("remove", wait=True)
            com.quit_privilege()

        destroy([com, auth])
        destroy([sub, pub], node=self.node)

    def test_chopper_wait_uses_endpoint_position(self):
        com = Commander()
        com.get_message = Mock(
            side_effect=[
                ChopperMsg(insert=True, position=10000, time=time.time()),
                ChopperMsg(insert=True, position=4750, time=time.time()),
            ]
        )

        com.wait_oc(target="chopper", position="insert")

        assert com.get_message.call_count == 2
        destroy(com)

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
