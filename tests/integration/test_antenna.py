import time

import pytest

from necst import namespace, qos
from necst.core import Commander
from necst.ctrl.exec_antenna_sim import configure_executor
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
from ..conftest import TesterNode, destroy, spinning


class TestAntenna(TesterNode):

    NodeName = "test_antenna"

    def test_interact(self):
        com = Commander()

        commanded = False
        converted = False
        pid_cmd = False
        responded = False

        target_az = 170.0
        target_el = 55.0

        def raw_coord_clbk(msg: CoordMsg):
            nonlocal commanded
            commanded = True

        def altaz_clbk(msg: CoordMsg):
            nonlocal converted
            converted = True

        def speed_clbk(msg: TimedAzElFloat64):
            nonlocal pid_cmd
            pid_cmd = True

        def encoder_clbk(msg: CoordMsg):
            nonlocal responded
            az, el = msg.lon, msg.lat
            if (abs(az - target_az) < 3) and (abs(el - target_el) < 3):  # Accuracy=3deg
                responded = True

        raw_coord = self.node.create_subscription(
            CoordMsg, f"{namespace.antenna}/raw_coord", raw_coord_clbk, qos.reliable
        )
        altaz = self.node.create_subscription(
            CoordMsg, f"{namespace.antenna}/altaz", altaz_clbk, qos.realtime
        )
        speed = self.node.create_subscription(
            TimedAzElFloat64, f"{namespace.antenna}/speed", speed_clbk, qos.realtime
        )
        encoder = self.node.create_subscription(
            CoordMsg, f"{namespace.antenna}/encoder", encoder_clbk, qos.realtime
        )

        executor = configure_executor()
        with spinning(executor=executor), spinning([self.node, com]):
            com.antenna(
                "point",
                lon=target_az,
                lat=target_el,
                unit="deg",
                frame="altaz",
                tracking_check=False,
            )
            timelimit = time.time() + 3
            while not commanded:
                assert time.time() < timelimit, "Command not published in 3s"
            timelimit += 5
            while not converted:
                assert time.time() < timelimit, "Coordinate command not completed in 8s"
            while not pid_cmd:
                assert time.time() < timelimit, "Speed command not published in 8s"
            timelimit += 12
            while not responded:
                assert time.time() < timelimit, "Motor not responded to command in 20s"

        destroy(executor)
        destroy(com)
        destroy([raw_coord, altaz, speed, encoder], self.node)

    @pytest.mark.skip(reason="Control still not accurate")
    def test_convergence(self):
        ...
