import time
from turtle import speed

import pytest

from necst import qos
from necst.ctrl import HorizontalCoord
from necst.ctrl.antenna.sim_devices import AntennaDeviceSimulator
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
from ..conftest import TesterNode, destroy, spinning


class TestAntennaDeviceSimulator(TesterNode):

    NodeName = "test_antenna_device_simulator"

    def test_node_info(self):
        encoder = AntennaDeviceSimulator()
        assert "ctrl/antenna" in encoder.get_namespace()
        assert "antenna_simulator" in encoder.get_name()

        destroy(encoder)

    def test_encoder_is_published(self):
        encoder = AntennaDeviceSimulator()

        encoder_az = encoder_el = None

        def update(msg):
            nonlocal encoder_az, encoder_el
            encoder_az = msg.az
            encoder_el = msg.el

        ns = encoder.get_namespace()
        cmd = self.node.create_publisher(TimedAzElFloat64, f"{ns}/speed", qos.realtime)
        # enc = self.node.create_publisher(CoordMsg, f"{ns}/encoder", qos.realtime)
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/encoder", update, qos.realtime
        )

        with spinning([encoder, self.node]):
            cmd.publish(TimedAzElFloat64(speed=20.0))
            # enc.publish(CoordMsg(lon=25.0, lat=45.0))

            timelimit = time.time() + 1
            while True:
                assert time.time() < timelimit, "Encoder command not published in 1s"
                az_condition = (encoder_az is not None) and (encoder_az > 0)
                el_condition = (encoder_el is not None) and (encoder_el == 0)
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(encoder)
        destroy([cmd, enc, sub], self.node)
