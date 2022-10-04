import time
import pytest

from necst import qos
from necst.ctrl import HorizontalCoord
from necst.ctrl.antenna.sim_devices import AntennaDeviceSimulator
from neclib.simulators.antenna import AntennaEncoderEmulator
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

        encoder_az0 = encoder_el0 = encoder_az1 = encoder_el1 = None

        def update0(msg):
            nonlocal encoder_az0, encoder_el0
            encoder_az0 = msg.lon
            encoder_el0 = msg.lat

        def update1(msg):
            nonlocal encoder_az1, encoder_el1
            encoder_az1 = msg.lon
            encoder_el1 = msg.lat

        ns = encoder.get_namespace()
        cmd = self.node.create_publisher(TimedAzElFloat64, f"{ns}/speed", qos.realtime)
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/encoder", update0, qos.realtime
        )

        with spinning([encoder, self.node]):
            cmd.publish(TimedAzElFloat64(az=10.0, el=10.0))
            sub

            timelimit = time.time() + 1
            while True:
                assert time.time() < timelimit, "Encoder command not published in 1s"
                az_condition = (encoder_az0 is not None) and (encoder_az0 < encoder_az1)
                el_condition = (encoder_el0 is not None) and (encoder_el0 < encoder_el1)
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(encoder)
        destroy([cmd, sub], self.node)
