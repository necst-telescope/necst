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

        encoder_az = encoder_el = None

        def update(msg):
            nonlocal encoder_az, encoder_el
            encoder_az = msg.az
            encoder_el = msg.el

        enc = AntennaEncoderEmulator()
        enc_az = enc.command(10.0, "az")
        enc_el = enc.command(10.0, "el")

        ns = encoder.get_namespace()
        cmd = self.node.create_publisher(TimedAzElFloat64, f"{ns}/speed", qos.realtime)
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/encoder", update, qos.realtime
        )

        with spinning([encoder, self.node]):
            cmd.publish(TimedAzElFloat64(az=10.0, el=10.0))

            timelimit = time.time() + 1
            while True:
                assert time.time() < timelimit, "Encoder command not published in 1s"
                az_condition = (encoder_az is not None) and (
                    enc_az - 1 < encoder_az < enc_az + 1
                )
                el_condition = (encoder_el is not None) and (
                    enc_el - 1 < encoder_el < enc_el + 1
                )
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(encoder)
        destroy([cmd, sub], self.node)
