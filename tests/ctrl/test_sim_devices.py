import time
from pyrsistent import l
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
        l_az = []
        l_el = []

        def update(msg):
            nonlocal encoder_az, encoder_el
            encoder_az = msg.lon
            encoder_el = msg.lat
            l_az.append(encoder_az)
            l_el.append(encoder_el)

        ns = encoder.get_namespace()
        cmd = self.node.create_publisher(TimedAzElFloat64, f"{ns}/speed", qos.realtime)
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/encoder", update, qos.realtime
        )

        with spinning([encoder, self.node]):
            cmd.publish(TimedAzElFloat64(az=2.0, el=2.0))

            timelimit = time.time() + 2
            time.sleep(1.0)

            while True:
                assert time.time() < timelimit, "Encoder command not published in 1s"
                az_condition = (encoder_az is not None) and (
                    l_az[0] < l_az[len(l_az) - 1]
                )
                el_condition = (encoder_el is not None) and (
                    l_el[0] < l_el[len(l_el) - 1]
                )
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(encoder)
        destroy([cmd, sub], self.node)
