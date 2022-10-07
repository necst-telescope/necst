import time
import pytest

from necst import qos
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

        l_az = []
        l_el = []

        def update(msg):
            l_az.append(msg.lon)
            l_el.append(msg.lat)

        # def test_speed_is_published(msg):
        #    print(msg)

        ns = encoder.get_namespace()
        cmd = self.node.create_publisher(TimedAzElFloat64, f"{ns}/speed", 1)
        sub = self.node.create_subscription(CoordMsg, f"{ns}/encoder", update, 1)
        # sub2 = self.node.create_subscription(
        #    TimedAzElFloat64,
        #    "test_encoder_is_published/speed",
        #    test_speed_is_published,
        #    1,
        # )

        with spinning([encoder, self.node]):
            cmd.publish(TimedAzElFloat64(az=2.0, el=2.0))

            time.sleep(1.0)
            timelimit = time.time() + 10

            while True:
                assert time.time() < timelimit, "Encoder command not published in 10s"
                print(l_az[len(l_az) - 1])
                az_condition = (len(l_az) != 0) and (l_az[0] < l_az[len(l_az) - 1])
                el_condition = (len(l_el) != 0) and (l_el[0] < l_el[len(l_el) - 1])
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(encoder)
        destroy([cmd, sub], self.node)
