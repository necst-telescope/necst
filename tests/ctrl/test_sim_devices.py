from base64 import encode
import time

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
        cmd = self.node.create_publisher(CoordMsg, f"{ns}/altaz", qos.realtime)
        enc = self.node.create_publisher(CoordMsg, f"{ns}/encoder", qos.realtime)
        sub = self.node.create_subscription(
            TimedAzElFloat64, f"{ns}/encoder", update, qos.realtime
        )
