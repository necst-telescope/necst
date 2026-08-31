import time

from necst import config, qos
from necst.ctrl.antenna.sim_devices import AntennaDeviceSimulator
from necst.ctrl.calibrator.sim_devices import ChopperSimulator
from necst.utils import spinning
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
from ..conftest import TesterNode, destroy


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

        ns = encoder.get_namespace()
        cmd = self.node.create_publisher(TimedAzElFloat64, f"{ns}/speed", qos.realtime)
        sub = self.node.create_subscription(
            CoordMsg, f"{ns}/encoder", update, qos.realtime
        )

        with spinning([encoder, self.node]):
            cmd.publish(TimedAzElFloat64(az=2.0, el=2.0))

            time.sleep(1.0)
            timelimit = time.time() + 10

            while True:
                assert time.time() < timelimit, "Encoder command not published in 10s"
                az_condition = (len(l_az) != 0) and (l_az[0] < l_az[-1])
                el_condition = (len(l_el) != 0) and (l_el[0] < l_el[-1])
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(encoder)
        destroy([cmd, sub], self.node)


class TestChopperSimulatorTelemetry:
    def test_insert_uses_configured_motor_position(self):
        msg = ChopperSimulator._telemetry_message("insert", 123.0)

        assert msg is not None
        assert msg.insert is True
        assert msg.position == config.chopper_motor_position["insert"]
        assert msg.time == 123.0

    def test_remove_uses_configured_motor_position(self):
        msg = ChopperSimulator._telemetry_message("remove", 456.0)

        assert msg is not None
        assert msg.insert is False
        assert msg.position == config.chopper_motor_position["remove"]
        assert msg.time == 456.0

    def test_unknown_position_is_not_published(self):
        assert ChopperSimulator._telemetry_message("moving", 789.0) is None
