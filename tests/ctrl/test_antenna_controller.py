import time
from typing import Tuple

from necst import qos
from necst.ctrl import AntennaPIDController
from necst_msgs.msg import CoordMsg, PIDMsg, TimedAzElFloat64
from ..conftest import TesterNode, destroy, spinning


class TestAntennaController(TesterNode):

    NodeName = "test_antenna"

    def test_node_info(self):
        controller = AntennaPIDController()
        assert "ctrl/antenna" in controller.get_namespace()
        assert "controller" in controller.get_name()

        destroy(controller)

    def test_speed_is_published(self):
        controller = AntennaPIDController()

        speed_az = speed_el = None

        def update(msg):
            nonlocal speed_az, speed_el
            speed_az = msg.az
            speed_el = msg.el

        ns = controller.get_namespace()
        cmd = self.node.create_publisher(CoordMsg, f"{ns}/altaz", qos.realtime)
        enc = self.node.create_publisher(CoordMsg, f"{ns}/encoder", qos.realtime)
        sub = self.node.create_subscription(
            TimedAzElFloat64, f"{ns}/speed", update, qos.realtime
        )

        with spinning([controller, self.node]):
            for i in range(10):
                cmd.publish(CoordMsg(lon=30.0, lat=45.0, time=time.time() + 0.1))
                enc.publish(CoordMsg(lon=25.0, lat=45.0, time=time.time() + 0.1))

            timelimit = time.time() + 1
            while True:
                assert time.time() < timelimit, "Speed command not published in 1s"
                az_condition = (speed_az is not None) and (speed_az > 0)
                el_condition = (speed_el is not None) and (speed_el == 0)
                if az_condition and el_condition:
                    break
                time.sleep(0.02)

        destroy(controller)
        destroy([cmd, enc, sub], self.node)

    def test_change_pid_parameter(self):
        def get_pid_param(ctrl, axis) -> Tuple[float, float, float]:
            pid_controller = ctrl.controller[axis]
            return (pid_controller.k_p, pid_controller.k_i, pid_controller.k_d)

        controller = AntennaPIDController()
        ns = controller.get_namespace()
        pub_pid = self.node.create_publisher(PIDMsg, f"{ns}/pid_param", qos.reliable)

        default_az_parameters = get_pid_param(controller, "az")
        with spinning(controller):
            new_parameters = tuple(param + 1 for param in default_az_parameters)
            msg = PIDMsg(
                k_p=new_parameters[0],
                k_i=new_parameters[1],
                k_d=new_parameters[2],
                axis="az",
            )
            pub_pid.publish(msg)

            timelimit = time.time() + 1
            while True:
                assert time.time() < timelimit, "PID parameters not updated in 1s"
                if get_pid_param(controller, "az") == new_parameters:
                    break
                time.sleep(0.05)

        destroy(controller)
        destroy(pub_pid, self.node)
