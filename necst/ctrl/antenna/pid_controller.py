import time

from neclib.controllers import PIDController
from rclpy.node import Node

from necst import config, namespace, qos
from necst_msgs.msg import CoordMsg, PIDMsg, TimedAzElFloat64


class AntennaPIDController(Node):

    NodeName = "controller"
    Namespace = namespace.antenna

    def __init__(self, **kwargs):
        super().__init__(self.NodeName, namespace=self.Namespace, **kwargs)
        self.logger = self.get_logger()
        self.controller = {
            "az": PIDController(),
            "el": PIDController(),
        }
        self.create_subscription(CoordMsg, "altaz", self.update_command, qos.realtime)
        self.create_subscription(
            CoordMsg, "encoder", self.update_encoder_reading, qos.realtime
        )
        self.publisher = self.create_publisher(TimedAzElFloat64, "speed", qos.realtime)
        self.create_timer(1 / config.antenna_command_frequency, self.calc_pid)
        self.create_subscription(
            PIDMsg, "pid_param", self.change_pid_param, qos.reliable
        )
        self.az = self.el = self.az_enc = self.el_enc = self.t = self.t_enc = None
        self.list = []

    def calc_pid(self) -> None:
        for msg in self.list:
            if msg.time >= time.time():
                return msg.lon, msg.lat
            else:
                pass
        if any(param is None for param in [self.az, self.el, self.az_enc, self.el_enc]):
            az_speed = 0.0
            el_speed = 0.0
        else:
            az_speed = self.controller["az"].get_speed(msg.lon, self.az_enc)
            el_speed = self.controller["el"].get_speed(msg.lat, self.el_enc)
        msg = TimedAzElFloat64(az=az_speed, el=el_speed, time=time.time())
        self.publisher.publish(msg)

    def update_command(self, msg: CoordMsg) -> None:
        self.list.append(msg)

    def update_encoder_reading(self, msg: CoordMsg) -> None:
        self.az_enc = msg.lon
        self.el_enc = msg.lat
        self.t_enc = msg.time

    def change_pid_param(self, msg: PIDMsg) -> None:
        axis = msg.axis.lower()
        self.controller[axis].k_p = msg.k_p
        self.controller[axis].k_i = msg.k_i
        self.controller[axis].k_d = msg.k_d


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AntennaPIDController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()
