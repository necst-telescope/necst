from neclib.controllers import PIDController
import rclpy
from rclpy.node import Node
from necst_msgs.msg import CoordMsg, PIDMsg, TimedFloat64
import time
import history depth


class Antenna_device(Node):

    node_name = "pid"

    def __init__(self, frequency: float = 50):
        self.controller = PIDController()
        self.create_subscription(CoordMsg, "altaz", self.init_ang, 1)
        self.create_subscription(CoordMsg, "encorder", self.init_enc, 1)
        self.publisher = self.create_publisher(TimedFloat64, "speed", QoSProfile, 1)
        self.create_timer(1/frequency, self.calc_pid)
        self.create_subscription_param(PIDMsg, "pid_param",
                                       self.change_pid_param, 1)

    def calc_pid(self):
        self.publisher.publish(TimedFloat64("speed", time.time()))

    def init_ang(self, msg):
        self.az = msg.lon
        self.el = msg.lat
        self.t = msg.time
     
    def init_enc(self, msg):
        self.az_enc = msg.lon_enc
        self.el_enc = msg.lat_enc
        self.t_enc = msg.time_enc

    def change_pid_param(self, msg):
        self.controller.k_p = msg.k_p
        self.controller.k_i = msg.k_i
        self.controller.k_d = msg.k_d


def main(args=None):
    rclpy.init(args=args)
    node = Antenna_device()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()


