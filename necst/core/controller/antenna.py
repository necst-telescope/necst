from neclib.controllers import PIDController
import rclpy
from rclpy.node import Node
from necst_msgs.msg import CoordMsg, PIDMsg, TimedAzElFloat64
import time


class Antenna_device(Node):

    node_name = "pid"

    def __init__(self, frequency: float = 50):
        self.controller = {
            "az": PIDController(),
            "el": PIDcontroller(),
        }
        self.create_subscription(CoordMsg, "altaz", self.init_ang, 1)
        self.create_subscription(CoordMsg, "encorder", self.init_enc, 1)
        self.publisher = self.create_publisher(TimedAzElFloat64, "speed", int, 1)
        self.create_timer(1/frequency, self.calc_pid)
        self.create_subscription_param(PIDMsg, "pid_param",
                                       self.change_pid_param, 1)

    def calc_pid(self):
        az_speed = self.controller["az"].get_speed(self.az, self.az_enc)
        el_speed = self.controller["el"].get_speed(self.el, self.el_enc)
        msg = TimedAzElFloat64(az=az_speed, el=el_speed, time=time.time())
        self.publisher.publish(msg)
       
    def init_ang(self, msg):
        self.az = msg.lon
        self.el = msg.lat
        self.t = msg.time
     
    def init_enc(self, msg):
        self.az_enc = msg.lon
        self.el_enc = msg.lat
        self.t_enc = msg.time
        
    def change_pid_param(self, msg):
        axis = msg.axis.lower()
        self.controller[axis].k_p = msg.k_p
        self.controller[axis].k_i = msg.k_i
        self.controller[axis].k_d = msg.k_d


def main(args=None):
    rclpy.init(args=args)
    node = Antenna_device()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()


