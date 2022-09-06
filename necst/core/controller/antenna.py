from neclib.controllers import PIDController
import rclpy
from rclpy.node import Node
from necst_msgs.msg import CoordMsg, PIDMsg, TimedFloat64
import time


class Antenna_device(Node):

    node_name = "pid"

    def __init__(self, frequency: float) -> None:
        self.controller = PIDController()
        self.create_subscription_ang(CoordMsg, "altaz", self.init_ang)
        self.create_subscription_enc(CoordMsg, "encorder", self.init_enc)
        self.publisher = self.create_publisher(PIDMsg, "speed", self.init_speed)
        self.create_timer(frequency, self.calc_pid)
        self.create_subscription_param(TimeFloat64, "pid_param",
                                       self.change_pid_param)

    def calc_pid(self):
        calculator = PIDController(pid_param=[self.k_p, self.k_i, self.k_d])
        calculator.time.push
        calculator.cmd_coord.push
        calculator.enc_coord.push
        calculator.error.push
        self.publisher.publish(TimeFloat64())

    def init_ang(self):
        self.create_subscription_ang.append("altaz")
     
    def init_enc(self):
        self.create_subscription_enc.append("encorder")

    def init_speed(self) -> None:
        dummy = 0
        self.get_speed(dummy, dummy, stop=True)
        self.command(0, "altaz")

    def change_pid_param(self, msg):
        self.controller.k_p = msg.k_p
        self.controller.k_i = msg.k_i
        self.controller.k_d = msg.k_d

    def emergency_stop(self) -> None:
        dummy = 0
        for _ in range(5):
            self.command(0, 'altaz')
            _ = self.command_ang_speed(dummy, dummy, stop=True)
            time.sleep(0.05)


def main(args=None):
    rclpy.init(args=args)
    node = Antenna_device()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()


