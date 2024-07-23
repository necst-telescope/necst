import time

import rclpy
from neclib.simulators.dome import DomeEncoderEmulator
from necst_msgs.msg import CoordMsg
from rclpy.node import Node

from necst import config, namespace, topic


class DomeDeviceSimulator(Node):
    NodeName = "dome_simulator"
    Namespace = namespace.dome

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = topic.dome_encoder.publisher(self)
        topic.dome_speed_cmd.subscription(self, self.dome_simulator)
        self.enc = DomeEncoderEmulator()
        self.create_timer(1 / config.dome_command_frequency, self.stream)

    def dome_simulator(self, msg):
        if msg.speed == "high":
            speed = 600 / 3600
        elif msg.speed == "mid":
            speed = 300 / 3600
        elif msg.speed == "low":
            speed = 60 / 3600
        else:
            speed = 0
        if msg.turn == "right":
            turn = 1.0
        else:
            turn = -1.0
        speed = speed * turn
        self.enc.command(speed, "az")

    def stream(self):
        encoder = self.enc.read()
        az_msg = encoder.az
        msg = CoordMsg(lon=az_msg, unit="deg", frame="altaz", time=time.time())
        self.publisher.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = DomeDeviceSimulator()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
