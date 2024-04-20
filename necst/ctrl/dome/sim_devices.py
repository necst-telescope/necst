import time

import rclpy
from neclib.simulators.antenna import DomeEncoderEmulator
from necst_msgs.msg import CoordMsg
from rclpy.node import Node

from necst import config, namespace, topic


class DomeDeviceSimulator(Node):
    NodeName = "dome_simulator"
    Namespace = namespace.dome

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = topic.dome_encoder.publisher(self)
        topic.dome_speed_cmd.subscription(self, self.antenna_simulator)
        self.enc = DomeEncoderEmulator()
        self.create_timer(1 / config.antenna_command_frequency, self.stream)

    def antenna_simulator(self, msg):
        self.enc.command(msg.az, "az")
        self.enc.command(msg.el, "el")

    def stream(self):
        encoder = self.enc.read()
        az_msg = encoder.az
        el_msg = encoder.el
        msg = CoordMsg(
            lon=az_msg, lat=el_msg, unit="deg", frame="altaz", time=time.time()
        )
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
