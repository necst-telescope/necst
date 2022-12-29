import time

import rclpy
from neclib.simulators.antenna import AntennaEncoderEmulator
from necst_msgs.msg import CoordMsg
from rclpy.node import Node

from necst import config, namespace, topic


class AntennaDeviceSimulator(Node):

    NodeName = "antenna_simulator"
    Namespace = namespace.antenna

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = topic.antenna_encoder.publisher(self)
        topic.antenna_speed_cmd.subscription(self, self.antenna_simulator)
        self.enc = AntennaEncoderEmulator()
        self.create_timer(1 / config.antenna.command_frequency, self.stream)

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
    node = AntennaDeviceSimulator()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
