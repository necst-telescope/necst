import rclpy
from neclib.simulators.antenna import AntennaEncoderEmulator
from rclpy.node import Node
from necst_msgs.msg import CoordMsg, TimedAzElFloat64
import time


class AntennaSimulater(Node):

    NodeName = "antenna_simulater"
    Namespace = f"/necst/{config.observatory}/ctrl/antenna"

    def __init__(self):  # はじめに実行される関数。配信や購読、タイマーを生成する。
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = self.create_publisher(CoordMsg, "encorder", 1)
        self.create_subscription(TimedAzElFloat64, "speed", self.antenna_simulater, 1)
        self.enc = AntennaEncoderEmulator()

    def antenna_simulater(self, msg):
        self.enc.command(msg.az, "az")
        self.enc.command(msg.el, "el")
        encorder = self.enc.read()
        az_msg = encorder.az
        el_msg = encorder.el
        msg = CoordMsg(
            lon=az_msg, lat=el_msg, unit="deg", frame="altaz", time=time.time()
        )
        self.publisher.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = AntennaSimulater()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
