import rclpy
from rclpy.executors import MultiThreadedExecutor

from .antenna import AntennaController


from rclpy.node import Node
from necst import config
from necst_msgs.msg import CoordMsg


class AntennaSimulator(Node):
    Namespace = f"/necst/{config.observatory}/ctrl/antenna"
    NodeName = "antenna_simulator"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.pub = self.create_publisher(CoordMsg, "encoder", 1)
        self.create_subscription(CoordMsg, "altaz", self.respond, 1)

    def respond(self, msg: CoordMsg) -> None:
        assert msg.frame == "altaz"
        assert msg.unit == "deg"
        self.pub.publish(msg)


def main(args=None) -> None:
    rclpy.init(args=args)

    executor = MultiThreadedExecutor()
    nodes = [
        AntennaController(),
        AntennaSimulator(),  # TODO: Replace with high-definition simulator
    ]
    _ = [executor.add_node(n) for n in nodes]
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()
        _ = [n.destroy_node() for n in nodes]
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
