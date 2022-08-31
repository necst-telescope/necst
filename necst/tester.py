import rclpy
from std_msgs.msg import String

from .core import PrivilegedNode


class Tester(PrivilegedNode):

    NodeName = "tester"

    def __init__(self) -> None:
        super().__init__()
        self.pub_a = self.create_publisher(String, "/test_a", 1)
        self.pub_b = self.create_publisher(String, "/test_b", 1)
        self.create_timer(3, self.a)
        self.create_timer(5, self.b)

    def a(self) -> None:
        self.logger.info("A is called")
        msg = String(data="a")
        self.pub_a.publish(msg)

    @PrivilegedNode.require_privilege
    def b(self) -> None:
        self.logger.info("B (privileged) is called")
        msg = String(data="b")
        self.pub_b.publish(msg)


def main(args=None):
    from .core import Authorizer
    from rclpy.executors import MultiThreadedExecutor

    rclpy.init(args=args)
    node = Tester()
    server = Authorizer()
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    executor.add_node(server)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()
        node.destroy_node()
        server.destroy_node()

        try:
            rclpy.shutdown()
        except:  # noqa: E722
            pass


if __name__ == "__main__":
    main()
