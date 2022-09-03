import rclpy
from std_msgs.msg import String

from .core import PrivilegedNode


class Tester(PrivilegedNode):

    NodeName = "tester"

    def __init__(self) -> None:
        super().__init__(self.NodeName)
        self.pub_a = self.create_publisher(String, "/test_a", 1)
        self.pub_b = self.create_publisher(String, "/test_b", 1)
        self.create_timer(1, self.a)
        self.create_timer(1, self.b)
        self.create_subscription(
            String, "/test_s", lambda msg: self.logger.info(msg.data), 1
        )

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
    import traceback
    from .core import Authorizer
    from rclpy.executors import SingleThreadedExecutor

    rclpy.init(args=args)
    node = Tester()
    server = Authorizer()
    executor = SingleThreadedExecutor()
    executor.add_node(node)
    executor.add_node(server)

    try:
        assert node.get_privilege(), "privilege isn't granted"
        executor.spin()
    except KeyboardInterrupt:
        node.logger.info("Exiting, due to shutdown signal.")
    except AssertionError:
        node.logger.warning(f"Exiting, due to :\n{traceback.format_exc()}")
    finally:
        [n.destroy_node() for n in executor.get_nodes()]
        executor.shutdown()

        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
