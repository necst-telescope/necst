import rclpy
from rclpy.node import Node

from . import Authorizer


def configure_server() -> Node:
    node = Authorizer()
    return node


def main(args=None) -> None:
    rclpy.init(args=args)

    node = configure_server()
    executor = rclpy.get_global_executor()

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        executor.shutdown()
        _ = [n.destroy_node() for n in executor.get_nodes()]
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
