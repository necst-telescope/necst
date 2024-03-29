from typing import Tuple

import rclpy
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node

from ..core import RecorderController
from ..rx.spectrometer import SpectralData


def configure_executor() -> Tuple[MultiThreadedExecutor, Node]:
    executor = MultiThreadedExecutor()
    nodes = [
        SpectralData(),
    ]
    _ = [executor.add_node(n) for n in nodes]
    return executor, RecorderController()


def main(args=None) -> None:
    rclpy.init(args=args)

    executor, node = configure_executor()

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()
        node.destroy_node()
        _ = [n.destroy_node() for n in executor.get_nodes()]
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
