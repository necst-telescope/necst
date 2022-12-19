from typing import Tuple

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node

from ..core import Recorder
from ..rx.spectrometer import SpectralData


def configure_executor() -> Tuple[SingleThreadedExecutor, Node]:
    executor = SingleThreadedExecutor()
    nodes = [
        SpectralData(),
    ]
    _ = [executor.add_node(n) for n in nodes]
    return executor, Recorder()


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
