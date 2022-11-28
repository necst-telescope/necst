import rclpy
from rclpy.executors import MultiThreadedExecutor

from .alert import AntennaDriveRangeAlert, AntennaSpeedAlert


def get_executor() -> MultiThreadedExecutor:
    executor = MultiThreadedExecutor()

    nodes = [
        AntennaDriveRangeAlert(),
        AntennaSpeedAlert(),
    ]

    [executor.add_node(node) for node in nodes]
    return executor


def main(args=None):
    rclpy.init(args=args)

    executor = get_executor()
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        [n.destroy_node() for n in executor.get_nodes()]
        executor.shutdown()
        rclpy.try_shutdown()
