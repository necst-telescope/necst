import rclpy
from rclpy.executors import MultiThreadedExecutor

from ..ctrl.dome import DomePIDController, DomeController, DomeTrackingStatus


def configure_executor() -> MultiThreadedExecutor:
    executor = MultiThreadedExecutor()
    nodes = [
        DomePIDController(),
        DomeController(),
        DomeTrackingStatus(),
    ]
    _ = [executor.add_node(n) for n in nodes]
    return executor


def main(args=None) -> None:
    rclpy.init(args=args)

    executor = configure_executor()

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()
        _ = [n.destroy_node() for n in executor.get_nodes()]
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
