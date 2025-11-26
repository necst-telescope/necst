import rclpy
from rclpy.executors import SingleThreadedExecutor

from ..rx.sis_bias import SISBias
from ..rx.sourcemeter import Sourcemeter
from .. import config


def configure_executor() -> SingleThreadedExecutor:
    executor = SingleThreadedExecutor()
    try:
        if config.sis_bias_reader.sis_USB._ == "KEITHLEY2450":
            nodes = [
                Sourcemeter(),
            ]
    except AttributeError:
        nodes = [
            SISBias(),
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
