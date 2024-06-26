import rclpy
from rclpy.executors import MultiThreadedExecutor

from ..ctrl.antenna import AntennaEncoderController, AntennaMotor, WeatherStationReader
from ..ctrl.calibrator import ChopperController
from ..ctrl.mirror import M2Controller, M4Controller


def configure_executor() -> MultiThreadedExecutor:
    executor = MultiThreadedExecutor()
    nodes = [
        AntennaEncoderController(),
        AntennaMotor(),
        WeatherStationReader(),
        ChopperController(),
        M2Controller(),
        M4Controller(),
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
        _ = [n.destroy_node() for n in executor.get_nodes()]
        executor.shutdown()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
