import rclpy
from rclpy.executors import MultiThreadedExecutor

from ..rx.attenuator import AttenuatorController
from ..rx.hemt_bias import HEMTBias
from ..rx.local_attenuator import LocalAttenuatorController
from ..rx.signal_generator import SignalGeneratorController
from ..rx.sis_bias import SISBias
from ..rx.thermometer import ThermometerController
from ..rx.vacuum_gauge import VacuumGaugeController


def configure_executor() -> MultiThreadedExecutor:
    executor = MultiThreadedExecutor()
    nodes = [
        AttenuatorController(),
        HEMTBias(),
        LocalAttenuatorController(),
        SignalGeneratorController(),
        SISBias(),
        ThermometerController(),
        VacuumGaugeController(),
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
