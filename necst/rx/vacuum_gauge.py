import time
from typing import Dict

from neclib.devices import VacuumGauge
from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class VacuumGaugeController(DeviceNode):
    NodeName = "vacuum_gauge"
    Namespace = namespace.rx

    def __init__(self) -> None:
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)

            self.logger = self.get_logger()
            self.io = VacuumGauge()

            self.publisher: Dict[str, Publisher] = {}

            self.create_safe_timer(1, self.stream)
            self.create_safe_timer(1, self.check_publisher)
            self.logger.info(f"Started {self.NodeName} Node...")
            self.logger.info(f"{self.io.get_pressure()}")
        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def check_publisher(self) -> None:
        for name in self.io.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.vacuum_gauge.publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            pressure = self.io.get_pressure().to_value("Torr")

            msg = DeviceReading(time=time.time(), value=pressure, id="")
            publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = VacuumGaugeController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.io.close()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
