import time
from typing import Dict

from neclib.devices import PowerMeter
from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class PowermeterController(DeviceNode):
    NodeName = "powermeter"
    Namespace = namespace.rx

    def __init__(self) -> None:
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)

            self.logger = self.get_logger()
            self.io = PowerMeter()
            self.publisher: Dict[str, Publisher] = {}
            self.create_timer(1, self.stream)
            self.create_timer(1, self.check_publisher)
            self.logger.info(f"Started {self.NodeName} Node...")
        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def check_publisher(self) -> None:
        for name in self.io.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.powermeter.publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            power = self.io.get_power().to_value("dBm").item()

            msg = DeviceReading(time=time.time(), value=power, id="")
            publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = PowermeterController()
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
