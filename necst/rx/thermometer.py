import time
from typing import Dict

from neclib.devices import Thermometer
from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class ThermometerController(DeviceNode):
    NodeName = "thermometer"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = Thermometer()

        self.publisher: Dict[str, Publisher] = {}

        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)
        self.logger.info(f"Started {self.NodeName} Node...")
        for key in self.io.Config.thermometer.channel.keys():
            self.logger.info(f"{key}: {self.io.get_temp(key)}")

    def check_publisher(self) -> None:
        for name in self.io.Config.thermometer.channel.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.thermometer[name].publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            temperature = self.io.get_temp(name).to_value("K").item()
            msg = DeviceReading(time=time.time(), value=temperature, id=name)
            publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = ThermometerController()
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
