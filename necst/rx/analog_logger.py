import time
from typing import Dict

from neclib.devices import AnalogLogger
from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class AnalogLoggerController(DeviceNode):
    NodeName = "analog_logger"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = AnalogLogger()

        self.publisher: Dict[str, Publisher] = {}

        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)
        self.logger.info(f"Started {self.NodeName} Node...")
        self.measure_channel = [
            id
            for id in self.io.Config.channel.keys()
            if not (id.startswith("sis") | id.startswith("hemt"))
        ]
        for key in self.measure_channel:
            self.logger.info(f"{key}: {self.io.get_all(key)[key]}")

    def check_publisher(self) -> None:
        for name in self.measure_channel:
            if name not in self.publisher:
                self.publisher[name] = topic.analog_logger[name].publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            value = self.io.get_all(name)[name].item().value
            msg = DeviceReading(time=time.time(), value=value, id=name)
            publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AnalogLoggerController()
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
