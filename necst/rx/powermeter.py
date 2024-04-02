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
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = PowerMeter()

        self.publisher: Dict[str, Publisher] = {}

        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)

    def check_poblisher(self) -> None:
        for name in self.io.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.powermeter[name].publisher(self)

    def stream(self) -> None:
        for (
            name,
            publisher,
        ) in self.publisher.items():
            power = self.io.get_power(name).item()
            msg = DeviceReading(time=time.time(), value=power, id=name)
            publisher.publish(msg)
