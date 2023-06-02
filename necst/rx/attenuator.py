import time
from typing import Dict

from neclib.devices import Attenuator
from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher

from .. import namespace, topic, config
from ..core import DeviceNode


class AttenuatorController(DeviceNode):

    NodeName = "attenuator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.io = Attenuator()
        self.publisher: Dict[str, Publisher] = {}
        topic.attenuator_cmd.subscription(self, self.set_loss)
        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)

    def set_loss(self, msg: DeviceReading) -> None:
        self.io.set_loss(dB=int(msg.value), id=msg.id)
        self.logger.info(f"Attenuator loss set to {msg.value} dB for device {msg.id}")

    def check_publisher(self) -> None:
        for key in self.io.keys():
            for name in config.attenuator[key].channel.keys():
                if key not in self.publisher:
                    self.publisher[f"{key}" + "." + f"{name}"] \
        = topic.attenuator[f"{key}" + "." + f"{name}"].publisher(self)

    def stream(self) -> None:
        for key, publisher in self.publisher.items():
            loss = self.io[key].get_loss(id=key.rsplit(".")[1]).value.item()
            msg = DeviceReading(id=key, value=float(loss), time=time.time())
            publisher.publish(msg)
