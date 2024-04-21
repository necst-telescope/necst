import time
from typing import Dict

from neclib.devices import LocalAttenuator
from necst_msgs.msg import LocalAttenuatorMsg
from rclpy.publisher import Publisher

from .. import namespace, topic, config
from ..core import DeviceNode


class LocalAttenuatorController(DeviceNode):
    NodeName = "local_attenuator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.io = LocalAttenuator()
        self.publisher: Dict[str, Publisher] = {}
        topic.local_attenuator_cmd.subscription(self, self.output_current)
        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)

    def output_current(self, msg: LocalAttenuatorMsg) -> None:
        if msg.finalize:
            self.io.finalize()
            return
        self.io.output_current(id=msg.id, current=msg.current)
        self.logger.info(f"Output current {msg.current} mA for ch{msg.ch}")

    def check_publisher(self) -> None:
        for name in self.io.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.local_attenuator.publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            for id in self.io.config.local_attenuator.channel.keys():
                outputrange = self.io.get_outputrange(id)
                msg = LocalAttenuatorMsg(
                    id=id, outputrange=outputrange, time=time.time()
                )
                publisher.publish(msg)
