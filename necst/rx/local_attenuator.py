import time
from typing import Dict

from neclib.devices import Attenuator
from necst_msgs.msg import LocalAttenuator
from rclpy.publisher import Publisher

from .. import namespace, topic, config
from ..core import DeviceNode


class LocalAttenuatorController(DeviceNode):
    NodeName = "local_attenuator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.io = Attenuator["local"]()
        self.publisher: Dict[str, Publisher] = {}
        topic.local_attenuator_cmd.subscription(self, self.output_current)
        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)

    def output_current(self, msg: LocalAttenuator) -> None:
        self.io.set_outputrange(ch=msg.ch, outputrange=msg.outputrange)
        self.logger.info(
            f"LocalAttenuator outputrange set to {msg.outputrange} for ch{msg.ch}"
        )

        self.io.output_current(ch=msg.ch, current=msg.current)
        self.logger.info(f"Output current {msg.current} mA for ch{msg.ch}")

    def check_publisher(self) -> None:
        self.publisher["local"] = topic.local_attenuator.publisher(self)

    def stream(self) -> None:
        for key, publisher in self.publisher.items():
            for ch in range(1, 9):
                outputrange = self.io[key].get_outputrange(ch=ch)
                msg = LocalAttenuator(
                    ch=ch, outputrange=outputrange[f"ch{ch}"], time=time.time()
                )
                publisher.publish(msg)
