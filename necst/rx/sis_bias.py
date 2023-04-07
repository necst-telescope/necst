import time
from typing import Dict

from neclib.devices import SisBiasReader, SisBiasSetter
from necst_msgs.msg import SISBias as SISBiasMsg
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class SISBias(DeviceNode):

    NodeName = "sis_bias"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.reader_io = SisBiasReader()
        self.setter_io = SisBiasSetter()

        self.pub: Dict[str, Publisher] = {}

        topic.sis_bias_cmd.subscription(self, self.set_voltage)
        self.create_timer(1, self.stream)

    def stream(self) -> None:
        channels = set(
            map(
                lambda x: x[:-2],
                filter(
                    lambda y: "_" not in y[:-2], self.reader_io.Config.channel.keys()
                ),
            )
        )
        for id in channels:
            current = self.reader_io.get_current(f"{id}_I").to_value("uA").item()
            voltage = self.reader_io.get_voltage(f"{id}_V").to_value("mV").item()
            power = self.reader_io.get_power(f"{id}_P").to_value("mW").item()
            msg = SISBiasMsg(
                time=time.time(), current=current, voltage=voltage, power=power, id=id
            )
            if id not in self.pub:
                self.pub[id] = topic.sis_bias[id].publisher(self)
            self.pub[id].publish(msg)
            time.sleep(0.01)

    def set_voltage(self, msg: SISBiasMsg) -> None:
        self.setter_io.set_voltage(mV=msg.voltage, id=msg.id)
        self.setter_io.apply_voltage()
        self.logger.info(f"Set voltage {msg.voltage} mV for ch {msg.id}")
