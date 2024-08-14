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
        sis_channel = [
            id for id in self.reader_io.Config.channel.keys() if id.startswith("sis")
        ]
        channels = set(map(lambda x: x[:-2], sis_channel))
        for id in channels:
            current = self.reader_io.get_current(f"{id}_I").to_value("uA").item()
            voltage = self.reader_io.get_voltage(f"{id}_V").to_value("mV").item()
            msg = SISBiasMsg(
                time=time.time(), current=current, voltage=voltage, id=[id]
            )
            if id not in self.pub:
                self.pub[id] = topic.sis_bias[id].publisher(self)
            self.pub[id].publish(msg)
            time.sleep(0.01)

    def set_voltage(self, msg: SISBiasMsg) -> None:
        if msg.finalize:
            self.setter_io.finalize()
            return
        else:
            keys = self.setter_io.keys()
            if None in keys:
                for id in msg.id:
                    self.setter_io.set_voltage(mV=msg.voltage, id=id)
                self.setter_io.apply_voltage()
            else:
                for id in msg.id:
                    for key in keys:
                        ch = self.setter_io[key].Config.channel.keys()
                        if id in ch:
                            self.setter_io[key].set_voltage(mV=msg.voltage, id=id)
                        break
                for key in keys:
                    self.setter_io[key].apply_voltage()
                    time.sleep(0.01)
            self.logger.info(f"Set voltage {msg.voltage} mV for ch {msg.id}")
            time.sleep(0.01)
