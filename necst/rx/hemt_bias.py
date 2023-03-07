import time
from typing import Dict

from neclib.devices import BiasReader
from necst_msgs.msg import HEMTBias as HEMTBiasMsg
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class HEMTBias(DeviceNode):
    NodeName = "hemt_bias"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()

        self.reader_io = BiasReader()

        self.pub: Dict[str, Publisher] = {}

        self.create_timer(1, self.stream)

    def stream(self) -> None:
        channels = set(map(lambda x: x[:-4], self.reader_io.Config.channel.keys()))
        for id in channels:
            v_drain = self.reader_io.get_voltage(f"{id}_Vdr").to_value("V").item()
            v_gate1 = self.reader_io.get_voltage(f"{id}_Vg1").to_value("V").item()
            v_gate2 = self.reader_io.get_voltage(f"{id}_Vg2").to_value("V").item()
            msg = HEMTBiasMsg(
                time=time.time(),
                v_drain=v_drain,
                v_gate1=v_gate1,
                v_gate2=v_gate2,
                id=id,
            )
            if id not in self.pub:
                self.pub[id] = topic.sis_bias[id].publisher(self)
            self.pub[id].publish(msg)
            time.sleep(0.01)
