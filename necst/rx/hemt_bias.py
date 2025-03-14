import time
from typing import Dict

from neclib.devices import HemtBiasReader
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
        self.reader_io = HemtBiasReader()
        self.pub: Dict[str, Publisher] = {}
        self.create_timer(1, self.stream)
        self.logger.info(f"Started {self.NodeName} Node...")
        hemt_channel = [
            id for id in self.reader_io.Config.channel.keys() if id.startswith("hemt")
        ]
        channels = set(map(lambda x: x[:-4], hemt_channel))
        data = self.reader_io.get_all("hemt")
        for ch in channels:
            self.logger.info(
                f"{ch}: {data[ch+'_Vdr']}, "
                f"{data[ch+'_Vg1']}, "
                f"{data[ch+'_Vg2']}, "
                f"{data[ch+'_Idr']}"
            )

    def stream(self) -> None:
        hemt_channel = [
            id for id in self.reader_io.Config.channel.keys() if id.startswith("hemt")
        ]
        channels = set(map(lambda x: x[:-4], hemt_channel))
        data = self.reader_io.get_all("hemt")
        for id in channels:
            v_drain = data[f"{id}_Vdr"].to_value("V").item()
            v_gate1 = data[f"{id}_Vg1"].to_value("V").item()
            v_gate2 = data[f"{id}_Vg2"].to_value("V").item()
            i_drain = data[f"{id}_Idr"].to_value("mA").item()
            msg = HEMTBiasMsg(
                time=time.time(),
                v_drain=v_drain,
                v_gate1=v_gate1,
                v_gate2=v_gate2,
                i_drain=i_drain,
                id=id,
            )
            if id not in self.pub:
                self.pub[id] = topic.hemt_bias[id].publisher(self)
            self.pub[id].publish(msg)
            time.sleep(0.01)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = HEMTBias()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.reader_io.close()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
