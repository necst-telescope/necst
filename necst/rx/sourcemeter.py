import time
from typing import List, Dict, Optional

from neclib.devices import SisBiasReader
from necst_msgs.msg import SISBias as SISBiasMsg
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class Sourcemeter(DeviceNode):
    NodeName = "sourcemeter"
    Namespace = namespace.rx

    def __init__(self) -> None:
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)
            self.logger = self.get_logger()

            self.reader_io: Dict[Optional[str], object] = SisBiasReader()
            # self.setter_io = SisBiasSetter()

            self.pub: Dict[str, Publisher] = {}
            self.create_safe_subscription(topic.sis_bias_cmd, self.set_voltage)
            self.create_safe_timer(1, self.stream)
            self.logger.info(f"Started {self.NodeName} Node...")

            # self.keys = ["sis_USB", "sis_LSB"]
            self.keys = list(self.reader_io.Config.keys())

            """
            data = {
            "sis_USB": {
                'sis_USB_V': <Quantity -1.129927e-06 mV>,
                'sis_USB_I': <Quantity 2.449562e-06 uA>
                },
            "sis_LSB": {
                'sis_LSB_V': <Quantity -1.129927e-06 mV>,
                'sis_LSB_I': <Quantity 2.449562e-06 uA>
                }
            }
            """
            data: Dict[str, Dict] = self.reader_io.get_all(target="sis")
            for id, data_dict in data.items():
                self.logger.info(f"{id}: {data_dict[id+'_V']}, {data_dict[id+'_I']}")
        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def stream(self) -> None:
        data: Dict[str, float] = self.reader_io.get_all(target="sis")
        for id, data_dict in data.items():
            current = data_dict[f"{id}_I"].to_value("uA").item()
            voltage = data_dict[f"{id}_V"].to_value("mV").item()
            msg = SISBiasMsg(
                time=time.time(), current=current, voltage=voltage, id=[id]
            )
            if id not in self.pub:
                self.pub[id] = topic.sis_bias[id].publisher(self)
            self.pub[id].publish(msg)
            time.sleep(0.01)

    def set_voltage(self, msg: SISBiasMsg) -> None:
        id: List = msg.id
        if msg.finalize:
            self.reader_io.finalize()
            return
        elif id[0] in self.keys:
            self.reader_io[id[0]].set_voltage(mV=msg.voltage)
            self.logger.info(f"Set voltage {msg.voltage} mV for ch {msg.id}")
            time.sleep(0.01)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = Sourcemeter()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.reader_io.close()
        _ = [
            node.reader_io[key].close() if None not in key else node.reader_io.close()
            for key in node.reader_io.keys()
        ]
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
