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
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)
            self.logger = self.get_logger()

            self.reader_io = SisBiasReader()
            self.setter_io = SisBiasSetter()

            self.pub: Dict[str, Publisher] = {}
            self.create_safe_subscription(topic.sis_bias_cmd, self.set_voltage)
            self.create_safe_timer(0.25, self.stream)
            self.logger.info(f"Started {self.NodeName} Node...")
            sis_channel = [
                id
                for id in self.reader_io.Config.channel.keys()
                if id.startswith("sis")
            ]
            channels = set(map(lambda x: x[:-2], sis_channel))
            data = self.reader_io.get_all(target="sis")
            for ch in channels:
                self.logger.info(f"{ch}: {data[ch+'_V']}, {data[ch+'_I']}")
        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def stream(self) -> None:
        sis_channel = [
            id for id in self.reader_io.Config.channel.keys() if id.startswith("sis")
        ]
        channels = set(map(lambda x: x[:-2], sis_channel))
        data = self.reader_io.get_all(target="sis")
        for id in channels:
            current = data[f"{id}_I"].to_value("uA").item()
            voltage = data[f"{id}_V"].to_value("mV").item()
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
            else:
                for id in msg.id:
                    for key in keys:
                        ch = self.setter_io[key].Config.channel.keys()
                        if id in ch:
                            self.setter_io[key].set_voltage(mV=msg.voltage, id=id)
                            break
                        else:
                            continue
            self.setter_io.apply_voltage()
            self.logger.info(f"Set voltage {msg.voltage} mV for ch {msg.id}")
            time.sleep(0.01)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = SISBias()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.reader_io.close()
        _ = [
            node.setter_io[key].close() if None not in key else node.setter_io.close()
            for key in node.setter_io.keys()
        ]
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
