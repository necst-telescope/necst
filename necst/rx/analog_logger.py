import time
from typing import Dict

from neclib.devices import AnalogLogger
from necst_msgs.msg import DeviceReading
from necst_msgs.msg import HEMTBias as HEMTBiasMsg
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class AnalogLoggerController(DeviceNode):
    NodeName = "analog_logger"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = AnalogLogger()
        self.measure_channel = [
            id for id in self.io.Config.channel.keys() if not id.startswith("sis")
        ]
        self.hemt_channel = [id for id in self.measure_channel if id.startswith("hemt")]
        self.other_channel = [
            id for id in self.measure_channel if not id.startwith("hemt")
        ]
        self.publisher: Dict[str, Publisher] = {}

        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)
        self.logger.warning("SIS Tuning and Measuring are no implemented in this Node.")
        self.logger.warning("Please use `sis_bias` Node to control SIS Bias.")
        # TODO: Implement SIS Control. Just measuring Bias Parameters...
        self.logger.info(f"Started {self.NodeName} Node...")
        time.sleep(0.5)
        for key in self.measure_channel:
            self.logger.info(f"{key}: {self.io.get_from_id(key)}")
            time.sleep(0.01)

    def check_publisher(self) -> None:
        for name in self.other_channel:
            if name not in self.publisher:
                self.publisher[name] = topic.analog_logger[name].publisher(self)
        for name in self.hemt_channel:
            if name not in self.publisher:
                self.publisher[name] = topic.hemt_bias[name].publisher(self)

    def stream(self) -> None:
        channels = set(map(lambda x: x[:-4], self.hemt_channel))
        data = self.reader_io.get_all("hemt")
        if len(self.hemt_channel) != 0:
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
                self.publisher[id].publish(msg)
                time.sleep(0.01)
        if len(self.other_channel) != 0:
            for name in self.other_channel:
                value = self.io.get_from_id(name).item()
                msg = DeviceReading(time=time.time(), value=value, id=name)
                self.publisher[name].publish(msg)
                time.sleep(0.01)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AnalogLoggerController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.io.close()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
