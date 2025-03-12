import time
from typing import Dict

from neclib.devices import AnalogLogger
from necst_msgs.msg import DeviceReading
from necst_msgs.msg import SISBias as SISBiasMsg
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
        self.sis_channel = [
            id for id in self.io.Config.channel.keys() if id.startswith("sis")
        ]
        self.hemt_channel = [
            id for id in self.io.Config.channel.keys() if id.startswith("hemt")
        ]
        self.other_channel = [
            id
            for id in self.io.Config.channel.keys()
            if not (id.startswith("sis") | id.startwith("hemt"))
        ]
        self.publisher: Dict[str, Publisher] = {}

        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)
        if len(self.sis_channel) != 0:
            from neclib.devices import SisBiasSetter

            self.setter_io = SisBiasSetter()
            topic.sis_bias_cmd.subscription(self, self.set_voltage)
        self.logger.info(f"Started {self.NodeName} Node...")
        time.sleep(0.5)
        for key in self.measure_channel:
            self.logger.info(f"{key}: {self.io.get_all(key)[key]}")

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

    def check_publisher(self) -> None:
        for name in self.measure_channel:
            if name not in self.publisher:
                self.publisher[name] = topic.analog_logger[name].publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            value = self.io.get_all(name)[name].item().value
            msg = DeviceReading(time=time.time(), value=value, id=name)
            publisher.publish(msg)


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
