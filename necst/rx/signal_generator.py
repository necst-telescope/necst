import time
from typing import Dict

from neclib.devices import SignalGenerator
from necst_msgs.msg import LocalSignal
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class SignalGeneratorController(DeviceNode):
    NodeName = "signal_generator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = SignalGenerator()

        self.publisher: Dict[str, Publisher] = {}
        topic.lo_signal_cmd.subscription(self, self.set_param)

        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)

    def check_publisher(self) -> None:
        for name in self.io.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.lo_signal[name].publisher(self)

    def set_param(self, msg: LocalSignal) -> None:
        if msg.output_status:
            self.io[msg.id].set_freq(GHz=msg.freq)
            self.io[msg.id].set_power(dBm=msg.power)
            self.io[msg.id].start_output()
            self.logger.info(
                f"Set freq = {msg.freq} GHz, power = {msg.power} dBm "
                f"and start output for device {msg.id}"
            )
        else:
            self.io[msg.id].stop_output()
            self.logger.info(f"Stop output in device {msg.id}")

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            freq = self.io[name].get_freq().to_value("GHz").item()
            power = self.io[name].get_power().value.item()
            output_status = self.io[name].get_output_status()
            msg = LocalSignal(
                time=time.time(),
                freq=float(freq),
                power=float(power),
                output_status=bool(output_status),
                id=name,
            )
            publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = SignalGeneratorController()
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
