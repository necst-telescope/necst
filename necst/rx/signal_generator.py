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
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)

            self.logger = self.get_logger()
            self.io = SignalGenerator()

            self.publisher: Dict[str, Publisher] = {}
            self.create_safe_subscription(topic.lo_signal_cmd, self.set_param)
            self.create_safe_timer(1, self.stream)
            self.create_safe_timer(1, self.check_publisher)
            self.create_safe_timer(1, self.check_status)
            self.logger.info(f"Started {self.NodeName} Node...")
            for key in self.io.keys():
                self.logger.info(
                    f"{key}: {self.io[key].get_power()}, "
                    f"{self.io[key].get_freq()}, "
                    f"Output is {self.io[key].get_output_status()}"
                )
        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

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

    def check_status(self):
        for name in self.io.keys():
            if self.io[name].check_reference_status():
                pass
            else:
                self.logger.warning("Warning: Internal reference selected")


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = SignalGeneratorController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        _ = [node.io[key].close() for key in node.io.keys()]
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
