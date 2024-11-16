import time
from typing import Dict

from neclib.devices import Attenuator
from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class AttenuatorController(DeviceNode):
    NodeName = "attenuator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.io = Attenuator()
        self.publisher: Dict[str, Publisher] = {}
        topic.attenuator_cmd.subscription(self, self.set_loss)
        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)
        self.logger.info(f"Started {self.NodeName} Node...\nStatus:")
        for key in self.io.keys():
            for ch in self.io[key].Config.channel.keys():
                self.logger.info(f"{key}, {ch}: {self.io.get_loss(id = ch)}")

    def set_loss(self, msg: DeviceReading) -> None:
        keys = self.io.keys()
        if None in keys:
            self.io.set_loss(dB=int(msg.value), id=msg.id)
        else:
            for key in keys:
                ch = self.io[key].Config.channel.keys()
                if msg.id in ch:
                    self.io[key].set_voltage(dB=msg.value, id=msg.id)
                    break
                else:
                    continue
        time.sleep(0.01)

    def check_publisher(self) -> None:
        for key in self.io.keys():
            for name in self.io[key].Config.channel.keys():
                if key not in self.publisher:
                    self.publisher[f"{key}" + "." + f"{name}"] = topic.attenuator[
                        f"{key}" + "." + f"{name}"
                    ].publisher(self)

    def stream(self) -> None:
        for key, publisher in self.publisher.items():
            loss = self.io[key].get_loss(id=key.rsplit(".")[1]).value.item()
            msg = DeviceReading(id=key, value=float(loss), time=time.time())
            publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AttenuatorController()
    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass
    finally:
        node.logger.info(f"Killing {node.NodeName} Node...")
        _ = [node.io[key].close() for key in node.io.keys()]
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
