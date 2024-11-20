import time
from typing import Dict

from neclib.devices import LocalAttenuator
from necst_msgs.msg import LocalAttenuatorMsg
from rclpy.publisher import Publisher

from .. import namespace, topic
from ..core import DeviceNode


class LocalAttenuatorController(DeviceNode):
    NodeName = "local_attenuator"
    Namespace = namespace.rx

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.io = LocalAttenuator()
        self.publisher: Dict[str, Publisher] = {}
        topic.local_attenuator_cmd.subscription(self, self.output_current)
        self.create_timer(1, self.stream)
        self.create_timer(1, self.check_publisher)
        self.logger.info(f"Started {self.NodeName} Node...")

    def output_current(self, msg: LocalAttenuatorMsg) -> None:
        if msg.finalize:
            self.io.finalize()
            return
        for id in msg.id:
            self.io.set_current(id=id, mA=msg.current)
        self.io.apply_current()
        self.logger.info(f"Set current {msg.current} mA for ch {msg.id}")
        time.sleep(0.01)

    def check_publisher(self) -> None:
        for name in self.io.Config.channel.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.local_attenuator.publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            for id in self.io.Config.local_attenuator.channel.keys():
                outputrange = self.io.get_outputrange(id)
                msg = LocalAttenuatorMsg(
                    id=[id], outputrange=str(outputrange), time=time.time()
                )
                publisher.publish(msg)
                time.sleep(0.01)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = LocalAttenuatorController()
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
