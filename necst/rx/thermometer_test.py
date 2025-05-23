import rclpy
from necst_msgs.msg import DeviceReading
from .. import namespace, topic
from ..core import DeviceNode


class ThermometerSubscriber(DeviceNode):
    NodeName = "thermometer_test"
    Namespace = namespace.rx

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.logger = self.get_logger()
        self.logger.info("Started ThermometerTest...")

        self.channels = list(self.io.Config.thermometer.channel.keys())

        for ch in self.channels:
            topic_name = topic.thermometer[ch].fullname
            self.create_safe_subscription(
                DeviceReading,
                topic_name,
                self._make_callback(ch),
                qos=10,
            )

    def _make_callback(self, ch: str):
        def _cb(msg: DeviceReading):
            self.logger.info(f"[{ch}] value={msg.value:.2f}  time={msg.time:.6f}")

        return _cb


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerSubscriber()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
