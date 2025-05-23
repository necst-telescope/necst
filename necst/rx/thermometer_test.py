import rclpy
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy

from .. import namespace
from ..core import DeviceNode
from necst_msgs.msg import DeviceReading
from neclib.devices import Thermometer


class ThermometerSubscriber(DeviceNode):
    NodeName = "thermometer_test"
    Namespace = namespace.rx

    def __init__(self):
        super().__init__(self.NodeName, namespace=self.Namespace)

        self.logger = self.get_logger()
        self.io = Thermometer()
        topic_name = "/necst/OMU1P85M/rx/thermometer/Shield40K1"

        qos_profile_subscriber = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            durability=DurabilityPolicy.VOLATILE,
        )

        self.create_subscription(
            DeviceReading, topic_name, self.callback, qos_profile_subscriber
        )

    def callback(self, msg):
        self.get_logger().info(f"Received message: {msg.id} - {msg.value} K")


def main():
    rclpy.init()
    node = ThermometerSubscriber()
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
