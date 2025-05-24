import rclpy
from rclpy.node import Node
from necst_msgs.msg import DeviceReading
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy


class ThermometrSubscrider(Node):
    def __init__(self):
        super().__init__("thermometer_test")

        self.topic_name = "/necst/OMU1P85M/rx/thermometer/Shield40K1"
        self.msg_type = DeviceReading

        qos_profile = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            durability=DurabilityPolicy.VOLATILE,
        )

        self.subscription = self.create_subscription(
            self.msg_type, self.topic_name, self.listener_callback, qos_profile
        )
        self.get_logger().info(
            f"Node '{self.get_name()}' subscribed to topic: {self.topic_name}"
        )

    def listener_callback(self, msg: DeviceReading):
        self.get_logger().info(
            f"Received on '{self.topic_name}': "
            f"ID='{msg.id}', Value={msg.value:.2f} K, OK={msg.is_ok}"
        )


def main(args=None):
    rclpy.init(args=args)

    minimal_node = ThermometrSubscrider()

    try:
        rclpy.spin(minimal_node)
    except KeyboardInterrupt:
        minimal_node.get_logger().info("KeyboardInterrupt, shutting down...")
    finally:
        if minimal_node and rclpy.ok() and not minimal_node.is_shutdown:
            minimal_node.destroy_node()
        if rclpy.ok():
            rclpy.try_shutdown()


if __name__ == "__main__":
    main()
