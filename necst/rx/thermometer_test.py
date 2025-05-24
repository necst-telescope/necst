import rclpy
from rclpy.node import Node
from necst_msgs.msg import DeviceReading
from rclpy.qos import qos_profile_sensor_data


class ThermometerSubscriber(Node):
    def __init__(self):
        super().__init__("thermometer_test")

        topic_to_subscribe = "/necst/OMU1P85M/rx/thermometer/Shield40K1"

        self.subscription = self.create_subscription(
            DeviceReading,
            topic_to_subscribe,
            self.listener_callback,
            qos_profile_sensor_data,
        )

    def listener_callback(self, msg: DeviceReading):
        print(
            f"Received - ID: {msg.id}, Value: {msg.value:.2f} K, Time: {msg.time:.0f}"
        )


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerSubscriber()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if node and rclpy.ok() and not node.is_shutdown:
            node.destroy_node()
        if rclpy.ok():
            rclpy.try_shutdown()


if __name__ == "__main__":
    main()
