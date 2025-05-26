import rclpy
from necst_msgs.msg import DeviceReading
from ..core import DeviceNode
from .. import qos


class ThermometerSubscriber(DeviceNode):
    def __init__(self):
        super().__init__("thermometer_test")

        topic_name = "/necst/OMU1P85M/rx/thermometer/Shield40K1"

        self.subscription = self.create_subscription(
            DeviceReading,
            topic_name,
            self.listener_callback,
            qos.realtime,
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
        node.io.close()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
