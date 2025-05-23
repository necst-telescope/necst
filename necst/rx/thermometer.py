import time
from typing import Dict
import functools

from necst_msgs.msg import DeviceReading
from rclpy.publisher import Publisher
from rclpy.subscription import Subscription
import rclpy

from neclib.devices import Thermometer
from .. import namespace, topic
from ..core import DeviceNode


class ThermometerController(DeviceNode):
    NodeName = "thermometer"
    Namespace = namespace.rx

    def __init__(self) -> None:
        try:
            super().__init__(self.NodeName, namespace=self.Namespace)

            self.logger = self.get_logger()
            self.io = Thermometer()

            self.publisher: Dict[str, Publisher] = {}
            self.channel_subscriptions: Dict[str, Subscription] = {}

            configured_channels = list(self.io.Config.thermometer.channel.keys())
            if not configured_channels:
                self.logger.warn("No thermometer channels configured in self.io.Config. Not creating channel subscriptions.")

            for key in configured_channels:
                subscribed_topic_name = f"subscribed_channels/{key}/device_reading"

                self.channel_subscriptions[key] = self.create_subscription(
                    DeviceReading,
                    subscribed_topic_name,
                    functools.partial(self.multi_channel_device_reading_callback, channel_id=key),
                    10
                )
                self.logger.info(
                    f"Subscribing to topic '{self.channel_subscriptions[key].topic_name}' for channel ID '{key}'"
                )

            self.create_safe_timer(1, self.stream)
            self.create_safe_timer(1, self.check_publisher)
            self.logger.info(f"Started {self.NodeName} Node...")
            for key in configured_channels:
                self.logger.info(f"Initial value for {key}: {self.io.get_temp(key)}")

        except Exception as e:
            self.logger.error(f"{self.NodeName} Node is shutdown due to Exception: {e}")
            self.destroy_node()

    def multi_channel_device_reading_callback(self, msg: DeviceReading, channel_id: str) -> None:
        self.logger.info(
            f"Received for channel ID '{channel_id}' (on topic '{self.channel_subscriptions[channel_id].topic_name}'): "
            f"Msg_ID='{msg.id}', Value={msg.value:.2f}, Time={msg.time:.0f}"
        )

    def check_publisher(self) -> None:
        for name in self.io.Config.thermometer.channel.keys():
            if name not in self.publisher:
                self.publisher[name] = topic.thermometer[name].publisher(self)

    def stream(self) -> None:
        for name, publisher in self.publisher.items():
            temperature = self.io.get_temp(name).to_value("K").item()
            msg = DeviceReading(time=time.time(), value=temperature, id=name)
            publisher.publish(msg)


def main(args=None):
    rclpy.init(args=args)
    node = ThermometerController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if hasattr(node, 'io') and node.io:
            node.io.close()
        if rclpy.ok() and node and not node.is_shutdown:
            node.destroy_node()
        if rclpy.ok():
            rclpy.try_shutdown()


if __name__ == "__main__":
    main()
