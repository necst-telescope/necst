from rclpy.node import Node


class DeviceNode(Node):
    """Device handling node, which knows procedures required to control devices."""

    def create_safe_timer(self, period, callback):
        def safe_callback():
            try:
                callback()
            except Exception as e:
                self.get_logger().error(f"Exception in {self.get_name()}: {e}")
                self.destroy_node()

        return self.create_timer(period, safe_callback)

    def create_safe_subscription(self, topic, callback):
        def safe_callback(msg):
            try:
                callback(msg)
            except Exception as e:
                self.get_logger().error(f"Exception in {self.get_name()}: {e}")
                self.destroy_node()

        return topic.subscription(self, safe_callback)

    def destroy_node(self):
        """Override destroy_node to ensure that the device is finalized.

        This implementation assumes the device controllers are registered as instance
        variables. In other cases, you should invoke `finalize` method manually.

        """
        for attrname in dir(self):
            attr = getattr(self, attrname)
            if hasattr(attr, "finalize"):
                attr.finalize()
        super().destroy_node()
