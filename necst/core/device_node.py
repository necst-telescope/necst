from rclpy.node import Node


class DeviceNode(Node):
    """Device handling node, which knows procedures required to control devices."""
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
