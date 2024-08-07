import time

from neclib.devices import DomeEncoder
from necst_msgs.msg import CoordMsg, DomeLimit

from ... import namespace, topic, config
from ...core import DeviceNode


class DomeEncoderController(DeviceNode):
    NodeName = "dome_encoder_readout"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.encoder_publisher = topic.dome_encoder.publisher(self)
        self.dome_publisher = topic.dome_limit_cmd.publisher(self)

        self.encoder = DomeEncoder()

        self.create_timer(1 / config.dome_enc_frequency, self.stream)

    def dome_limit(self):
        msg = DomeLimit(check=True, limit=0)
        self.dome_publisher.publish(msg)
        return

    def stream(self) -> None:
        self.dome_limit()
        readings = self.encoder.get_dome_reading()
        msg = CoordMsg(
            lon=readings.to_value("deg"),
            unit="deg",
            frame="altaz",
            time=time.time(),
        )
        self.encoder_publisher.publish(msg)


class DomeLimitController(DeviceNode):
    NodeName = "dome_limit_checker"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.encoder = DomeEncoder()
        topic.dome_limit.subscription(self, self.set_counter)

    def set_counter(self, msg: DomeLimit):
        limit = msg.limit

        if limit != 0:
            self.encoder.dome_set_counter(limit)
        # self.get_count()
        return limit


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = DomeEncoderController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
