import time

from neclib.devices import DomeEncoder
from necst_msgs.msg import CoordMsg

from ... import namespace, topic
from ...core import DeviceNode


class DomeEncoderController(DeviceNode):
    NodeName = "dome_encoder_readout"
    Namespace = namespace.dome

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = topic.dome_encoder.publisher(self)
        self.encoder = DomeEncoder()
        self.create_timer(1 / 15, self.stream)

    def stream(self) -> None:
        readings = self.encoder.get_reading()
        msg = CoordMsg(
            lon=readings.to_value("deg").item(),
            unit="deg",
            frame="altaz",
            time=time.time(),
        )
        self.publisher.publish(msg)


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
