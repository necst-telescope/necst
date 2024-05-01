import time

from neclib.devices import AntennaEncoder
from necst_msgs.msg import CoordMsg

from ... import namespace, topic
from ...core import DeviceNode


class AntennaEncoderController(DeviceNode):

    NodeName = "encoder_readout"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = topic.antenna_encoder.publisher(self)
        self.encoder = AntennaEncoder()
        self.create_timer(1 / 15, self.stream)

    def stream(self) -> None:
        record_time = time.time()
        readings = self.encoder.get_reading()
        msg = CoordMsg(
            lon=readings["az"].to_value("deg").item(),
            lat=readings["el"].to_value("deg").item(),
            unit="deg",
            frame="altaz",
            time=record_time,
        )
        self.publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AntennaEncoderController()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
