import time

from neclib.devices import AntennaEncoder as AntennaEncoderDevice
from rclpy.node import Node

from necst_msgs.msg import CoordMsg
from ... import namespace, qos


class AntennaEncoder(Node):

    NodeName = "encoder_readout"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = self.create_publisher(CoordMsg, "encoder", qos.realtime)
        self.encoder = AntennaEncoderDevice()
        self.create_timer(1 / 50, self.stream)  # TODO: Parametrize

    def stream(self) -> None:
        az_reading = self.encoder.get_reading("az")
        el_reading = self.encoder.get_reading("el")
        msg = CoordMsg(
            lon=az_reading, lat=el_reading, unit="deg", frame="altaz", time=time.time()
        )
        self.publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = AntennaEncoder()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
