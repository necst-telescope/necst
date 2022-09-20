import time

from neclib.devices import antenna_encoder
from rclpy.node import Node

from necst_msgs.msg import CoordMsg
from ... import config, namespace, qos


class EncoderDevice(Node):

    NodeName = "encoder_readout"
    Namespace = namespace.antenna

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = self.create_publisher(CoordMsg, "encoder", qos.realtime)
        ports = config.antenna_encoder_port
        self.encoder_az = antenna_encoder(ports.az)
        self.encoder_el = antenna_encoder(ports.el)
        self.create_timer(1 / config.antenna_command_frequency, self.stream)

    def stream(self) -> None:
        az_reading = self.encoder_az.get_reading()
        el_reading = self.encoder_el.get_reading()
        msg = CoordMsg(
            lon=az_reading, lat=el_reading, unit="deg", frame="altaz", time=time.time()
        )
        self.publisher.publish(msg)


def main(args=None):
    import rclpy

    rclpy.init(args=args)
    node = EncoderDevice()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
