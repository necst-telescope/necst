import rclpy
from neclib.coordinates import CoordCalculator
from rclpy.node import Node


from necst import config
from necst_msgs.msg import CoordMsg


class HorizontalCoord(Node):

    NodeName = "coord_altaz"
    Namespace = f"/necst/{config.observatory}/ctrl/antenna"

    def __init__(self) -> None:
        super().__init__(self.NodeName, namespace=self.Namespace)
        self.publisher = self.create_publisher(CoordMsg, "altaz", 1)
        self.create_subscription(CoordMsg, "raw_coord", self.convert, 1)

        self.converter = CoordCalculator(config.location, config.pointing_param_path)
        # TODO: Handle weather data.

    def convert(self, msg: CoordMsg) -> None:
        name_query = bool(msg.name)
        if name_query:
            az, el = self.converter.get_altaz_by_name(msg.name, msg.time)
        else:
            az, el = self.converter.get_altaz(
                msg.lon, msg.lat, msg.frame, unit=msg.unit, obstime=msg.time
            )

        converted = CoordMsg(
            lon=az.to("deg").value,
            lat=el.to("deg").value,
            unit="deg",
            frame="altaz",
            time=msg.time,
        )
        self.publisher.publish(converted)


def main(args=None):
    rclpy.init(args=args)
    node = HorizontalCoord()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
