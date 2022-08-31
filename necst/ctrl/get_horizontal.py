from rclpy.node import Node

from neclib.ctrl import CoordCalculator
from necst_msgs.msg import CoordMsg  # name は astropy.coordinates.(get_body, get_icrs_coordinates) に渡すのを想定

from ..utils.callbacks import coord_msg_clbk  # 返り値は types.SimpleNameSpace 型


class HorizontalCoord(Node):

    node_name = "coord_altaz"

    def __init__(self) -> None:
        super().__init__(self.node_name)
        self.publisher = self.create_publisher(CoordMsg, "altaz", 1)
        self.create_subscription(CoordMsg, "raw_coord", self.convert, 1)

    def convert(self, msg: CoordMsg) -> None:
        ...
        self.publisher.publish(...)


def main(args=None):
    ...
    node = HorizontalCoord()
    ...


if __name__ == "__main__":
    main()