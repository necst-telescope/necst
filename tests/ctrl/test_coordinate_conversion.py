from necst.ctrl.calculations import HorizontalCoord
from necst_msgs.msg import CoordMsg

from ..conftest import TesterNode, is_destroyed


class TestHorizontalCoord(TesterNode):

    NodeName = "test_horizontal_coord"

    def test_node_info(self):
        converter = HorizontalCoord()
        assert "ctrl/antenna" in converter.get_namespace()
        assert "altaz_coord" in converter.get_name()

        converter.destroy_node()
        assert is_destroyed(converter)

    def test_coordinate_frame_conversion(self):
        converter = HorizontalCoord()

        az = el = None

        def update(msg: CoordMsg) -> None:
            nonlocal az, el
            az = msg.lon
            el = msg.lat
            assert msg.unit == "deg"
            assert msg.frame == "altaz"

        ns = converter.get_namespace()
        raw_cmd = self.node.create_publisher(CoordMsg, f"{ns}/raw_coord", 1)
        converted = self.node.create_subscription(CoordMsg, f"{ns}/altaz", update, 1)

    def test_name_to_coordinate(self):
        ...

    def test_outdated_query(self):
        ...
